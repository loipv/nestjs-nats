import type { JsMsg } from '@nats-io/jetstream';
import type { Msg, MsgHdrs } from '@nats-io/transport-node';
import type { LoggerService } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
import { NatsBatchContext } from '../context/nats-batch-context.js';
import type { NatsContext } from '../context/nats-context.js';
import type { NatsServiceOptions } from '../decorators/handler.decorators.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsModuleError } from '../errors/errors.js';
import { compose } from './compose.js';
import type { InboundMiddleware, NatsPlugin, Next } from './types.js';

export interface InboundMessage {
  subject: string;
  data: Uint8Array;
  headers?: MsgHdrs;
  raw: Msg | JsMsg;
  /**
   * 1-based delivery attempt: the server's delivery count for JetStream messages; for
   * core messages 1, increased by in-process retries.
   */
  attempt: number;
}

interface HandlerInfoBase {
  readonly connection: string;
  readonly className: string;
  readonly methodName: string;
}

/** Describes the handler a delivery is for (a read-only view for plugins). */
export type HandlerInfo =
  | (HandlerInfoBase & {
      readonly kind: 'subscribe';
      readonly subject: string;
      readonly queue?: string;
    })
  | (HandlerInfoBase & {
      readonly kind: 'consume';
      readonly stream: string;
      readonly durable: string;
      /** `'push'` for a `@ConsumePush` durable, `'pull'` for a `@Consume` one. */
      readonly delivery: 'pull' | 'push';
    })
  | (HandlerInfoBase & {
      readonly kind: 'ordered';
      readonly stream: string;
      /** The consumer's filter subjects; empty when it reads the whole stream. */
      readonly filterSubjects: readonly string[];
    })
  | (HandlerInfoBase & {
      readonly kind: 'service-endpoint';
      readonly service: { readonly name: string; readonly version: string };
      readonly endpoint: string;
      /** The endpoint's subject (its name unless `subject` was set). */
      readonly subject: string;
    });

/**
 * What became of one message of a delivery. Exactly one is recorded per message the
 * settlement or the idempotency middleware handled.
 */
export type MessageOutcome =
  /**
   * Acked: auto ack after success, or the handler acked it itself. `error` is set when the
   * handler acked it and then threw.
   */
  | { action: 'ack'; error?: unknown }
  /** Handed back for another attempt: JetStream `nak(delay)`, or a core in-process retry. */
  | { action: 'retry'; delayMs: number; error: unknown }
  /** Failed for good and published to the dead-letter subject. */
  | { action: 'dlq'; subject: string; error: unknown }
  /** JetStream `term` without a dead letter (non-retryable or retries exhausted). */
  | { action: 'term'; error?: unknown }
  /** Core: given up on, not dead-lettered. */
  | { action: 'drop'; error?: unknown }
  /** Left out of the handler: a duplicate, or being processed elsewhere. */
  | { action: 'skipped'; reason: 'duplicate' | 'in-progress' }
  /** A reply delivery: the reply (an error reply when `error` is set) is sent by the adapter. */
  | { action: 'replied'; error?: unknown }
  /** Nothing to settle: success on a core subscription, or a manual-ack message left unacked. */
  | { action: 'none' };

/**
 * One unit of inbound work flowing through the inbound pipeline: one message, or the
 * messages of a batch (`context` is then a `NatsBatchContext` whose `messages[i]` is the
 * context of `messages[i]`).
 */
export interface Delivery {
  readonly handler: HandlerInfo;
  readonly messages: InboundMessage[];
  readonly context: NatsContext | NatsBatchContext;
  readonly codec: Codec;
  readonly logger: LoggerService;
  /**
   * What became of each message, keyed by the entries of `messages`. It is filled in by
   * the settlement and idempotency middlewares and is complete once the `next()` of a
   * middleware with `stage: 'outer'` has resolved or rejected (read it there, after
   * awaiting `next()`).
   */
  readonly outcomes: Map<InboundMessage, MessageOutcome>;
  result?: unknown;
}

/** Internal: records the outcome of `msg` on `d`, replacing an earlier one. */
export function recordOutcome(d: Delivery, msg: InboundMessage, outcome: MessageOutcome): void {
  (d as Partial<Delivery>).outcomes?.set(msg, outcome);
}

/** Internal: key of the full handler definition on deliveries built by the adapters. */
export const HANDLER_DEFINITION: unique symbol = Symbol('nats.handlerDefinition');

/** Internal: key of the in-process retry scheduler on core subscription deliveries. */
export const RETRY_SCHEDULER: unique symbol = Symbol('nats.retryScheduler');

/**
 * Internal: key of the signal aborting the in-place waits of a delivery (the DLQ publish
 * retries of an ordered consumer) at shutdown.
 */
export const ABORT_SIGNAL: unique symbol = Symbol('nats.abortSignal');

/** Internal: key of the set of messages the handler must not process (idempotency). */
export const SKIPPED: unique symbol = Symbol('nats.skipped');

/** Internal: key of the messages of a batch that failed while the others succeeded. */
export const FAILURES: unique symbol = Symbol('nats.failures');

/** Internal: a delivery as built by the adapters, carrying the full handler definition. */
export interface InternalDelivery extends Delivery {
  readonly [HANDLER_DEFINITION]: HandlerDefinition;
  /** Messages the invoker leaves out (e.g. duplicates); the adapter starts it empty. */
  readonly [SKIPPED]: Set<InboundMessage>;
  /**
   * Messages of a batch that failed on their own (undecodable, `ctx.fail()`, returned as
   * `failed`) with their error; settled as failures even when the handler succeeds.
   */
  readonly [FAILURES]: Map<InboundMessage, unknown>;
  /** Processes `msg` again (as the next attempt) after `delayMs`; set by the core adapter. */
  readonly [RETRY_SCHEDULER]?: (msg: InboundMessage, delayMs: number) => void;
  /** Aborts the in-place waits of the settlement; set by the ordered adapter. */
  readonly [ABORT_SIGNAL]?: AbortSignal;
}

/** One message of a delivery together with its own context. */
export interface DeliveryPart {
  message: InboundMessage;
  context: NatsContext;
}

export interface DeliveryInit {
  definition: HandlerDefinition;
  /** Defaults to `toHandlerInfo(definition)`. */
  handler?: HandlerInfo;
  connection: string;
  /** Exactly one part unless `batch`. */
  parts: DeliveryPart[];
  /** Whether the handler receives the messages as a batch. */
  batch: boolean;
  codec: Codec;
  logger: LoggerService;
  retry?: (msg: InboundMessage, delayMs: number) => void;
  /** Aborted at shutdown: cancels the in-place waits of the settlement. */
  signal?: AbortSignal;
}

/** Internal: builds the delivery of one message, or of a batch of messages. */
export function createDelivery(init: DeliveryInit): InternalDelivery {
  const { parts } = init;
  const failures = new Map<InboundMessage, unknown>();
  let context: NatsContext | NatsBatchContext;
  if (init.batch) {
    const byContext = new Map(parts.map((p) => [p.context, p.message]));
    context = new NatsBatchContext({
      connection: init.connection,
      messages: parts.map((p) => p.context),
      onFail: (ctx, error) => {
        const message = byContext.get(ctx);
        if (message) failures.set(message, error);
      },
    });
  } else {
    const [part] = parts;
    if (!part || parts.length > 1) {
      throw new NatsModuleError(`A delivery holds one message, got ${parts.length}`);
    }
    context = part.context;
  }
  return {
    handler: init.handler ?? toHandlerInfo(init.definition),
    [HANDLER_DEFINITION]: init.definition,
    [SKIPPED]: new Set(),
    [FAILURES]: failures,
    ...(init.retry ? { [RETRY_SCHEDULER]: init.retry } : {}),
    ...(init.signal ? { [ABORT_SIGNAL]: init.signal } : {}),
    messages: parts.map((p) => p.message),
    outcomes: new Map(),
    context,
    codec: init.codec,
    logger: init.logger,
  };
}

/** The context of each message of `d`, index-aligned with `d.messages`. */
export function messageContexts(d: Delivery): ReadonlyArray<NatsContext> {
  return d.context instanceof NatsBatchContext ? d.context.messages : [d.context];
}

/** Internal: the full definition behind a delivery built by an adapter. */
export function definitionOf(d: Delivery): HandlerDefinition {
  const def = (d as Partial<InternalDelivery>)[HANDLER_DEFINITION];
  if (!def) throw new NatsModuleError('Delivery carries no handler definition');
  return def;
}

/**
 * Builds the public, frozen description of a handler. Service endpoints take their
 * service options from `service` when given, else from the definition.
 */
export function toHandlerInfo(def: HandlerDefinition, service?: NatsServiceOptions): HandlerInfo {
  const base = {
    connection: def.connection,
    className: def.className,
    methodName: def.methodName,
  };
  switch (def.kind) {
    case 'subscribe': {
      const { queue } = def.options;
      return Object.freeze({
        kind: def.kind,
        ...base,
        subject: def.subject,
        ...(queue !== undefined ? { queue } : {}),
      });
    }
    case 'consume':
      return Object.freeze({
        kind: def.kind,
        ...base,
        stream: def.options.stream,
        durable: def.options.durable,
        delivery: def.delivery,
      });
    case 'ordered': {
      const { filterSubjects } = def.options;
      return Object.freeze({
        kind: def.kind,
        ...base,
        stream: def.options.stream,
        filterSubjects: Object.freeze(
          filterSubjects === undefined
            ? []
            : typeof filterSubjects === 'string'
              ? [filterSubjects]
              : [...filterSubjects],
        ),
      });
    }
    case 'service-endpoint': {
      const svc = service ?? def.service;
      if (!svc) {
        throw new NatsModuleError(
          `@ServiceEndpoint ${def.className}.${def.methodName} has no @NatsService`,
        );
      }
      return Object.freeze({
        kind: def.kind,
        ...base,
        service: Object.freeze({ name: svc.name, version: svc.version }),
        endpoint: def.endpoint,
        subject: def.options.subject ?? def.endpoint,
      });
    }
  }
}

type InboundHandler = (d: Delivery, next: Next) => Promise<void>;

const toHandler =
  (mw: InboundMiddleware): InboundHandler =>
  (d, next) =>
    mw.handle(d, next);

/**
 * Builds the inbound pipeline: plugins with `stage: 'outer'`, then the core
 * middlewares, then plugins with `stage: 'inner'` (or no stage), then the terminal.
 * Plugins keep their registration order within a stage.
 */
export function buildInboundPipeline(
  parts: { plugins: NatsPlugin[]; core: InboundMiddleware[] },
  terminal: (d: InternalDelivery) => Promise<void>,
): (d: InternalDelivery) => Promise<void> {
  const outer: InboundHandler[] = [];
  const inner: InboundHandler[] = [];
  for (const { inbound } of parts.plugins) {
    if (!inbound) continue;
    (inbound.stage === 'outer' ? outer : inner).push(toHandler(inbound));
  }
  return compose<InternalDelivery>([...outer, ...parts.core.map(toHandler), ...inner], terminal);
}
