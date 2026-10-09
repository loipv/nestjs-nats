import type { JsMsg } from '@nats-io/jetstream';
import type { Msg, MsgHdrs } from '@nats-io/transport-node';
import type { LoggerService } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
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
    })
  | (HandlerInfoBase & {
      readonly kind: 'service-endpoint';
      readonly service: { readonly name: string; readonly version: string };
      readonly endpoint: string;
      /** The endpoint's subject (its name unless `subject` was set). */
      readonly subject: string;
    });

/** One unit of inbound work flowing through the inbound pipeline. */
export interface Delivery {
  readonly handler: HandlerInfo;
  readonly messages: InboundMessage[];
  readonly context: NatsContext;
  readonly codec: Codec;
  readonly logger: LoggerService;
  result?: unknown;
}

/** Internal: key of the full handler definition on deliveries built by the adapters. */
export const HANDLER_DEFINITION: unique symbol = Symbol('nats.handlerDefinition');

/** Internal: key of the in-process retry scheduler on core subscription deliveries. */
export const RETRY_SCHEDULER: unique symbol = Symbol('nats.retryScheduler');

/** Internal: key of the set of messages the handler must not process (idempotency). */
export const SKIPPED: unique symbol = Symbol('nats.skipped');

/** Internal: a delivery as built by the adapters, carrying the full handler definition. */
export interface InternalDelivery extends Delivery {
  readonly [HANDLER_DEFINITION]: HandlerDefinition;
  /** Messages the invoker leaves out (e.g. duplicates); the adapter starts it empty. */
  readonly [SKIPPED]: Set<InboundMessage>;
  /** Processes `msg` again (as the next attempt) after `delayMs`; set by the core adapter. */
  readonly [RETRY_SCHEDULER]?: (msg: InboundMessage, delayMs: number) => void;
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
      });
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
