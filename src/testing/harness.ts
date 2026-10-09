import type { JsMsg } from '@nats-io/jetstream';
import type { Msg } from '@nats-io/transport-node';
import { type INestApplicationContext, Injectable } from '@nestjs/common';
import {
  createHandlerPipeline,
  defaultPipelineDeps,
  type HandlerPipeline,
} from '../adapters/handler-pipeline.js';
import { resolveCloudEvent, writeCloudEvent } from '../cloudevents/codec.js';
import type { CloudEventInit } from '../cloudevents/types.js';
import { type Codec, encodePayload } from '../codec/codec.js';
import { type HeadersInit, toMsgHdrs } from '../codec/headers.js';
import { type ConnectionEntry, NatsConnectionManager } from '../connection/connection-manager.js';
import { NatsContext, NatsJetStreamContext } from '../context/nats-context.js';
import { type HandlerDefinition, HandlerExplorer } from '../discovery/handler-explorer.js';
import { validateHandlers } from '../discovery/handler-validator.js';
import { NatsConfigError } from '../errors/errors.js';
import {
  createDelivery,
  type DeliveryPart,
  type InboundMessage,
  type MessageOutcome,
} from '../pipeline/delivery.js';
import { isReplyDelivery, type ServiceError, toServiceError } from '../pipeline/reply.js';
import { DLQ_HEADERS } from '../settlement/dlq.js';
import type { PublishedMessage } from './fake-connection.js';
import { fakeConnectionOf } from './mock-clients.js';
import { subjectMatches } from './subject.js';

/** The handler to dispatch to: `'Class.method'` (a router: `'Class.@CloudEventRouter'`), or a subject. */
export type DispatchTarget = { handler: string } | { subject: string };

export interface DispatchOptions {
  headers?: HeadersInit;
  /** JetStream delivery count (default 1); `> 1` marks the message redelivered. */
  deliveryCount?: number;
  /** Core delivery attempt (default 1), as after in-process retries. */
  attempt?: number;
  /** Core subscriptions: the message is a request (it has a reply subject). */
  request?: boolean;
  /** Sends the payload as a CloudEvent (binary mode unless `mode` says otherwise). */
  cloudEvent?: CloudEventInit;
  /** The message's actual subject; defaults to the target subject, else the handler's. */
  subject?: string;
}

export type DispatchSettlement =
  | 'ack'
  | 'nak'
  | 'term'
  | 'dlq'
  | 'retry'
  | 'drop'
  | 'skipped'
  | 'none'
  | 'replied';

export interface DispatchResult {
  /** What the handler returned (never set for a batch). */
  result?: unknown;
  /**
   * The fate of the first message: its outcome's action, a JetStream retry being `'nak'`
   * and a core retry `'retry'` (which the harness does not run again).
   */
  settlement: DispatchSettlement;
  /** The outcome of each message, index-aligned with the dispatched payloads. */
  outcomes: MessageOutcome[];
  /** The redelivery delay of a `'nak'`, in ms. */
  nakDelay?: number;
  /** The reply to a request: its `data`, or the service `error` sent instead. */
  reply?: { data?: unknown; error?: ServiceError };
  /** The dead letters published while the messages were processed. */
  dlq: PublishedMessage[];
}

const label = (def: HandlerDefinition) => `${def.className}.${def.methodName}`;

/** The subjects a handler receives from: the ones a `{ subject }` target is matched against. */
function subjectsOf(def: HandlerDefinition): string[] {
  switch (def.kind) {
    case 'subscribe':
      return [def.subject];
    case 'consume': {
      const { filter_subject: one, filter_subjects: many } = def.options.config ?? {};
      return one ? [one] : (many ?? []);
    }
    case 'service-endpoint':
      return [def.options.subject ?? def.endpoint];
  }
}

function describeHandler(def: HandlerDefinition): string {
  const subjects = subjectsOf(def).map((s) => `"${s}"`);
  const source =
    def.kind === 'consume'
      ? [`${def.options.stream}/${def.options.durable}`, ...subjects]
      : subjects;
  return `${label(def)} (${def.kind}${source.length > 0 ? ` ${source.join(', ')}` : ''})`;
}

function positiveInteger(name: string, value: number | undefined): number {
  if (value === undefined) return 1;
  if (!Number.isInteger(value) || value < 1) {
    throw new NatsConfigError(
      `dispatch ${name} ${String(value)} is invalid; expected an integer >= 1`,
    );
  }
  return value;
}

/** A call made on a fake JetStream message, as recorded in its `calls`. */
type JsMsgCall =
  | { op: 'ack' }
  | { op: 'nak'; delay?: number }
  | { op: 'term'; reason?: string }
  | { op: 'working' };

/**
 * Runs fake messages through the real inbound pipeline of a handler (plugins, settlement,
 * idempotency, Nest guards/pipes/interceptors/filters) without a NATS server. Get it with
 * `NatsTestHarness.create(app)` from an application built with `NatsTestingModule`.
 * Idempotency stores keep their state across dispatches of the same application.
 */
@Injectable()
export class NatsTestHarness {
  private handlers?: HandlerDefinition[];
  private readonly pipelines = new Map<HandlerDefinition, HandlerPipeline>();
  private sequence = 0;
  private inboxes = 0;

  constructor(
    private readonly explorer: HandlerExplorer,
    private readonly manager: NatsConnectionManager,
  ) {}

  static create(app: INestApplicationContext): NatsTestHarness {
    return app.get(NatsTestHarness);
  }

  /**
   * Dispatches `payload` to the handler `target` designates and reports what became of
   * it. A batch handler takes an array of payloads, one message each. Rejects with a
   * `NatsConfigError` when the handlers are misconfigured (as bootstrap would) or the
   * target matches no handler or several; with the error itself when the pipeline fails
   * outside settlement (e.g. a plugin throws) on a delivery that expects no reply.
   */
  async dispatch(
    target: DispatchTarget,
    payload: unknown,
    options: DispatchOptions = {},
  ): Promise<DispatchResult> {
    const def = this.resolve(target);
    const entry = this.manager.get(def.connection);
    const pipeline = this.pipelineOf(def, entry);
    const batch = def.kind !== 'service-endpoint' && def.options.batch !== undefined;
    if (batch && (!Array.isArray(payload) || payload.length === 0)) {
      throw new NatsConfigError(
        `${label(def)} is a batch handler: dispatch a non-empty array of payloads, one per message`,
      );
    }
    const payloads: unknown[] = batch ? (payload as unknown[]) : [payload];
    const subject =
      options.subject ?? ('subject' in target ? target.subject : this.defaultSubject(def));
    const parts = payloads.map((p) => this.part(def, entry, pipeline.codec, subject, p, options));
    const d = createDelivery({
      definition: def,
      handler: pipeline.info,
      connection: entry.name,
      batch,
      parts,
      codec: pipeline.codec,
      logger: entry.logger,
      // Recorded in the outcome only: the harness never runs a retry.
      ...(def.kind === 'subscribe' ? { retry: () => {} } : {}),
    });

    const dlq: PublishedMessage[] = [];
    const stopListening = fakeConnectionOf(entry).onPublish((message) => {
      if (DLQ_HEADERS.subject in message.headers) dlq.push(message);
    });
    let failure: { error: unknown } | undefined;
    try {
      await pipeline.run(d);
    } catch (error) {
      failure = { error };
    } finally {
      stopListening();
    }

    let reply: DispatchResult['reply'];
    if (isReplyDelivery(d)) {
      reply = failure
        ? { error: replyError(failure.error) }
        : this.replyOf(def, pipeline, d.result);
    } else if (failure) {
      throw failure.error;
    }

    const outcomes = d.messages.map((m): MessageOutcome => d.outcomes.get(m) ?? { action: 'none' });
    const first = outcomes[0] ?? { action: 'none' };
    const settlement: DispatchSettlement =
      first.action === 'retry' ? (def.kind === 'consume' ? 'nak' : 'retry') : first.action;
    return {
      ...(d.result !== undefined ? { result: d.result } : {}),
      settlement,
      outcomes,
      ...(settlement === 'nak' && first.action === 'retry' ? { nakDelay: first.delayMs } : {}),
      ...(reply ? { reply } : {}),
      dlq,
    };
  }

  /** The reply the adapter would send for a successful delivery (none for an empty subscription result). */
  private replyOf(
    def: HandlerDefinition,
    pipeline: HandlerPipeline,
    result: unknown,
  ): DispatchResult['reply'] {
    if (def.kind === 'subscribe' && result === undefined) return undefined;
    try {
      encodePayload(pipeline.codec, result);
    } catch (err) {
      return { error: replyError(err) };
    }
    return { data: result };
  }

  /** The discovered handlers, validated on first use as bootstrap validates them. */
  private explore(): HandlerDefinition[] {
    if (!this.handlers) {
      const defs = this.explorer.explore();
      validateHandlers(defs, this.manager);
      this.handlers = defs;
    }
    return this.handlers;
  }

  private resolve(target: DispatchTarget): HandlerDefinition {
    const defs = this.explore();
    let matches: HandlerDefinition[];
    let wanted: string;
    if (typeof (target as { handler?: unknown }).handler === 'string') {
      const { handler } = target as { handler: string };
      wanted = `handler "${handler}"`;
      matches = defs.filter((def) => label(def) === handler);
    } else if (typeof (target as { subject?: unknown }).subject === 'string') {
      const { subject } = target as { subject: string };
      wanted = `subject "${subject}"`;
      matches = defs.filter((def) => subjectsOf(def).some((p) => subjectMatches(p, subject)));
    } else {
      throw new NatsConfigError(
        "dispatch target must be { handler: 'Class.method' } or { subject: '...' }",
      );
    }
    const [match] = matches;
    if (match && matches.length === 1) return match;
    if (!match) {
      const known = defs.map(describeHandler).join('; ') || 'none';
      throw new NatsConfigError(`No NATS handler matches ${wanted}. Handlers: ${known}`);
    }
    throw new NatsConfigError(
      `Several NATS handlers match ${wanted}: ${matches.map(describeHandler).join('; ')}`,
    );
  }

  private pipelineOf(def: HandlerDefinition, entry: ConnectionEntry): HandlerPipeline {
    let pipeline = this.pipelines.get(def);
    if (!pipeline) {
      pipeline = createHandlerPipeline(def, entry, defaultPipelineDeps);
      this.pipelines.set(def, pipeline);
    }
    return pipeline;
  }

  /** The subject of a dispatched message when neither the target nor the options give one. */
  private defaultSubject(def: HandlerDefinition): string {
    return subjectsOf(def)[0] ?? (def.kind === 'consume' ? def.options.stream : label(def));
  }

  /** One fake message (with its context) as the handler's adapter would receive it. */
  private part(
    def: HandlerDefinition,
    entry: ConnectionEntry,
    codec: Codec,
    subject: string,
    payload: unknown,
    options: DispatchOptions,
  ): DeliveryPart {
    const h = toMsgHdrs(options.headers);
    let data: Uint8Array;
    if (options.cloudEvent) {
      // As on publish: bytes get no default datacontenttype.
      const defaults: { source?: string; contentType?: string } = {};
      if (!(payload instanceof Uint8Array)) defaults.contentType = codec.contentType;
      const source = entry.options.cloudEvents?.source;
      if (source !== undefined) defaults.source = source;
      const meta = resolveCloudEvent(options.cloudEvent, defaults);
      data = writeCloudEvent(meta, payload, codec, options.cloudEvent.mode ?? 'binary', h);
    } else {
      data = encodePayload(codec, payload);
    }
    const headers = h.keys().length > 0 ? h : undefined;
    const base = { subject, data, ...(headers ? { headers } : {}) };
    const message = (raw: Msg | JsMsg, attempt: number): InboundMessage => ({
      subject,
      data,
      ...(headers ? { headers } : {}),
      raw,
      attempt,
    });
    switch (def.kind) {
      case 'consume': {
        const deliveryCount = positiveInteger('deliveryCount', options.deliveryCount);
        const raw = fakeJsMsg(
          base,
          def.options.stream,
          def.options.durable,
          deliveryCount,
          ++this.sequence,
        );
        return {
          message: message(raw, deliveryCount),
          context: new NatsJetStreamContext({ kind: 'jetstream', connection: entry.name, raw }),
        };
      }
      case 'subscribe': {
        const attempt = positiveInteger('attempt', options.attempt);
        const reply = options.request ? this.inbox() : '';
        const raw = fakeMsg(base, reply);
        return {
          message: message(raw, attempt),
          context: new NatsContext({ kind: 'core', connection: entry.name, raw }),
        };
      }
      case 'service-endpoint': {
        const raw = fakeMsg(base, this.inbox());
        return {
          message: message(raw, 1),
          context: new NatsContext({ kind: 'service', connection: entry.name, raw }),
        };
      }
    }
  }

  private inbox(): string {
    return `_INBOX.harness.${++this.inboxes}`;
  }
}

/** The error reply sent for `err` (header values cannot hold CR/LF). */
function replyError(err: unknown): ServiceError {
  const { code, description } = toServiceError(err);
  return { code, description: description.replace(/[\r\n]+/g, ' ') };
}

type FakeBase = { subject: string; data: Uint8Array; headers?: Msg['headers'] };

const textDecoder = new TextDecoder();

function bodyReaders(data: Uint8Array) {
  return {
    json: <T>() => JSON.parse(textDecoder.decode(data)) as T,
    string: () => textDecoder.decode(data),
  };
}

/** A fake core message (or service request) recording its responses. */
function fakeMsg(base: FakeBase, reply: string): Msg {
  const responses: Array<{ data?: unknown; headers?: unknown }> = [];
  return {
    ...base,
    reply,
    sid: 0,
    ...bodyReaders(base.data),
    responses,
    respond(data?: unknown, opts?: { headers?: unknown }) {
      responses.push({ data, ...(opts?.headers ? { headers: opts.headers } : {}) });
      return true;
    },
    respondError(code: number, description: string) {
      responses.push({ headers: { code, description } });
      return true;
    },
  } as unknown as Msg;
}

/** A fake JetStream message recording its settle calls. */
function fakeJsMsg(
  base: FakeBase,
  stream: string,
  durable: string,
  deliveryCount: number,
  seq: number,
): JsMsg {
  const calls: JsMsgCall[] = [];
  const redelivered = deliveryCount > 1;
  const time = new Date();
  const timestampNanos = BigInt(time.getTime()) * 1_000_000n;
  return {
    ...base,
    ...bodyReaders(base.data),
    sid: 0,
    seq,
    redelivered,
    time,
    timestamp: time.toISOString(),
    timestampNanos,
    info: {
      domain: '',
      stream,
      consumer: durable,
      deliveryCount,
      streamSequence: seq,
      deliverySequence: seq,
      timestampNanos: Number(timestampNanos),
      pending: 0,
      redelivered,
    },
    /** Every ack/nak/term/working call made on the message, in order. */
    calls,
    ack: () => {
      calls.push({ op: 'ack' });
    },
    nak: (delay?: number) => {
      calls.push({ op: 'nak', ...(delay !== undefined ? { delay } : {}) });
    },
    term: (reason?: string) => {
      calls.push({ op: 'term', ...(reason !== undefined ? { reason } : {}) });
    },
    working: () => {
      calls.push({ op: 'working' });
    },
    ackAck: () => {
      calls.push({ op: 'ack' });
      return Promise.resolve(true);
    },
    next: () => {},
  } as unknown as JsMsg;
}
