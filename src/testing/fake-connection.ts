import type { JetStreamPublishOptions, PubAck } from '@nats-io/jetstream';
import {
  headers as createHeaders,
  type Msg,
  type MsgHdrs,
  NoRespondersError,
  type Payload,
  type PublishOptions,
  type Status,
} from '@nats-io/transport-node';
import { parseCloudEvent, readCloudEventMeta } from '../cloudevents/codec.js';
import type { CloudEventMeta } from '../cloudevents/types.js';
import { type Codec, decodePayload, encodePayload } from '../codec/codec.js';
import { fromMsgHdrs } from '../codec/headers.js';
import { NatsConfigError } from '../errors/errors.js';
import { subjectMatches } from './subject.js';

/** A message published through a fake connection, as recorded by the mocks. */
export interface PublishedMessage {
  subject: string;
  /**
   * The payload decoded with the client's codec: the event's `data` for a CloudEvent;
   * the raw bytes when they cannot be decoded.
   */
  data: unknown;
  /** The bytes put on the wire. */
  raw: Uint8Array;
  headers: Record<string, string | string[]>;
  /** The CloudEvent attributes, when the message is a CloudEvent. */
  cloudEvent?: CloudEventMeta;
  /** The reply subject (a request's inbox). */
  reply?: string;
}

/** Stubs the answer to the requests sent to a subject (see `whenRequest`). */
export interface RequestStub {
  /** Answers with `value`, encoded with the client's codec. */
  reply(value: unknown): void;
  /** Answers with a service error (`NatsRequestError` with code `'service_error'`). */
  fail(error: { code: number; description: string }): void;
}

type StubbedAnswer = { data: Uint8Array } | { error: { code: number; description: string } };

const EMPTY = new Uint8Array(0);
const MSG_ID_HEADER = 'Nats-Msg-Id';
const SERVICE_ERROR_CODE_HEADER = 'Nats-Service-Error-Code';
const SERVICE_ERROR_HEADER = 'Nats-Service-Error';

function toBytes(payload?: Payload): Uint8Array {
  if (payload === undefined || payload === null) return EMPTY;
  if (typeof payload === 'string') return new TextEncoder().encode(payload);
  // A copy, like the bytes written to the wire.
  return Uint8Array.from(payload);
}

/** Describes a published message the way a test reads it. */
export function toPublishedMessage(
  codec: Codec,
  subject: string,
  raw: Uint8Array,
  headers?: MsgHdrs,
  reply?: string,
): PublishedMessage {
  const out: PublishedMessage = { subject, data: raw, raw, headers: fromMsgHdrs(headers) };
  const message = { data: raw, ...(headers ? { headers } : {}) };
  const cloudEvent = readCloudEventMeta(message);
  try {
    out.data = cloudEvent
      ? parseCloudEvent(message).data(codec)
      : decodePayload(codec, raw, headers);
  } catch {
    // Undecodable: `data` stays the raw bytes.
  }
  if (cloudEvent) out.cloudEvent = cloudEvent;
  if (reply) out.reply = reply;
  return out;
}

/** A raw message handed back by the fake connection (a stubbed reply). */
function replyMsg(subject: string, data: Uint8Array, headers?: MsgHdrs): Msg {
  return {
    subject,
    data,
    sid: 0,
    reply: '',
    ...(headers ? { headers } : {}),
    respond: () => false,
    json: <T>() => JSON.parse(new TextDecoder().decode(data)) as T,
    string: () => new TextDecoder().decode(data),
  } as Msg;
}

/** The subset of the NATS JetStream client the library uses, recording publishes. */
export interface FakeJetStream {
  publish(
    subject: string,
    payload?: Payload,
    opts?: Partial<JetStreamPublishOptions>,
  ): Promise<PubAck>;
}

/**
 * Internal: an in-memory stand-in for a `NatsConnection` (and its JetStream client) that
 * never touches the network. It records what is published, answers requests from stubs
 * (no stub: no responders) and refuses subscriptions.
 */
export class FakeNatsConnection {
  /** Core publishes and requests, in order. */
  readonly published: PublishedMessage[] = [];
  /** JetStream publishes, in order. */
  readonly jsPublished: PublishedMessage[] = [];
  private readonly stubs: Array<{ pattern: string; answer: StubbedAnswer }> = [];
  private readonly listeners = new Set<(message: PublishedMessage) => void>();
  private readonly msgIds = new Set<string>();
  private readonly closedPromise: Promise<void>;
  private resolveClosed!: () => void;
  private jsSeq = 0;
  private inboxes = 0;
  private state: 'open' | 'draining' | 'closed' = 'open';
  private js?: FakeJetStream;

  constructor(private readonly codec: Codec) {
    this.closedPromise = new Promise<void>((resolve) => {
      this.resolveClosed = resolve;
    });
  }

  /** Calls `listener` with every message recorded from now on; returns an unsubscribe. */
  onPublish(listener: (message: PublishedMessage) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Stubs the requests sent to `subject` (a NATS subject pattern); the latest stub wins. */
  whenRequest(subject: string): RequestStub {
    return {
      reply: (value) => {
        this.stubs.push({ pattern: subject, answer: { data: encodePayload(this.codec, value) } });
      },
      fail: (error) => {
        this.stubs.push({ pattern: subject, answer: { error: { ...error } } });
      },
    };
  }

  // --- NatsConnection ---

  publish(subject: string, payload?: Payload, options?: PublishOptions): void {
    this.record(this.published, subject, toBytes(payload), options?.headers, options?.reply);
  }

  request(
    subject: string,
    payload?: Payload,
    options?: { headers?: MsgHdrs; timeout?: number },
  ): Promise<Msg> {
    const inbox = `_INBOX.mock.${++this.inboxes}`;
    this.record(this.published, subject, toBytes(payload), options?.headers, inbox);
    const stub = this.stubs.findLast((s) => subjectMatches(s.pattern, subject));
    if (!stub) return Promise.reject(new NoRespondersError(subject));
    const { answer } = stub;
    if ('data' in answer) return Promise.resolve(replyMsg(inbox, answer.data));
    const h = createHeaders();
    h.set(SERVICE_ERROR_CODE_HEADER, String(answer.error.code));
    h.set(SERVICE_ERROR_HEADER, answer.error.description);
    return Promise.resolve(replyMsg(inbox, EMPTY, h));
  }

  subscribe(): never {
    throw new NatsConfigError(
      'Subscriptions are not available on a fake NATS connection; dispatch messages with NatsTestHarness',
    );
  }

  flush(): Promise<void> {
    return Promise.resolve();
  }

  rtt(): Promise<number> {
    return Promise.resolve(0);
  }

  getServer(): string {
    return 'mock';
  }

  isClosed(): boolean {
    return this.state === 'closed';
  }

  isDraining(): boolean {
    return this.state === 'draining';
  }

  /** Resolves (never with an error) once the connection is closed. */
  closed(): Promise<undefined> {
    return this.closedPromise.then(() => undefined);
  }

  drain(): Promise<void> {
    return this.close();
  }

  close(): Promise<void> {
    this.state = 'closed';
    this.resolveClosed();
    return Promise.resolve();
  }

  /** No status events; ends once the connection is closed. */
  status(): AsyncIterable<Status> {
    const closed = this.closedPromise;
    return {
      [Symbol.asyncIterator]: () => ({
        next: () => closed.then(() => ({ done: true as const, value: undefined })),
      }),
    };
  }

  // --- JetStream ---

  /** A JetStream client whose publishes are recorded and acked by a fake `'mock'` stream. */
  jetstream(): FakeJetStream {
    this.js ??= {
      publish: (subject, payload, opts) => {
        const headers = opts?.headers;
        const msgId = opts?.msgID ?? headers?.get(MSG_ID_HEADER);
        const duplicate = msgId ? this.msgIds.has(msgId) : false;
        if (msgId) this.msgIds.add(msgId);
        this.record(this.jsPublished, subject, toBytes(payload), headers);
        return Promise.resolve({ stream: 'mock', seq: ++this.jsSeq, duplicate });
      },
    };
    return this.js;
  }

  private record(
    into: PublishedMessage[],
    subject: string,
    raw: Uint8Array,
    headers?: MsgHdrs,
    reply?: string,
  ): void {
    const message = toPublishedMessage(this.codec, subject, raw, headers, reply);
    into.push(message);
    for (const listener of this.listeners) listener(message);
  }
}
