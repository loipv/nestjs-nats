import type { PubAck, StreamExpectations } from '@nats-io/jetstream';
import {
  Batcher,
  type ContextRunner,
  DEFAULT_BATCHER_MAX_SIZE,
  DEFAULT_BATCHER_MAX_WAIT,
} from '../batch/batcher.js';
import type { CloudEventInit } from '../cloudevents/types.js';
import type { Codec } from '../codec/codec.js';
import type { NatsHeadersInit } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsConfigError } from '../errors/errors.js';
import { createOutgoingMessage } from '../pipeline/outbound.js';
import {
  type AtomicMessage,
  type AtomicPublishAck,
  type AtomicPublishOptions,
  publishAtomic,
} from './atomic.js';
import { MSG_ID_HEADER, publishOutgoing } from './publish.js';

/** Default cap of publishes awaiting their PubAck at once in `publishMany`. */
const DEFAULT_PUBLISH_WINDOW = 256;

export type JetStreamPublishOptions = {
  msgId?: string;
  expect?: Partial<StreamExpectations>;
  headers?: NatsHeadersInit;
  codec?: Codec;
  timeout?: number;
  /**
   * Publishes a CloudEvent; without `msgId` (or a `Nats-Msg-Id` header), `Nats-Msg-Id` is
   * its `id` (server-side dedupe).
   */
  cloudEvent?: CloudEventInit;
};

export type PublishManyResult = { ok: true; ack: PubAck } | { ok: false; error: Error };

export class JetStreamClient {
  /** Instances are provided by `NatsModule`; this constructor is not part of the public API. */
  constructor(private readonly entry: ConnectionEntry) {}

  async publish(
    subject: string,
    data?: unknown,
    opts: JetStreamPublishOptions = {},
  ): Promise<PubAck> {
    const message = createOutgoingMessage(this.entry, 'jetstream', subject, data, opts);
    // As a header (rather than the `msgID` publish option) so outbound plugins see it. A
    // `Nats-Msg-Id` header given by the caller wins over the CloudEvent id. The server
    // matches the name exactly, so a caller's header of another case is re-set canonically.
    if (opts.msgId !== undefined) {
      message.headers.delete(MSG_ID_HEADER, 'insensitive');
      message.headers.set(MSG_ID_HEADER, opts.msgId);
    } else if (message.headers.has(MSG_ID_HEADER, 'insensitive')) {
      const values = message.headers.values(MSG_ID_HEADER, 'insensitive');
      message.headers.delete(MSG_ID_HEADER, 'insensitive');
      for (const v of values) message.headers.append(MSG_ID_HEADER, v);
    } else {
      const ceId = message.cloudEvent?.meta.id;
      if (ceId !== undefined) message.headers.set(MSG_ID_HEADER, ceId);
    }
    return publishOutgoing(this.entry, message, opts);
  }

  /**
   * Publishes `messages` (1 to 1000) as one atomic batch: the stream stores all of them or
   * none (NATS server >= 2.12, stream with `allow_atomic`). Each message runs through the
   * outbound plugins, whose `next()` settles with the commit; once the batch is sent, the
   * result is the commit's, without waiting for the plugins (a plugin failing after that is
   * logged). A single message is
   * published as usual. A `Nats-Msg-Id` header is refused, and a CloudEvent's id is not used
   * as one.
   */
  publishAtomic(
    messages: readonly AtomicMessage[],
    opts?: AtomicPublishOptions,
  ): Promise<AtomicPublishAck> {
    return publishAtomic(this.entry, messages, opts);
  }

  /**
   * Publishes `items` with at most `window` publishes awaiting their PubAck at once. Each item
   * goes through `publish()`, so outbound plugins apply. Results are in input order and an
   * item's failure is its result, never a rejection.
   */
  async publishMany(
    items: ReadonlyArray<{ subject: string; data?: unknown; opts?: JetStreamPublishOptions }>,
    opts: { window?: number } = {},
  ): Promise<PublishManyResult[]> {
    return this.#publishMany(items, opts);
  }

  /** `publishMany`, running an item's publish through its `run` when it has one. */
  async #publishMany(
    items: ReadonlyArray<{
      subject: string;
      data?: unknown;
      opts?: JetStreamPublishOptions;
      run?: ContextRunner;
    }>,
    opts: { window?: number },
  ): Promise<PublishManyResult[]> {
    const window = opts.window ?? DEFAULT_PUBLISH_WINDOW;
    if (!(window >= 1)) throw new NatsConfigError('publishMany window must be >= 1');
    const results = new Array<PublishManyResult>(items.length);
    let next = 0;
    const worker = async (): Promise<void> => {
      while (next < items.length) {
        const i = next++;
        const item = items[i];
        if (!item) continue;
        const publish = () => this.publish(item.subject, item.data, item.opts);
        try {
          results[i] = { ok: true, ack: await (item.run ? item.run(publish) : publish()) };
        } catch (err) {
          results[i] = { ok: false, error: err instanceof Error ? err : new Error(String(err)) };
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(Math.ceil(window), items.length) }, worker));
    return results;
  }

  /**
   * A batcher that groups publishes and flushes them through `publishMany`. It is flushed
   * and closed when the connection closes.
   */
  batcher(opts: { maxSize?: number; maxWait?: number; window?: number } = {}): Batcher {
    let off: (() => void) | undefined;
    const batcher = new Batcher(
      (items, o) => this.#publishMany(items, o ?? {}),
      {
        maxSize: opts.maxSize ?? DEFAULT_BATCHER_MAX_SIZE,
        maxWait: opts.maxWait ?? DEFAULT_BATCHER_MAX_WAIT,
        window: opts.window,
      },
      () => off?.(),
    );
    off = this.entry.onClose(() => batcher.close());
    return batcher;
  }
}
