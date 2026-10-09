import type {
  JetStreamPublishOptions as NatsPublishOptions,
  PubAck,
  StreamExpectations,
} from '@nats-io/jetstream';
import {
  Batcher,
  type ContextRunner,
  DEFAULT_BATCHER_MAX_SIZE,
  DEFAULT_BATCHER_MAX_WAIT,
} from '../batch/batcher.js';
import type { CloudEventInit } from '../cloudevents/types.js';
import type { Codec } from '../codec/codec.js';
import type { HeadersInit } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import { compose } from '../pipeline/compose.js';
import {
  createOutgoingMessage,
  encodeOutgoing,
  outboundMiddlewares,
} from '../pipeline/outbound.js';

/** JetStream's de-duplication header, set from the `msgId` publish option. */
const MSG_ID_HEADER = 'Nats-Msg-Id';

/** Default cap of publishes awaiting their PubAck at once in `publishMany`. */
const DEFAULT_PUBLISH_WINDOW = 256;

export type JetStreamPublishOptions = {
  msgId?: string;
  expect?: Partial<StreamExpectations>;
  headers?: HeadersInit;
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
  constructor(private readonly entry: ConnectionEntry) {}

  async publish(
    subject: string,
    data?: unknown,
    opts: JetStreamPublishOptions = {},
  ): Promise<PubAck> {
    const message = createOutgoingMessage(this.entry, 'jetstream', subject, data, opts);
    // As a header (rather than the `msgID` publish option) so outbound plugins see it. A
    // `Nats-Msg-Id` header given by the caller wins over the CloudEvent id.
    if (opts.msgId !== undefined) {
      message.headers.delete(MSG_ID_HEADER, 'insensitive');
      message.headers.set(MSG_ID_HEADER, opts.msgId);
    } else if (!message.headers.has(MSG_ID_HEADER, 'insensitive')) {
      const ceId = message.cloudEvent?.meta.id;
      if (ceId !== undefined) message.headers.set(MSG_ID_HEADER, ceId);
    }
    let ack: PubAck | undefined;
    const run = compose(outboundMiddlewares(this.entry), async (m) => {
      const payload = encodeOutgoing(m);
      const publishOpts: Partial<NatsPublishOptions> = {};
      if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
      if (opts.expect !== undefined) publishOpts.expect = opts.expect;
      if (opts.timeout !== undefined) publishOpts.timeout = opts.timeout;
      ack = await this.entry.js().publish(m.subject, payload, publishOpts);
    });
    await run(message);
    if (!ack) {
      throw new NatsModuleError(`JetStream publish to "${subject}" produced no acknowledgement`);
    }
    return ack;
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
