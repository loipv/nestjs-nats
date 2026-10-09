import type { JetStreamPublishOptions, PubAck, StreamExpectations } from '@nats-io/jetstream';
import { type Codec, encodePayload } from '../codec/codec.js';
import type { HeadersInit } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsModuleError } from '../errors/errors.js';
import { compose } from '../pipeline/compose.js';
import { createOutgoingMessage, outboundMiddlewares } from '../pipeline/outbound.js';

export class JetStreamClient {
  constructor(private readonly entry: ConnectionEntry) {}

  async publish(
    subject: string,
    data?: unknown,
    opts: {
      msgId?: string;
      expect?: Partial<StreamExpectations>;
      headers?: HeadersInit;
      codec?: Codec;
      timeout?: number;
    } = {},
  ): Promise<PubAck> {
    const message = createOutgoingMessage(this.entry, 'jetstream', subject, data, opts);
    let ack: PubAck | undefined;
    const run = compose(outboundMiddlewares(this.entry), async (m) => {
      const publishOpts: Partial<JetStreamPublishOptions> = {};
      if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
      if (opts.msgId !== undefined) publishOpts.msgID = opts.msgId;
      if (opts.expect !== undefined) publishOpts.expect = opts.expect;
      if (opts.timeout !== undefined) publishOpts.timeout = opts.timeout;
      ack = await this.entry.js().publish(m.subject, encodePayload(m.codec, m.data), publishOpts);
    });
    await run(message);
    if (!ack) {
      throw new NatsModuleError(`JetStream publish to "${subject}" produced no acknowledgement`);
    }
    return ack;
  }
}
