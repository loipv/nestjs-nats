import type {
  JetStreamPublishOptions as NatsPublishOptions,
  PubAck,
  StreamExpectations,
} from '@nats-io/jetstream';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsModuleError } from '../errors/errors.js';
import { compose } from '../pipeline/compose.js';
import { encodeOutgoing, outboundMiddlewares } from '../pipeline/outbound.js';
import type { OutgoingMessage } from '../pipeline/types.js';

/** JetStream's de-duplication header. */
export const MSG_ID_HEADER = 'Nats-Msg-Id';

/**
 * Runs `message` through the outbound plugins of `entry` and publishes it to JetStream.
 * `wrapError` maps a failure of the publish itself (not of a plugin).
 */
export async function publishOutgoing(
  entry: ConnectionEntry,
  message: OutgoingMessage,
  opts: { expect?: Partial<StreamExpectations>; timeout?: number },
  wrapError: (err: unknown) => unknown = (err) => err,
): Promise<PubAck> {
  const { subject } = message;
  let ack: PubAck | undefined;
  const run = compose(outboundMiddlewares(entry), async (m) => {
    const payload = encodeOutgoing(m);
    const publishOpts: Partial<NatsPublishOptions> = {};
    if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
    if (opts.expect !== undefined) publishOpts.expect = opts.expect;
    if (opts.timeout !== undefined) publishOpts.timeout = opts.timeout;
    try {
      ack = await entry.js().publish(m.subject, payload, publishOpts);
    } catch (err) {
      throw wrapError(err);
    }
  });
  await run(message);
  if (!ack) {
    throw new NatsModuleError(`JetStream publish to "${subject}" produced no acknowledgement`);
  }
  return ack;
}
