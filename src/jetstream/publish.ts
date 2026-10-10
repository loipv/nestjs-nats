import type {
  JetStreamPublishOptions as NatsPublishOptions,
  PubAck,
  StreamExpectations,
} from '@nats-io/jetstream';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsModuleError } from '../errors/errors.js';
import { encodeOutgoing, sendOutbound, timerDelay } from '../pipeline/outbound.js';
import type { OutgoingMessage } from '../pipeline/types.js';

/** JetStream's de-duplication header. */
export const MSG_ID_HEADER = 'Nats-Msg-Id';
/** The timeout (ms) when the JetStream client has none. */
export const DEFAULT_JETSTREAM_TIMEOUT = 5000;

/** The JetStream client's timeout (ms), as a timer delay. */
export function jetStreamTimeout(entry: ConnectionEntry): number {
  // `getOptions` is missing on the fake JetStream client of /testing.
  return timerDelay(entry.js().getOptions?.().timeout || DEFAULT_JETSTREAM_TIMEOUT);
}

/**
 * Runs `message` through the outbound plugins of `entry` and publishes it to JetStream. The
 * plugins must pass it on within the timeout (`opts.timeout`, else the JetStream client's);
 * once it is published, the result is the PubAck (or the publish failure), without waiting
 * for the plugins. `errors` replaces the errors of the publish itself (not of a plugin):
 * `wrap` maps its failure, `expired` and `skipped` are the plugins not passing it on in time
 * or at all.
 */
export function publishOutgoing(
  entry: ConnectionEntry,
  message: OutgoingMessage,
  opts: { expect?: Partial<StreamExpectations>; timeout?: number },
  errors: {
    wrap?: (err: unknown) => unknown;
    expired?: (ms: number) => Error;
    skipped?: () => Error;
  } = {},
): Promise<PubAck> {
  const { subject } = message;
  const what = `JetStream publish to "${subject}"`;
  return sendOutbound(
    entry,
    message,
    async (m) => {
      const payload = encodeOutgoing(m);
      const publishOpts: Partial<NatsPublishOptions> = {};
      if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
      if (opts.expect !== undefined) publishOpts.expect = opts.expect;
      if (opts.timeout !== undefined) publishOpts.timeout = opts.timeout;
      try {
        return await entry.js().publish(m.subject, payload, publishOpts);
      } catch (err) {
        throw errors.wrap ? errors.wrap(err) : err;
      }
    },
    {
      what,
      timeout: opts.timeout || jetStreamTimeout(entry),
      ...(errors.expired ? { expired: errors.expired } : {}),
      skipped: errors.skipped ?? (() => new NatsModuleError(`${what} produced no acknowledgement`)),
    },
  );
}
