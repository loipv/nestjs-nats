import type { BatchAck, PubAck, StreamExpectations } from '@nats-io/jetstream';
import type { MsgHdrs } from '@nats-io/transport-node';
import type { CloudEventInit } from '../cloudevents/types.js';
import type { Codec } from '../codec/codec.js';
import type { NatsHeadersInit } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { redactError, redactText, serverSecrets } from '../connection/redact.js';
import { NatsAtomicPublishError, NatsConfigError } from '../errors/errors.js';
import { compose } from '../pipeline/compose.js';
import {
  createOutgoingMessage,
  encodeOutgoing,
  outboundMiddlewares,
} from '../pipeline/outbound.js';
import { errorMessage } from '../util/errors.js';
import { MSG_ID_HEADER, publishOutgoing } from './publish.js';

/** The most messages the server accepts in one atomic batch. */
export const MAX_ATOMIC_MESSAGES = 1000;
/** The commit timeout (ms) when the JetStream client has none. */
export const DEFAULT_ATOMIC_TIMEOUT = 5000;

export interface AtomicMessage {
  subject: string;
  data?: unknown;
  headers?: NatsHeadersInit;
  codec?: Codec;
  cloudEvent?: CloudEventInit;
}

export interface AtomicPublishOptions {
  /** Timeout (ms) of the commit's ack. Defaults to the JetStream client's timeout; never 0. */
  timeout?: number;
  /** Expectations checked on the first message (there is no `lastMsgID`). */
  expect?: Partial<Omit<StreamExpectations, 'lastMsgID'>>;
}

export interface AtomicPublishAck {
  stream: string;
  /** The sequence of the last message. */
  seq: number;
  /** The number of messages stored. */
  count: number;
  /** The `Nats-Batch-Id`; unset for a single message. */
  batch?: string;
  duplicate: false;
  domain?: string;
}

interface Staged {
  subject: string;
  payload: Uint8Array;
  headers: MsgHdrs;
}

/**
 * Publishes `messages` as one atomic batch. Each message runs through the outbound plugins;
 * their terminal step only stages the encoded message and waits for the commit, sent once
 * every message is staged, so a plugin's `next()` settles with the commit. A plugin failing
 * before its message is staged aborts the batch: nothing is sent.
 */
export async function publishAtomic(
  entry: ConnectionEntry,
  messages: readonly AtomicMessage[],
  opts: AtomicPublishOptions = {},
): Promise<AtomicPublishAck> {
  const count = messages.length;
  if (count < 1 || count > MAX_ATOMIC_MESSAGES) {
    throw new NatsConfigError(`publishAtomic needs 1 to ${MAX_ATOMIC_MESSAGES} messages`);
  }
  if (opts.timeout !== undefined && !(opts.timeout > 0)) {
    throw new NatsConfigError('publishAtomic timeout must be > 0');
  }
  const outgoing = messages.map((m, index) => {
    const message = createOutgoingMessage(entry, 'jetstream', m.subject, m.data, m);
    if (message.headers.has(MSG_ID_HEADER, 'insensitive')) {
      throw new NatsConfigError(
        `publishAtomic message ${index} has a ${MSG_ID_HEADER} header: the messages of an atomic batch are not de-duplicated`,
      );
    }
    message.atomic = { index, count };
    return message;
  });
  // `getOptions` is missing on the fake JetStream client of /testing.
  const timeout = opts.timeout ?? entry.js().getOptions?.().timeout ?? DEFAULT_ATOMIC_TIMEOUT;
  const subject = outgoing[0]?.subject ?? '';
  const what = `Atomic publish of ${count} message${count === 1 ? '' : 's'} to "${subject}"`;
  const failed = (err: unknown): NatsAtomicPublishError => {
    const { pass, token, servers } = entry.options.connection;
    const secrets = [pass, token, ...serverSecrets(servers)];
    return new NatsAtomicPublishError(`${what} failed: ${redactText(errorMessage(err), secrets)}`, {
      subject,
      count,
      cause: redactError(err, secrets),
    });
  };
  const abortError = (reason: string, cause?: unknown): NatsAtomicPublishError =>
    new NatsAtomicPublishError(`${what} aborted: ${reason}`, { subject, count, cause });

  const [first] = outgoing;
  if (count === 1 && first) {
    const ack = await publishOutgoing(entry, first, { ...opts, timeout }, failed);
    return toAck(ack, 1);
  }

  const staged = new Array<Staged>(count);
  let stagedCount = 0;
  let sending: Promise<AtomicPublishAck> | undefined;
  let aborted = false;
  let resolveCommit!: () => void;
  let rejectCommit!: (err: unknown) => void;
  const committed = new Promise<void>((resolve, reject) => {
    resolveCommit = resolve;
    rejectCommit = reject;
  });
  // Nothing may be waiting on it when the batch is aborted.
  committed.catch(() => {});
  /** Aborts the batch unless it was sent (then its commit settles every plugin's next()). */
  const abort = (err: NatsAtomicPublishError): void => {
    if (sending) return;
    aborted = true;
    rejectCommit(err);
  };

  const send = async (): Promise<AtomicPublishAck> => {
    // Called once every message is staged.
    const head = staged[0] as Staged;
    const last = staged[count - 1] as Staged;
    try {
      const batch = await entry.js().startBatch(head.subject, head.payload, {
        headers: head.headers,
        timeout,
        ...(opts.expect ? { expect: opts.expect } : {}),
      });
      for (const m of staged.slice(1, -1)) batch.add(m.subject, m.payload, { headers: m.headers });
      // Explicit rather than nats.js's fallback: its commit passes 0, which its publish
      // replaces with the client's timeout.
      const ack = await batch.commit(last.subject, last.payload, {
        headers: last.headers,
        timeout,
      });
      return toAck(ack, ack.count, ack.batch);
    } catch (err) {
      throw failed(err);
    }
  };

  const middlewares = outboundMiddlewares(entry);
  const chains = outgoing.map((message, index) =>
    compose(middlewares, async (m) => {
      staged[index] = { subject: m.subject, payload: encodeOutgoing(m), headers: m.headers };
      // A chain may fail after staging its own message, before the last one is staged.
      if (++stagedCount === count && !aborted) {
        sending = send();
        sending.then(resolveCommit, rejectCommit);
      }
      await committed;
    })(message).then(
      () => {
        if (staged[index]) return;
        const err = abortError(`an outbound plugin did not pass message ${index} on`);
        abort(err);
        throw err;
      },
      (err: unknown) => {
        abort(abortError(`message ${index} failed in an outbound plugin`, err));
        throw err;
      },
    ),
  );
  await Promise.all(chains);
  // Every message was staged, so the batch was sent (a plugin may have swallowed its error).
  return sending as Promise<AtomicPublishAck>;
}

function toAck(ack: PubAck | BatchAck, count: number, batch?: string): AtomicPublishAck {
  return {
    stream: ack.stream,
    seq: ack.seq,
    count,
    batch,
    duplicate: false,
    ...(ack.domain !== undefined ? { domain: ack.domain } : {}),
  };
}
