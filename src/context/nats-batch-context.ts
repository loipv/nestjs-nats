import type { JsMsg } from '@nats-io/jetstream';
import type { Msg as NatsMsg } from '@nats-io/transport-node';
import { NatsModuleError } from '../errors/errors.js';
import { ABORT_HANDLER, type NatsContext } from './nats-context.js';

export interface NatsBatchContextInit {
  connection: string;
  messages: ReadonlyArray<NatsContext>;
  /** Records the failure of one of `messages`. */
  onFail: (message: NatsContext, error: unknown) => void;
}

/**
 * Context of a batch handler (`batch` option of `@Consume` and `@ConsumePush`). In the
 * handler, `messages[i]` is the context of the message whose payload is at index `i`
 * of the payload array.
 */
export class NatsBatchContext {
  readonly kind = 'batch' as const;
  readonly connection: string;
  /** The context of each message, a `NatsJetStreamContext`. */
  readonly messages: ReadonlyArray<NatsContext>;
  readonly #onFail: NatsBatchContextInit['onFail'];
  /** Created on first use: most handlers never read `signal`. */
  #abort?: AbortController;

  constructor(init: NatsBatchContextInit) {
    this.connection = init.connection;
    this.messages = Object.freeze([...init.messages]);
    this.#onFail = init.onFail;
  }

  /**
   * Aborted when the batch handler outlives its `timeout`, with the `NatsHandlerTimeoutError`
   * as its reason (the signal of each message is aborted too); never aborted otherwise.
   */
  get signal(): AbortSignal {
    this.#abort ??= new AbortController();
    return this.#abort.signal;
  }

  /** Internal: aborts `signal` (the first reason is kept). */
  [ABORT_HANDLER](reason: unknown): void {
    this.#abort ??= new AbortController();
    this.#abort.abort(reason);
  }

  /**
   * Marks one message as failed while the others succeed: it is retried, dead-lettered
   * or dropped on its own. `target` is its index in `messages` (the payload array in the
   * handler) or its raw message.
   */
  fail(target: number | NatsMsg | JsMsg, error: unknown): void {
    const message =
      typeof target === 'number'
        ? this.messages[target]
        : this.messages.find((m) => m.raw === target);
    if (!message) {
      throw new NatsModuleError(
        typeof target === 'number'
          ? `Cannot fail message ${target} of a batch of ${this.messages.length}`
          : 'Cannot fail a message that is not part of the batch',
      );
    }
    this.#onFail(message, error);
  }
}
