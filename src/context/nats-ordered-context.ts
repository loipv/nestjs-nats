import type { JsMsg } from '@nats-io/jetstream';
import { NatsConfigError } from '../errors/errors.js';
import { NatsContext } from './nats-context.js';

export interface NatsOrderedContextInit {
  connection: string;
  raw: JsMsg;
  /** 1-based attempt of the handler on this message (in-place retries). */
  attempt: number;
}

const notSettleable = () => new NatsConfigError('ordered consumer messages cannot be settled');

/**
 * Context of an `@ConsumeOrdered` handler. An ordered consumer acknowledges nothing: a
 * failed message is retried in place, then dead-lettered or dropped, so `ack()`, `nak()`,
 * `term()` and `working()` throw a `NatsConfigError`.
 */
export class NatsOrderedContext extends NatsContext<JsMsg> {
  readonly #attempt: number;

  constructor(init: NatsOrderedContextInit) {
    super({ kind: 'ordered', connection: init.connection, raw: init.raw });
    this.#attempt = init.attempt;
  }

  get stream(): string {
    return this.raw.info.stream;
  }

  get streamSequence(): number {
    return this.raw.info.streamSequence;
  }

  /** The consumer's delivery sequence (restarts when the consumer is recreated). */
  get deliverySequence(): number {
    return this.raw.info.deliverySequence;
  }

  /** Messages left in the stream for the consumer after this one. */
  get pending(): number {
    return this.raw.info.pending;
  }

  /** 1-based attempt of the handler on this message. */
  get attempt(): number {
    return this.#attempt;
  }

  ack(): never {
    throw notSettleable();
  }

  nak(_delayMs?: number): never {
    throw notSettleable();
  }

  term(_reason?: string): never {
    throw notSettleable();
  }

  working(): never {
    throw notSettleable();
  }
}
