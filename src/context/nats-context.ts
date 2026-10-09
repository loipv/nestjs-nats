import type { JsMsg } from '@nats-io/jetstream';
import type { Msg as NatsMsg } from '@nats-io/transport-node';
import { fromMsgHdrs } from '../codec/headers.js';

export type NatsContextKind = 'core' | 'jetstream' | 'service';

export interface NatsContextInit<R extends NatsMsg | JsMsg = NatsMsg | JsMsg> {
  kind: NatsContextKind;
  connection: string;
  raw: R;
}

export class NatsContext<R extends NatsMsg | JsMsg = NatsMsg | JsMsg> {
  readonly #kind: NatsContextKind;
  readonly #connection: string;
  readonly #raw: R;
  #headers?: Record<string, string | string[]>;

  constructor(init: NatsContextInit<R>) {
    this.#kind = init.kind;
    this.#connection = init.connection;
    this.#raw = init.raw;
  }

  get kind(): NatsContextKind {
    return this.#kind;
  }

  get connection(): string {
    return this.#connection;
  }

  get subject(): string {
    return this.#raw.subject;
  }

  get headers(): Record<string, string | string[]> {
    this.#headers ??= fromMsgHdrs(this.#raw.headers);
    return this.#headers;
  }

  /** The subject to reply to, if the sender expects a reply (never for JetStream messages). */
  get replyTo(): string | undefined {
    // A JetStream message's reply subject is its `$JS.ACK...` subject, not a reply target.
    if (this.#kind === 'jetstream') return undefined;
    return (this.#raw as NatsMsg).reply || undefined;
  }

  get raw(): R {
    return this.#raw;
  }
}

/** Internal: key of the settle call made on a JetStream context (see `NatsJetStreamContext`). */
export const SETTLED_BY: unique symbol = Symbol('nats.settledBy');

/** Internal: the settle call (`ack`/`nak`/`term`) that settled a JetStream message. */
export type SettleCall = { op: 'ack' } | { op: 'nak'; delayMs: number } | { op: 'term' };

export class NatsJetStreamContext extends NatsContext<JsMsg> {
  #settled = false;
  #settledBy?: SettleCall;

  /**
   * Always `undefined`: a JetStream message's reply subject is its `$JS.ACK...` subject,
   * and publishing to it would acknowledge the message. Use `ack()`/`nak()`/`term()`.
   */
  override get replyTo(): string | undefined {
    return undefined;
  }

  get stream(): string {
    return this.raw.info.stream;
  }

  get seq(): number {
    return this.raw.info.streamSequence;
  }

  get deliveryCount(): number {
    return this.raw.info.deliveryCount;
  }

  get settled(): boolean {
    return this.#settled;
  }

  /** Internal: which call settled the message (`undefined` while unsettled); read by the settlement. */
  get [SETTLED_BY](): SettleCall | undefined {
    return this.#settledBy;
  }

  // Each settle call marks the message settled only once the raw call succeeded: if it
  // throws, the error reaches the caller and the message can still be settled later.

  ack(): void {
    if (this.#settled) return;
    this.raw.ack();
    this.#settled = true;
    this.#settledBy = { op: 'ack' };
  }

  nak(delayMs?: number): void {
    if (this.#settled) return;
    this.raw.nak(delayMs);
    this.#settled = true;
    this.#settledBy = { op: 'nak', delayMs: delayMs ?? 0 };
  }

  term(reason?: string): void {
    if (this.#settled) return;
    this.raw.term(reason);
    this.#settled = true;
    this.#settledBy = { op: 'term' };
  }

  working(): void {
    this.raw.working();
  }
}
