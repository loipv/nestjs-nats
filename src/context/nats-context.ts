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

  get replyTo(): string | undefined {
    // JsMsg does not declare `reply` in its type, but the underlying message carries it.
    return (this.#raw as { reply?: string }).reply || undefined;
  }

  get raw(): R {
    return this.#raw;
  }
}

export class NatsJetStreamContext extends NatsContext<JsMsg> {
  #settled = false;

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

  ack(): void {
    if (this.#settled) return;
    this.#settled = true;
    this.raw.ack();
  }

  nak(delayMs?: number): void {
    if (this.#settled) return;
    this.#settled = true;
    this.raw.nak(delayMs);
  }

  term(reason?: string): void {
    if (this.#settled) return;
    this.#settled = true;
    this.raw.term(reason);
  }

  working(): void {
    this.raw.working();
  }
}
