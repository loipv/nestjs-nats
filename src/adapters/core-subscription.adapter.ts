import { ServiceErrorCodeHeader, ServiceErrorHeader } from '@nats-io/services';
import { headers, type Msg, type Subscription } from '@nats-io/transport-node';
import { type Codec, encodePayload, jsonCodec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsContext } from '../context/nats-context.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { basicSettlement } from '../pipeline/basic-settlement.js';
import {
  buildInboundPipeline,
  HANDLER_DEFINITION,
  type HandlerInfo,
  type InternalDelivery,
  toHandlerInfo,
} from '../pipeline/delivery.js';
import { invokeHandler } from '../pipeline/invoker.js';
import { toServiceError } from '../pipeline/reply.js';
import type { HandlerAdapter } from './adapter.js';
import { createLimiter, type Limiter } from './limiter.js';

type SubscribeDefinition = HandlerDefinition & { kind: 'subscribe' };

const EMPTY = new Uint8Array(0);

function errorTrace(err: unknown): string {
  return err instanceof Error ? (err.stack ?? err.message) : String(err);
}

/** Runs a `@Subscribe` handler on a core NATS subscription (optionally in a queue group). */
export class CoreSubscriptionAdapter implements HandlerAdapter {
  private readonly limiter: Limiter;
  private readonly codec: Codec;
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly info: HandlerInfo;
  private readonly label: string;
  private sub?: Subscription;
  private loop?: Promise<void>;

  constructor(
    private readonly def: SubscribeDefinition,
    private readonly entry: ConnectionEntry,
  ) {
    this.limiter = createLimiter(def.options.concurrency ?? 1);
    this.codec = def.options.codec ?? entry.options.codec ?? jsonCodec();
    this.info = toHandlerInfo(def);
    this.run = buildInboundPipeline(
      { plugins: entry.options.plugins ?? [], core: [basicSettlement] },
      invokeHandler,
    );
    this.label = `${def.className}.${def.methodName} on "${def.subject}"`;
  }

  async start(): Promise<void> {
    if (this.sub) return;
    const { queue } = this.def.options;
    const sub = this.entry.nc.subscribe(this.def.subject, queue ? { queue } : {});
    this.sub = sub;
    this.loop = this.consume(sub);
    // Make sure the server knows about the subscription before bootstrap completes.
    await this.entry.nc.flush();
  }

  async stop(): Promise<void> {
    const sub = this.sub;
    if (!sub || sub.isClosed() || this.entry.nc.isClosed()) return;
    try {
      await sub.drain();
    } catch (err) {
      this.entry.logger.error(`Failed to drain subscription of ${this.label}`, errorTrace(err));
    }
  }

  async idle(): Promise<void> {
    // The loop ends once the drained subscription has yielded its buffered messages.
    await this.loop;
    await this.limiter.idle();
  }

  private async consume(sub: Subscription): Promise<void> {
    try {
      for await (const msg of sub) {
        await this.limiter.acquire();
        void this.handle(msg)
          .catch((err: unknown) =>
            this.entry.logger.error(`Failed to process message for ${this.label}`, errorTrace(err)),
          )
          .finally(() => this.limiter.release());
      }
    } catch (err) {
      this.entry.logger.error(`Subscription of ${this.label} failed`, errorTrace(err));
    }
  }

  private async handle(msg: Msg): Promise<void> {
    const context = new NatsContext({ kind: 'core', connection: this.entry.name, raw: msg });
    const d: InternalDelivery = {
      handler: this.info,
      [HANDLER_DEFINITION]: this.def,
      messages: [{ subject: msg.subject, data: msg.data, headers: msg.headers, raw: msg }],
      context,
      codec: this.codec,
      logger: this.entry.logger,
    };
    try {
      await this.run(d);
    } catch (err) {
      if (context.replyTo) this.respondError(msg, err);
      else this.entry.logger.error(`Handler ${this.label} failed`, errorTrace(err));
      return;
    }
    if (!context.replyTo || d.result === undefined) return;
    let payload: Uint8Array;
    try {
      payload = encodePayload(this.codec, d.result);
    } catch (err) {
      this.entry.logger.error(`Failed to encode reply of ${this.label}`, errorTrace(err));
      this.respondError(msg, err);
      return;
    }
    this.respond(msg, () => msg.respond(payload));
  }

  private respondError(msg: Msg, err: unknown): void {
    const { code, description } = toServiceError(err);
    this.respond(msg, () => {
      const h = headers();
      // Header values must not contain CR/LF.
      h.set(ServiceErrorHeader, description.replace(/[\r\n]+/g, ' '));
      h.set(ServiceErrorCodeHeader, String(code));
      return msg.respond(EMPTY, { headers: h });
    });
  }

  private respond(msg: Msg, send: () => boolean): void {
    try {
      send();
    } catch (err) {
      this.entry.logger.error(
        `Failed to reply to "${msg.reply}" for ${this.label}`,
        errorTrace(err),
      );
    }
  }
}
