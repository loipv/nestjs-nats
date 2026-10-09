import {
  type ConsumerMessages,
  JetStreamApiCodes,
  JetStreamApiError,
  type JsMsg,
} from '@nats-io/jetstream';
import type { Codec } from '../codec/codec.js';
import { jsonCodec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
import type { NatsStreamManager } from '../jetstream/stream-manager.js';
import { basicSettlement } from '../pipeline/basic-settlement.js';
import { buildInboundPipeline, type Delivery } from '../pipeline/delivery.js';
import { invokeHandler } from '../pipeline/invoker.js';
import type { HandlerAdapter } from './adapter.js';
import { createLimiter, type Limiter } from './limiter.js';

type ConsumeDefinition = HandlerDefinition & { kind: 'consume' };

function errorTrace(err: unknown): string {
  return err instanceof Error ? (err.stack ?? err.message) : String(err);
}

/** Runs a `@Consume` handler on a JetStream pull consumer (`consumer.consume()`). */
export class JetStreamConsumerAdapter implements HandlerAdapter {
  private readonly limiter: Limiter;
  private readonly codec: Codec;
  private readonly run: (d: Delivery) => Promise<void>;
  private readonly label: string;
  private readonly concurrency: number;
  private messages?: ConsumerMessages;
  private loop?: Promise<void>;
  private stopping = false;

  constructor(
    private readonly def: ConsumeDefinition,
    private readonly entry: ConnectionEntry,
    private readonly streams: NatsStreamManager,
  ) {
    this.concurrency = def.options.concurrency ?? 1;
    this.limiter = createLimiter(this.concurrency);
    this.codec = def.options.codec ?? entry.options.codec ?? jsonCodec();
    this.run = buildInboundPipeline(
      { plugins: entry.options.plugins ?? [], core: [basicSettlement] },
      invokeHandler,
    );
    this.label = `${def.className}.${def.methodName} on "${def.options.stream}/${def.options.durable}"`;
  }

  async start(): Promise<void> {
    if (this.messages) return;
    const { stream, durable, config } = this.def.options;
    if (config) await this.streams.ensureConsumer(stream, { ...config, durable_name: durable });
    const consumer = await this.getConsumer(stream, durable);
    const messages = await consumer.consume({ max_messages: Math.max(2 * this.concurrency, 2) });
    this.messages = messages;
    this.loop = this.consume(messages);
  }

  stop(): Promise<void> {
    this.stopping = true;
    const messages = this.messages;
    if (!messages) return Promise.resolve();
    // `close()` resolves only after every buffered message has been yielded, which waits
    // on in-flight handlers; shutdown bounds that wait through `idle()`, so don't await it.
    void messages
      .close()
      .catch((err: unknown) =>
        this.entry.logger.error(`Failed to close consumer of ${this.label}`, errorTrace(err)),
      );
    return Promise.resolve();
  }

  async idle(): Promise<void> {
    await this.loop;
    await this.limiter.idle();
  }

  private async getConsumer(stream: string, durable: string) {
    try {
      return await this.entry.js().consumers.get(stream, durable);
    } catch (err) {
      if (
        err instanceof JetStreamApiError &&
        (err.code === JetStreamApiCodes.ConsumerNotFound ||
          err.code === JetStreamApiCodes.StreamNotFound)
      ) {
        throw new NatsConfigError(
          `Consumer "${stream}/${durable}" not found and no config declared for ${this.def.className}.${this.def.methodName}`,
          { cause: err },
        );
      }
      throw err;
    }
  }

  private async consume(messages: ConsumerMessages): Promise<void> {
    try {
      for await (const msg of messages) {
        await this.limiter.acquire();
        if (this.stopping) {
          // Received but never started: hand it back for immediate redelivery.
          this.limiter.release();
          this.nak(msg);
          continue;
        }
        void this.handle(msg)
          .catch((err: unknown) =>
            this.entry.logger.error(`Failed to process message for ${this.label}`, errorTrace(err)),
          )
          .finally(() => this.limiter.release());
      }
    } catch (err) {
      if (!this.stopping) {
        this.entry.logger.error(`Consumer of ${this.label} failed`, errorTrace(err));
      }
    }
  }

  private nak(msg: JsMsg): void {
    try {
      msg.nak();
    } catch (err) {
      this.entry.logger.error(`Failed to nak message for ${this.label}`, errorTrace(err));
    }
  }

  private async handle(msg: JsMsg): Promise<void> {
    const context = new NatsJetStreamContext({
      kind: 'jetstream',
      connection: this.entry.name,
      raw: msg,
    });
    const d: Delivery = {
      handler: this.def,
      messages: [{ subject: msg.subject, data: msg.data, headers: msg.headers, raw: msg }],
      context,
      codec: this.codec,
      logger: this.entry.logger,
    };
    await this.run(d);
  }
}
