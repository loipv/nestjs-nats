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
import {
  buildInboundPipeline,
  HANDLER_DEFINITION,
  type HandlerInfo,
  type InternalDelivery,
  toHandlerInfo,
} from '../pipeline/delivery.js';
import { invokeHandler } from '../pipeline/invoker.js';
import { errorTrace } from '../util/errors.js';
import type { ForceStopResult, HandlerAdapter } from './adapter.js';
import { Dispatcher, type DispatchItem } from './dispatcher.js';

type ConsumeDefinition = HandlerDefinition & { kind: 'consume' };

const isPowerOfTwo = (n: number): boolean => n >= 1 && (n & (n - 1)) === 0;

/** Runs a `@Consume` handler on a JetStream pull consumer (`consumer.consume()`). */
export class JetStreamConsumerAdapter implements HandlerAdapter {
  private readonly dispatcher: Dispatcher<JsMsg>;
  private readonly codec: Codec;
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly info: HandlerInfo;
  private readonly label: string;
  private readonly concurrency: number;
  private messages?: ConsumerMessages;
  private loop?: Promise<void>;
  private stopping = false;
  /** Contexts of the messages being processed. */
  private readonly inFlight = new Set<NatsJetStreamContext>();

  constructor(
    private readonly def: ConsumeDefinition,
    private readonly entry: ConnectionEntry,
    private readonly streams: NatsStreamManager,
  ) {
    this.concurrency = def.options.concurrency ?? 1;
    this.codec = def.options.codec ?? entry.options.codec ?? jsonCodec();
    this.info = toHandlerInfo(def);
    this.run = buildInboundPipeline(
      { plugins: entry.options.plugins ?? [], core: [basicSettlement] },
      invokeHandler,
    );
    this.label = `${def.className}.${def.methodName} on "${def.options.stream}/${def.options.durable}"`;
    this.dispatcher = new Dispatcher<JsMsg>({
      concurrency: this.concurrency,
      process: (items) => this.process(items),
      logger: entry.logger,
      label: this.label,
      // Received but never started: hand it back for immediate redelivery.
      onRejected: (msg) => this.nak(msg),
    });
  }

  async start(): Promise<void> {
    if (this.messages) return;
    const { stream, durable, config } = this.def.options;
    if (config) await this.streams.ensureConsumer(stream, { ...config, durable_name: durable });
    const consumer = await this.getConsumer(stream, durable, config !== undefined);
    const messages = await consumer.consume({ max_messages: Math.max(2 * this.concurrency, 2) });
    if (this.stopping) {
      // stop() ran while start() was awaiting: nothing will ever read these messages.
      await messages.close();
      return;
    }
    this.messages = messages;
    this.loop = this.consume(messages);
    void this.watchStatus(messages);
  }

  stop(): Promise<void> {
    this.stopping = true;
    this.dispatcher.stop();
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
    await this.dispatcher.idle();
  }

  forceStop(): ForceStopResult {
    let nakked = 0;
    for (const context of this.inFlight) {
      if (context.settled) continue;
      try {
        context.nak();
        nakked++;
      } catch (err) {
        this.entry.logger.error(`Failed to nak message for ${this.label}`, errorTrace(err));
      }
    }
    return { nakked, cancelledRetries: this.dispatcher.cancelRetries() };
  }

  private async getConsumer(stream: string, durable: string, configDeclared: boolean) {
    try {
      return await this.entry.js().consumers.get(stream, durable);
    } catch (err) {
      if (
        err instanceof JetStreamApiError &&
        (err.code === JetStreamApiCodes.ConsumerNotFound ||
          err.code === JetStreamApiCodes.StreamNotFound)
      ) {
        throw new NatsConfigError(
          configDeclared
            ? `Consumer "${stream}/${durable}" not found for ${this.def.className}.${this.def.methodName}; its config is declared but the JetStream sync mode did not create it`
            : `Consumer "${stream}/${durable}" not found and no config declared for ${this.def.className}.${this.def.methodName}`,
          { cause: err },
        );
      }
      throw err;
    }
  }

  private async consume(messages: ConsumerMessages): Promise<void> {
    try {
      for await (const msg of messages) await this.dispatcher.push(msg, msg.info.deliveryCount);
    } catch (err) {
      if (!this.stopping) {
        this.entry.logger.error(`Consumer of ${this.label} failed`, errorTrace(err));
      }
      return;
    }
    if (!this.stopping) {
      this.entry.logger.error(
        `Consumer of ${this.label} ended unexpectedly; no more messages will be received`,
      );
    }
  }

  /** Surfaces consumer problems the library only reports through `status()` while it retries. */
  private async watchStatus(messages: ConsumerMessages): Promise<void> {
    try {
      for await (const n of messages.status()) {
        if (this.stopping) return;
        switch (n.type) {
          case 'consumer_deleted':
            this.entry.logger.error(
              `Consumer of ${this.label} was deleted; no messages will be received until it is recreated`,
            );
            break;
          case 'consumer_not_found':
          case 'stream_not_found':
          case 'heartbeats_missed':
            // The library retries forever; log on exponentially spaced attempts only.
            if (!('count' in n) || isPowerOfTwo(n.count)) {
              const attempt = 'count' in n ? ` (attempt ${n.count})` : '';
              this.entry.logger.warn(`Consumer of ${this.label} reported ${n.type}${attempt}`);
            }
            break;
          default:
            break;
        }
      }
    } catch (err) {
      if (!this.stopping) {
        this.entry.logger.warn(`Status watch of ${this.label} failed: ${errorTrace(err)}`);
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

  private async process(items: DispatchItem<JsMsg>[]): Promise<void> {
    for (const item of items) await this.handle(item);
  }

  private async handle({ msg, attempt }: DispatchItem<JsMsg>): Promise<void> {
    const context = new NatsJetStreamContext({
      kind: 'jetstream',
      connection: this.entry.name,
      raw: msg,
    });
    const d: InternalDelivery = {
      handler: this.info,
      [HANDLER_DEFINITION]: this.def,
      messages: [{ subject: msg.subject, data: msg.data, headers: msg.headers, raw: msg, attempt }],
      context,
      codec: this.codec,
      logger: this.entry.logger,
    };
    this.inFlight.add(context);
    try {
      await this.run(d);
    } catch (err) {
      // Reached only when the pipeline failed outside settlement (e.g. a plugin threw).
      this.entry.logger.error(`Handler ${this.label} failed`, errorTrace(err));
      if (!context.settled) {
        try {
          context.nak();
        } catch (nakErr) {
          this.entry.logger.error(`Failed to nak message for ${this.label}`, errorTrace(nakErr));
        }
      }
    } finally {
      this.inFlight.delete(context);
    }
  }
}
