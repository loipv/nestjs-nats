import {
  type ConsumerMessages,
  JetStreamApiCodes,
  JetStreamApiError,
  type JsMsg,
} from '@nats-io/jetstream';
import { type ResolvedBatchOptions, resolveBatchOptions } from '../batch/collector.js';
import type { Codec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
import type { NatsStreamManager } from '../jetstream/stream-manager.js';
import {
  createDelivery,
  type HandlerInfo,
  type InboundMessage,
  type InternalDelivery,
} from '../pipeline/delivery.js';
import { backoffDelay, DEFAULT_RETRY, type RetryPolicy } from '../settlement/retry.js';
import { errorTrace } from '../util/errors.js';
import type { ForceStopResult, HandlerAdapter } from './adapter.js';
import { Dispatcher, type DispatchItem } from './dispatcher.js';
import { createHandlerPipeline, defaultPipelineDeps } from './handler-pipeline.js';

type ConsumeDefinition = HandlerDefinition & { kind: 'consume' };

const isPowerOfTwo = (n: number): boolean => n >= 1 && (n & (n - 1)) === 0;

/** Runs a `@Consume` handler on a JetStream pull consumer (`consumer.consume()`). */
export class JetStreamConsumerAdapter implements HandlerAdapter {
  private readonly dispatcher: Dispatcher<JsMsg>;
  private readonly codec: Codec;
  private readonly retry: RetryPolicy | false;
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly info: HandlerInfo;
  private readonly label: string;
  private readonly concurrency: number;
  private readonly batch?: ResolvedBatchOptions;
  private messages?: ConsumerMessages;
  private loop?: Promise<void>;
  private stopping = false;
  /** Contexts of the messages being processed (every message of a batch). */
  private readonly inFlight = new Set<NatsJetStreamContext>();
  /**
   * Every message received and not yet given up on: waiting for a slot, being collected,
   * in a batch waiting for a slot, or being processed (then with its context, which tells
   * whether it is already settled). The heartbeat extends their ack deadline.
   */
  private readonly unsettled = new Map<JsMsg, NatsJetStreamContext | undefined>();
  private heartbeat?: ReturnType<typeof setInterval>;

  constructor(
    private readonly def: ConsumeDefinition,
    private readonly entry: ConnectionEntry,
    private readonly streams: NatsStreamManager,
  ) {
    this.concurrency = def.options.concurrency ?? 1;
    const pipeline = createHandlerPipeline(def, entry, defaultPipelineDeps);
    this.codec = pipeline.codec;
    this.info = pipeline.info;
    this.retry = pipeline.retry;
    this.run = pipeline.run;
    this.label = `${def.className}.${def.methodName} on "${def.options.stream}/${def.options.durable}"`;
    this.batch = resolveBatchOptions(def.options.batch);
    this.dispatcher = new Dispatcher<JsMsg>({
      concurrency: this.concurrency,
      ...(this.batch ? { batch: this.batch } : {}),
      process: (items) => this.process(items),
      logger: entry.logger,
      label: this.label,
      // Received but never started: hand it back for immediate redelivery.
      onRejected: (msg) => {
        this.unsettled.delete(msg);
        this.nak(msg);
      },
    });
  }

  async start(): Promise<void> {
    if (this.messages) return;
    const { stream, durable, config } = this.def.options;
    if (config) await this.streams.ensureConsumer(stream, { ...config, durable_name: durable });
    const consumer = await this.getConsumer(stream, durable, config !== undefined);
    const { config: actual } = await consumer.info(true);
    this.checkMaxDeliver(actual.max_deliver);
    // `ack_wait` is in nanoseconds.
    const ackWaitMs = (actual.ack_wait ?? 0) / 1e6;
    this.checkBatchWait(ackWaitMs);
    // Just enough pulled messages to keep every slot busy, or to fill a batch: they wait
    // for a slot meanwhile. The heartbeat covers every message the client has handed over;
    // while every slot is busy, up to `max_messages - 1` more can still sit in the client's
    // own buffer, out of its reach.
    const messages = await consumer.consume({
      max_messages: this.batch ? Math.max(this.batch.maxSize, this.concurrency) : this.concurrency,
    });
    if (this.stopping) {
      // stop() ran while start() was awaiting: nothing will ever read these messages.
      await messages.close();
      return;
    }
    this.messages = messages;
    this.startHeartbeat(ackWaitMs);
    this.loop = this.consume(messages);
    void this.watchStatus(messages);
  }

  stop(): Promise<void> {
    this.stopping = true;
    // Messages still being processed keep their heartbeat until `idle()` / `forceStop()`.
    if (this.unsettled.size === 0) this.stopHeartbeat();
    // Hands the batch being collected to the handler; `idle()` waits for it.
    void this.dispatcher.stop();
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
    if (this.stopping) this.stopHeartbeat();
  }

  forceStop(): ForceStopResult {
    this.stopHeartbeat();
    let nakked = 0;
    const nak = (settle: () => void) => {
      try {
        settle();
        nakked++;
      } catch (err) {
        this.entry.logger.error(`Failed to nak message for ${this.label}`, errorTrace(err));
      }
    };
    for (const context of this.inFlight) {
      if (!context.settled) nak(() => context.nak());
    }
    // Received but not started (collected, or in a batch waiting for a slot).
    for (const msg of this.dispatcher.abandon()) nak(() => msg.nak());
    this.unsettled.clear();
    return { nakked, cancelledRetries: this.dispatcher.cancelRetries() };
  }

  /** The server stops redelivering after `max_deliver`: warn when that is before the DLQ. */
  private checkMaxDeliver(maxDeliver: number | undefined): void {
    if (!this.retry || maxDeliver === undefined || maxDeliver <= 0) return;
    if (maxDeliver > this.retry.maxRetries) return;
    const { stream, durable } = this.def.options;
    this.entry.logger.warn(
      `Consumer ${stream}/${durable} for ${this.def.className}.${this.def.methodName} has max_deliver ${maxDeliver} ≤ maxRetries ${this.retry.maxRetries}; the server will stop redelivering before the DLQ is reached`,
    );
  }

  /**
   * Collected messages are kept alive by the heartbeat only: when a batch waits as long
   * as `ack_wait`, a single late heartbeat (e.g. a blocked event loop) is enough for the
   * server to redeliver them while they are still collected.
   */
  private checkBatchWait(ackWaitMs: number): void {
    if (!this.batch || ackWaitMs <= 0 || this.batch.maxWait < ackWaitMs) return;
    const { stream, durable } = this.def.options;
    this.entry.logger.warn(
      `${this.def.className}.${this.def.methodName} on "${stream}/${durable}" has batch.maxWait ${this.batch.maxWait} ms >= the consumer's ack_wait ${ackWaitMs} ms; collected messages rely on heartbeats alone and may be redelivered and processed twice if one is late. Lower batch.maxWait or raise ack_wait`,
    );
  }

  /**
   * Extends the ack deadline of every received, unsettled message twice per `ack_wait`:
   * the ones being processed and the ones still waiting (for a slot, or in a batch).
   */
  private startHeartbeat(ackWaitMs: number): void {
    if (ackWaitMs <= 0 || this.heartbeat) return;
    this.heartbeat = setInterval(() => this.beat(), ackWaitMs / 2);
    // The heartbeat must not keep the process alive.
    this.heartbeat.unref();
  }

  private stopHeartbeat(): void {
    if (this.heartbeat === undefined) return;
    clearInterval(this.heartbeat);
    this.heartbeat = undefined;
  }

  private beat(): void {
    for (const [msg, context] of this.unsettled) {
      if (context?.settled) continue;
      try {
        msg.working();
      } catch (err) {
        this.entry.logger.error(`Failed to extend ack deadline for ${this.label}`, errorTrace(err));
      }
    }
    if (this.stopping && this.unsettled.size === 0) this.stopHeartbeat();
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
      for await (const msg of messages) {
        // Heartbeated from now on, while it waits for a slot or a batch.
        this.unsettled.set(msg, undefined);
        await this.dispatcher.push(msg, msg.info.deliveryCount);
      }
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
    if (this.batch) return this.handle(items, true);
    for (const item of items) await this.handle([item], false);
  }

  private async handle(items: DispatchItem<JsMsg>[], batch: boolean): Promise<void> {
    const parts = items.map(({ msg, attempt }) => ({
      message: {
        subject: msg.subject,
        data: msg.data,
        headers: msg.headers,
        raw: msg,
        attempt,
      } satisfies InboundMessage,
      context: new NatsJetStreamContext({
        kind: 'jetstream',
        connection: this.entry.name,
        raw: msg,
      }),
    }));
    const d = createDelivery({
      definition: this.def,
      handler: this.info,
      connection: this.entry.name,
      batch,
      parts,
      codec: this.codec,
      logger: this.entry.logger,
    });
    for (const { context } of parts) {
      this.inFlight.add(context);
      this.unsettled.set(context.raw, context);
    }
    try {
      await this.run(d);
    } catch (err) {
      // Reached only when the pipeline failed outside settlement (e.g. a plugin threw):
      // hand the messages back with the retry backoff.
      this.entry.logger.error(`Handler ${this.label} failed`, errorTrace(err));
      for (const { message, context } of parts) {
        if (context.settled) continue;
        try {
          context.nak(backoffDelay(this.retry || DEFAULT_RETRY, message.attempt));
        } catch (nakErr) {
          this.entry.logger.error(`Failed to nak message for ${this.label}`, errorTrace(nakErr));
        }
      }
    } finally {
      // Settled by now (or left to the handler with `ack: 'manual'`): no more heartbeats.
      for (const { context } of parts) {
        this.inFlight.delete(context);
        this.unsettled.delete(context.raw);
      }
    }
  }
}
