import {
  type ConsumerConfig,
  type ConsumerMessages,
  JetStreamApiCodes,
  JetStreamApiError,
  type JsMsg,
} from '@nats-io/jetstream';
import { type ResolvedBatchOptions, resolveBatchOptions } from '../batch/collector.js';
import type { Codec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import type { ConsumePushOptions } from '../decorators/handler.decorators.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
import type { NatsStreamManager } from '../jetstream/stream-manager.js';
import { pushConsumerDefaults } from '../jetstream/stream-sync.js';
import {
  createDelivery,
  type HandlerInfo,
  type InboundMessage,
  type InternalDelivery,
} from '../pipeline/delivery.js';
import { backoffDelay, DEFAULT_RETRY, type RetryPolicy } from '../settlement/retry.js';
import { errorTrace } from '../util/errors.js';
import { isPowerOfTwo } from '../util/power-of-two.js';
import {
  type ForceStopResult,
  type HandlerAdapter,
  NO_RESTART,
  type RestartBinding,
} from './adapter.js';
import { Dispatcher, type DispatchItem } from './dispatcher.js';
import { createHandlerPipeline, defaultPipelineDeps } from './handler-pipeline.js';
import { RestartSupervisor } from './restart-supervisor.js';

type ConsumeDefinition = HandlerDefinition & { kind: 'consume' };

/** nats.js's refusal to bind a push consumer that another subscriber already bound. */
const isAlreadyBound = (err: unknown): err is Error =>
  err instanceof Error &&
  err.name === 'InvalidOperationError' &&
  err.message.includes('already bound');

/** A bound consumer: its config as the server reports it, and how to start receiving. */
interface Intake {
  config: ConsumerConfig;
  consume(): Promise<ConsumerMessages>;
}

/**
 * Runs a `@Consume` handler on a JetStream pull consumer (`consumer.consume()`), or a
 * `@ConsumePush` handler on a durable push consumer.
 */
export class JetStreamConsumerAdapter implements HandlerAdapter {
  private readonly dispatcher: Dispatcher<JsMsg>;
  private readonly codec: Codec;
  private readonly retry: RetryPolicy | false;
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly info: HandlerInfo;
  private readonly label: string;
  private readonly concurrency: number;
  private readonly batch?: ResolvedBatchOptions;
  /** `@ConsumePush`: messages are read as soon as they arrive, not when a slot is free. */
  private readonly push: boolean;
  /** Messages the handler can hold at once: every slot busy, or a full batch. */
  private readonly capacity: number;
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
  private readonly supervisor: RestartSupervisor;

  constructor(
    private readonly def: ConsumeDefinition,
    private readonly entry: ConnectionEntry,
    private readonly streams: NatsStreamManager,
    restart: RestartBinding = NO_RESTART,
  ) {
    this.concurrency = def.options.concurrency ?? 1;
    const pipeline = createHandlerPipeline(def, entry, defaultPipelineDeps);
    this.codec = pipeline.codec;
    this.info = pipeline.info;
    this.retry = pipeline.retry;
    this.run = pipeline.run;
    this.push = def.delivery === 'push';
    this.label = `${def.className}.${def.methodName} on "${def.options.stream}/${def.options.durable}"${this.push ? ' (push)' : ''}`;
    this.batch = resolveBatchOptions(def.options.batch);
    this.capacity = this.batch ? Math.max(this.batch.maxSize, this.concurrency) : this.concurrency;
    this.supervisor = new RestartSupervisor({
      policy: restart.policy,
      label: this.label,
      logger: entry.logger,
      isConnectionClosed: () => entry.nc.isClosed(),
      reopen: () => this.open(false),
      onState: (() => {
        const track = restart.track(this.label);
        return (state) => {
          track(state);
          // Nothing will be pushed anymore: let the batch being collected settle.
          if (state === 'failed') void this.dispatcher.stop();
        };
      })(),
    });
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
    await this.open(true);
    this.supervisor.started();
  }

  /**
   * Binds the consumer and starts reading it. The dispatcher, the heartbeat and the
   * messages received so far are kept across a reopen; config warnings are logged on
   * the `first` open only.
   */
  private async open(first: boolean): Promise<void> {
    const intake = this.push ? await this.openPush() : await this.openPull();
    const { config: actual } = intake;
    if (first) this.checkMaxDeliver(actual.max_deliver);
    // `ack_wait` is in nanoseconds.
    const ackWaitMs = (actual.ack_wait ?? 0) / 1e6;
    if (first) this.checkBatchWait(ackWaitMs);
    const messages = await intake.consume();
    if (this.stopping) {
      // stop() ran while opening: nothing will ever read these messages.
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
    this.supervisor.stop();
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
    await this.supervisor.settled();
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

  private async openPull(): Promise<Intake> {
    const { stream, durable, config } = this.def.options;
    if (config) await this.streams.ensureConsumer(stream, { ...config, durable_name: durable });
    const consumer = await this.lookup(
      () => this.entry.js().consumers.get(stream, durable),
      config !== undefined,
    );
    const { config: actual } = await consumer.info(true);
    return {
      config: actual,
      // Just enough pulled messages to keep every slot busy, or to fill a batch: they wait
      // for a slot meanwhile. The heartbeat covers every message the client has handed
      // over; while every slot is busy, up to `max_messages - 1` more can still sit in the
      // client's own buffer, out of its reach.
      consume: () => consumer.consume({ max_messages: this.capacity }),
    };
  }

  /**
   * The server delivers up to `max_ack_pending` unacked messages, all read at once and
   * heartbeated while they wait for a slot; a consumer the library creates allows just
   * as many as the handler can hold.
   */
  private async openPush(): Promise<Intake> {
    const { stream, durable, config } = this.def.options;
    // Validated at bootstrap: the option and `config.deliver_group` never disagree.
    const deliverGroup =
      (this.def.options as ConsumePushOptions).deliverGroup ?? config?.deliver_group;
    if (config) {
      await this.streams.ensureConsumer(
        stream,
        { ...config, durable_name: durable },
        undefined,
        pushConsumerDefaults({
          stream,
          durable,
          ...(deliverGroup !== undefined ? { deliverGroup } : {}),
          maxAckPending: this.capacity,
        }),
      );
    }
    const consumer = await this.lookup(async () => {
      try {
        return await this.entry.js().consumers.getPushConsumer(stream, durable);
      } catch (err) {
        // Missing consumer or stream: mapped by `lookup()`.
        if (err instanceof JetStreamApiError) throw err;
        // nats.js refuses a consumer it cannot bind as a push consumer (e.g. a pull one,
        // without `deliver_subject`): tell why from its info when the checks can. Without
        // the info, nats.js's error is the one to report.
        const actual = await this.streams
          .jsm()
          .then((jsm) => jsm.consumers.info(stream, durable))
          .then(
            (info) => info.config,
            () => undefined,
          );
        if (actual) this.checkPushConsumer(actual, deliverGroup, err);
        throw err;
      }
    }, config !== undefined);
    const { config: actual } = await consumer.info(true);
    this.checkPushConsumer(actual, deliverGroup);
    return {
      config: actual,
      consume: async () => {
        try {
          return await consumer.consume();
        } catch (err) {
          // Without a deliver group, nats.js binds one subscriber only.
          if (isAlreadyBound(err) && !deliverGroup) {
            throw new NatsConfigError(
              `${this.pushConsumerName} is already bound to another subscriber (${err.message}); give every instance the same deliverGroup to share its messages`,
              { cause: err },
            );
          }
          throw err;
        }
      },
    };
  }

  private get pushConsumerName(): string {
    return `Consumer "${this.consumerName}" for ${this.handlerName}`;
  }

  /**
   * A push consumer must deliver to this handler's group, and not without limit;
   * `cause` is the error that led to the check, if any.
   */
  private checkPushConsumer(
    actual: ConsumerConfig,
    group: string | undefined,
    cause?: unknown,
  ): void {
    const name = this.pushConsumerName;
    const fail = (message: string) =>
      new NatsConfigError(message, cause === undefined ? undefined : { cause });
    if (!actual.deliver_subject) {
      throw fail(
        `${name} is not a push consumer (it has no deliver_subject); use @Consume for a pull consumer`,
      );
    }
    const actualGroup = actual.deliver_group || undefined;
    if (actualGroup !== group) {
      const has = actualGroup === undefined ? 'no deliver_group' : `deliver_group "${actualGroup}"`;
      const declared = group === undefined ? 'none' : `deliverGroup "${group}"`;
      throw fail(`${name} has ${has}, but the handler declares ${declared}`);
    }
    const maxAckPending = actual.max_ack_pending ?? 0;
    if (maxAckPending <= 0) {
      throw fail(
        `${name} has an unlimited max_ack_pending (${maxAckPending}); a push consumer needs a limit, as every pending message is delivered at once`,
      );
    }
  }

  private get consumerName(): string {
    return `${this.def.options.stream}/${this.def.options.durable}`;
  }

  private get handlerName(): string {
    return `${this.def.className}.${this.def.methodName}`;
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

  /** Binds the consumer, a missing one (or stream) being a configuration error. */
  private async lookup<T>(get: () => Promise<T>, configDeclared: boolean): Promise<T> {
    const { stream, durable } = this.def.options;
    try {
      return await get();
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
    let error: unknown;
    let failed = false;
    try {
      for await (const msg of messages) {
        // Heartbeated from now on, while it waits for a slot or a batch.
        this.unsettled.set(msg, undefined);
        if (!this.push) {
          await this.dispatcher.push(msg, msg.info.deliveryCount);
          continue;
        }
        // Push: keep reading (nats.js answers flow control only as the iterator yields);
        // the dispatcher queues it in FIFO order, and `max_ack_pending` bounds the queue.
        void this.dispatcher
          .push(msg, msg.info.deliveryCount)
          .catch((err: unknown) =>
            this.entry.logger.error(
              `Failed to dispatch message for ${this.label}`,
              errorTrace(err),
            ),
          );
      }
    } catch (err) {
      failed = true;
      error = err;
    }
    // Messages that were replaced are not this handler's intake anymore.
    if (this.stopping || messages !== this.messages) return;
    const tail = this.supervisor.restartable ? '' : '; no more messages will be received';
    if (failed) {
      this.entry.logger.error(`Consumer of ${this.label} failed${tail}`, errorTrace(error));
    } else {
      this.entry.logger.error(`Consumer of ${this.label} ended unexpectedly${tail}`);
    }
    this.supervisor.ended(error);
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
          case 'flow_control':
          case 'heartbeat':
            // Push consumers: nats.js answers / tracks these itself.
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
