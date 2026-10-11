import {
  type Consumer,
  type ConsumerMessages,
  DeliverPolicy,
  JetStreamApiCodes,
  JetStreamApiError,
  type JsMsg,
  type OrderedConsumerOptions,
} from '@nats-io/jetstream';
import type { Codec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsOrderedContext } from '../context/nats-ordered-context.js';
import type {
  ConsumeOrderedOptions,
  OrderedDeliverPolicy,
} from '../decorators/handler.decorators.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
import {
  createDelivery,
  type HandlerInfo,
  type InboundMessage,
  type InternalDelivery,
} from '../pipeline/delivery.js';
import { backoffDelay, DEFAULT_RETRY, type RetryPolicy } from '../settlement/retry.js';
import { errorTrace } from '../util/errors.js';
import { isPowerOfTwo } from '../util/power-of-two.js';
import { sleep } from '../util/sleep.js';
import {
  type ForceStopResult,
  type HandlerAdapter,
  NO_RESTART,
  type RestartBinding,
} from './adapter.js';
import { discardMessages } from './discard-messages.js';
import { createHandlerPipeline, defaultPipelineDeps } from './handler-pipeline.js';
import { RestartSupervisor } from './restart-supervisor.js';

type OrderedDefinition = HandlerDefinition & { kind: 'ordered' };

/** Where the consumer starts, in nats.js terms. */
function deliverPolicyOptions(policy: OrderedDeliverPolicy): Partial<OrderedConsumerOptions> {
  if (typeof policy === 'string') return { deliver_policy: policy };
  if ('startSeq' in policy) {
    return { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: policy.startSeq };
  }
  const { startTime } = policy;
  return {
    deliver_policy: DeliverPolicy.StartTime,
    opt_start_time: typeof startTime === 'string' ? startTime : startTime.toISOString(),
  };
}

/** The nats.js options of the ordered consumer: only what the handler declares. */
function orderedConsumerOptions(options: ConsumeOrderedOptions): Partial<OrderedConsumerOptions> {
  const { filterSubjects, deliverPolicy, headersOnly } = options;
  return {
    ...(filterSubjects !== undefined
      ? { filter_subjects: typeof filterSubjects === 'string' ? [filterSubjects] : filterSubjects }
      : {}),
    ...(deliverPolicy !== undefined ? deliverPolicyOptions(deliverPolicy) : {}),
    ...(headersOnly ? { headers_only: true } : {}),
  };
}

/**
 * Runs a `@ConsumeOrdered` handler on an ordered consumer: an ephemeral, ack-less consumer
 * that nats.js recreates on a gap. Messages are handled one at a time, in stream order; a
 * failed message is retried in place (the next one waits), then dead-lettered or dropped.
 */
export class OrderedConsumerAdapter implements HandlerAdapter {
  private readonly codec: Codec;
  private readonly retry: RetryPolicy | false;
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly info: HandlerInfo;
  private readonly label: string;
  /** Aborted at shutdown: cancels the retry wait and the DLQ publish retries. */
  private readonly abort = new AbortController();
  private messages?: ConsumerMessages;
  private loop?: Promise<void>;
  /** Waiting to retry the current message. */
  private waiting = false;
  /** Stream sequence of the last message handled: a reopen resumes right after it. */
  private lastSeq?: number;
  private readonly supervisor: RestartSupervisor;

  constructor(
    private readonly def: OrderedDefinition,
    private readonly entry: ConnectionEntry,
    restart: RestartBinding = NO_RESTART,
  ) {
    const pipeline = createHandlerPipeline(def, entry, defaultPipelineDeps);
    this.codec = pipeline.codec;
    this.info = pipeline.info;
    this.retry = pipeline.retry;
    this.run = pipeline.run;
    this.label = `${def.className}.${def.methodName} on "${def.options.stream}" (ordered)`;
    this.supervisor = new RestartSupervisor({
      policy: restart.policy,
      label: this.label,
      logger: entry.logger,
      isConnectionClosed: () => entry.nc.isClosed(),
      reopen: () => this.open(),
      onState: restart.track(this.label),
    });
  }

  private get stopping(): boolean {
    return this.abort.signal.aborted;
  }

  async start(): Promise<void> {
    if (this.messages) return;
    await this.open();
    this.supervisor.started();
  }

  /** Creates the consumer and starts reading it; a reopen resumes after the last handled message. */
  private async open(): Promise<void> {
    const consumer = await this.getConsumer();
    // Prefetches nats.js's default number of messages: they are not acked, so they need
    // no heartbeat while they wait.
    const messages = await consumer.consume();
    if (this.stopping) {
      // stop() ran while opening: nothing will handle these messages, and an ordered
      // consumer has nothing to hand back.
      await discardMessages(
        messages,
        () => {},
        (err) =>
          this.entry.logger.error(`Failed to close consumer of ${this.label}`, errorTrace(err)),
      );
      return;
    }
    this.messages = messages;
    this.loop = this.consume(messages);
    void this.watchStatus(messages);
  }

  /** The consumer's options: the declared ones, or after the last handled message. */
  private consumerOptions(): Partial<OrderedConsumerOptions> {
    const declared = orderedConsumerOptions(this.def.options);
    if (this.lastSeq === undefined) return declared;
    const { deliver_policy, opt_start_seq, opt_start_time, ...rest } = declared;
    return {
      ...rest,
      deliver_policy: DeliverPolicy.StartSequence,
      opt_start_seq: this.lastSeq + 1,
    };
  }

  stop(): Promise<void> {
    // Cancels the retry wait (and a failing dead letter's), abandoning that message.
    this.abort.abort();
    this.supervisor.stop();
    if (this.messages) this.close(this.messages);
    return Promise.resolve();
  }

  async idle(): Promise<void> {
    await this.supervisor.settled();
    await this.loop;
  }

  forceStop(): ForceStopResult {
    // Nothing to nak: an ordered consumer acknowledges nothing. A wait that stop() already
    // cancelled is not cancelled here.
    const cancelledRetries = this.waiting && !this.stopping ? 1 : 0;
    this.abort.abort();
    return { nakked: 0, cancelledRetries };
  }

  /** Creates the ordered consumer, a missing stream being a configuration error. */
  private async getConsumer(): Promise<Consumer> {
    const { stream } = this.def.options;
    try {
      return await this.entry.js().consumers.get(stream, this.consumerOptions());
    } catch (err) {
      if (err instanceof JetStreamApiError && err.code === JetStreamApiCodes.StreamNotFound) {
        throw new NatsConfigError(
          `Stream "${stream}" not found for ${this.def.className}.${this.def.methodName}`,
          { cause: err },
        );
      }
      throw err;
    }
  }

  private close(messages: ConsumerMessages): void {
    // `close()` resolves only after every buffered message has been yielded, which waits
    // on the handler; shutdown bounds that wait through `idle()`, so don't await it.
    void messages
      .close()
      .catch((err: unknown) =>
        this.entry.logger.error(`Failed to close consumer of ${this.label}`, errorTrace(err)),
      );
  }

  private async consume(messages: ConsumerMessages): Promise<void> {
    let error: unknown;
    let failed = false;
    try {
      for await (const msg of messages) {
        // Prefetched messages still buffered at shutdown are left unprocessed: an ordered
        // consumer has nothing to hand back.
        if (this.stopping) return;
        if (!(await this.handle(msg))) return;
        this.lastSeq = msg.info.streamSequence;
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

  /**
   * Handles `msg` until it is settled (processed, dead-lettered or dropped), retrying it in
   * place as the settlement requests; `false` when its retry was abandoned at shutdown.
   */
  private async handle(msg: JsMsg): Promise<boolean> {
    for (let attempt = 1; ; attempt++) {
      // Set by the settlement (through the scheduler) when the message is to be retried.
      const retry: { delayMs?: number } = {};
      const d = createDelivery({
        definition: this.def,
        handler: this.info,
        connection: this.entry.name,
        batch: false,
        parts: [
          {
            message: {
              subject: msg.subject,
              data: msg.data,
              headers: msg.headers,
              raw: msg,
              attempt,
            } satisfies InboundMessage,
            context: new NatsOrderedContext({ connection: this.entry.name, raw: msg, attempt }),
          },
        ],
        codec: this.codec,
        logger: this.entry.logger,
        retry: (_msg, delayMs) => {
          retry.delayMs = delayMs;
        },
        signal: this.abort.signal,
      });
      try {
        await this.run(d);
      } catch (err) {
        // Reached only when the pipeline failed outside settlement (e.g. a plugin threw):
        // retry the message with the retry backoff.
        this.entry.logger.error(`Handler ${this.label} failed`, errorTrace(err));
        retry.delayMs = backoffDelay(this.retry || DEFAULT_RETRY, attempt);
      }
      if (retry.delayMs === undefined) return true;
      this.waiting = true;
      const due = await sleep(retry.delayMs, this.abort.signal);
      this.waiting = false;
      if (!due) {
        this.entry.logger.warn(
          `Retry of ${this.label} (stream seq ${msg.info.streamSequence}) abandoned at shutdown`,
        );
        return false;
      }
    }
  }

  /** Surfaces consumer problems the library only reports through `status()` while it retries. */
  private async watchStatus(messages: ConsumerMessages): Promise<void> {
    const warnSpaced = (type: string, count: number | undefined) => {
      // The library retries forever; log on exponentially spaced attempts only.
      if (count !== undefined && !isPowerOfTwo(count)) return;
      const attempt = count !== undefined ? ` (attempt ${count})` : '';
      this.entry.logger.warn(`Consumer of ${this.label} reported ${type}${attempt}`);
    };
    try {
      for await (const n of messages.status()) {
        if (this.stopping) return;
        switch (n.type) {
          case 'ordered_consumer_recreated':
            this.entry.logger.debug?.(`Consumer of ${this.label} was recreated as "${n.name}"`);
            break;
          case 'heartbeats_missed':
          case 'consumer_not_found':
            warnSpaced(n.type, n.count);
            break;
          case 'stream_not_found':
            warnSpaced(n.type, n.consumerCreateFails);
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
}
