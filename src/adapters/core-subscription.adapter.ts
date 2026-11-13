import { ServiceErrorCodeHeader, ServiceErrorHeader } from '@nats-io/services';
import { headers, type Msg, type Subscription } from '@nats-io/transport-node';
import { type ResolvedBatchOptions, resolveBatchOptions } from '../batch/collector.js';
import { type Codec, encodePayload } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsContext } from '../context/nats-context.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import {
  createDelivery,
  type DeliveryPart,
  type HandlerInfo,
  type InboundMessage,
  type InternalDelivery,
  LINGERING,
} from '../pipeline/delivery.js';
import { batchRequestError, type ServiceError, toServiceError } from '../pipeline/reply.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import {
  type ForceStopResult,
  type HandlerAdapter,
  NO_RESTART,
  type RestartBinding,
} from './adapter.js';
import { Dispatcher, type DispatchItem } from './dispatcher.js';
import { createHandlerPipeline, defaultPipelineDeps } from './handler-pipeline.js';
import { RestartSupervisor } from './restart-supervisor.js';

type SubscribeDefinition = HandlerDefinition & { kind: 'subscribe' };

const EMPTY = new Uint8Array(0);

/** Runs a `@Subscribe` handler on a core NATS subscription (optionally in a queue group). */
export class CoreSubscriptionAdapter implements HandlerAdapter {
  private readonly dispatcher: Dispatcher<Msg>;
  private readonly codec: Codec;
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly info: HandlerInfo;
  private readonly label: string;
  private readonly batch?: ResolvedBatchOptions;
  /** Resolves once every run that outlived the handler's timeout has ended. */
  private readonly lingering: () => Promise<void>;
  private readonly idempotent: boolean;

  private sub?: Subscription;
  private loop?: Promise<void>;
  private stopping = false;
  /** Whether the loop of the current subscription is still running. */
  private loopRunning = false;
  private readonly supervisor: RestartSupervisor;
  /** Whether a request rejected by this batch handler was already logged. */
  private warnedBatchRequest = false;

  constructor(
    private readonly def: SubscribeDefinition,
    private readonly entry: ConnectionEntry,
    restart: RestartBinding = NO_RESTART,
  ) {
    const pipeline = createHandlerPipeline(def, entry, defaultPipelineDeps);
    this.codec = pipeline.codec;
    this.info = pipeline.info;
    this.run = pipeline.run;
    this.lingering = pipeline.lingering;
    this.idempotent = pipeline.idempotent;
    this.label = `${def.className}.${def.methodName} on "${def.subject}"`;
    this.batch = resolveBatchOptions(def.options.batch);
    this.supervisor = new RestartSupervisor({
      policy: restart.policy,
      label: this.label,
      logger: entry.logger,
      isConnectionClosed: () => entry.nc.isClosed(),
      reopen: () => this.open(),
      onState: (() => {
        const track = restart.track(this.label);
        return (state) => {
          track(state);
          // Nothing will be pushed anymore: let pending retries and batches settle.
          if (state === 'failed') void this.dispatcher.stop();
        };
      })(),
    });
    this.dispatcher = new Dispatcher<Msg>({
      concurrency: def.options.concurrency ?? 1,
      ...(this.batch ? { batch: this.batch } : {}),
      process: (items) => this.process(items),
      logger: entry.logger,
      label: this.label,
      // A retry that fires once the subscription is stopping cannot be processed anymore.
      onRejected: (msg) =>
        entry.logger.warn(
          `Dropped message on "${msg.subject}" for ${this.label}: the subscription is stopping`,
        ),
    });
  }

  async start(): Promise<void> {
    if (this.sub) return;
    await this.open();
    this.supervisor.started();
  }

  private async open(): Promise<void> {
    if (this.stopping) return;
    const { queue } = this.def.options;
    const sub = this.entry.nc.subscribe(this.def.subject, queue ? { queue } : {});
    const previous = { sub: this.sub, loop: this.loop, running: this.loopRunning };
    this.sub = sub;
    this.loopRunning = true;
    this.loop = this.consume(sub);
    try {
      // Make sure the server knows about the subscription before bootstrap completes.
      await this.entry.nc.flush();
    } catch (err) {
      // The new subscription is dropped: its loop is no longer current and is ignored.
      this.sub = previous.sub;
      this.loop = previous.loop;
      this.loopRunning = previous.running;
      // stop() may have seen the new loop as running; its own end is ignored from now on.
      if (this.stopping && !this.loopRunning) void this.dispatcher.stop();
      try {
        sub.unsubscribe();
      } catch {
        // already closed
      }
      throw err;
    }
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.supervisor.stop();
    // Without a running loop nobody else stops the dispatcher (e.g. during a restart delay).
    if (!this.sub || !this.loopRunning) void this.dispatcher.stop();
    const sub = this.sub;
    if (!sub || sub.isClosed() || this.entry.nc.isClosed()) return;
    try {
      await sub.drain();
    } catch (err) {
      this.entry.logger.error(`Failed to drain subscription of ${this.label}`, errorTrace(err));
    }
  }

  async idle(): Promise<void> {
    await this.supervisor.settled();
    // The loop ends once the drained subscription has yielded its buffered messages.
    await this.loop;
    await this.dispatcher.idle();
    await this.lingering();
  }

  forceStop(): ForceStopResult {
    // Core messages cannot be handed back to the server: the ones not started are lost.
    const dropped = this.dispatcher.abandon().length;
    if (dropped > 0) {
      this.entry.logger.warn(
        `Dropped ${dropped} unstarted message${dropped === 1 ? '' : 's'} (collected or queued) for ${this.label} at forced stop`,
      );
    }
    return { nakked: 0, cancelledRetries: this.dispatcher.cancelRetries() };
  }

  private async consume(sub: Subscription): Promise<void> {
    let error: unknown;
    let failed = false;
    try {
      for await (const msg of sub) {
        if (this.batch && msg.reply) this.rejectBatchRequest(msg);
        else await this.dispatcher.push(msg);
      }
    } catch (err) {
      failed = true;
      error = err;
    }
    // A subscription that was replaced or dropped is not this handler's intake anymore.
    if (sub !== this.sub) return;
    this.loopRunning = false;
    if (!this.stopping) {
      const tail = this.supervisor.restartable ? '' : '; no more messages will be received';
      if (failed) {
        this.entry.logger.error(`Subscription of ${this.label} failed${tail}`, errorTrace(error));
      } else {
        this.entry.logger.error(`Subscription of ${this.label} ended unexpectedly${tail}`);
      }
      this.supervisor.ended(error);
    }
    // Nothing more will be pushed unless the subscription is reopened. The batch being
    // collected is handed to the handler; `idle()` waits for it.
    if (this.stopping || this.supervisor.state === 'failed') void this.dispatcher.stop();
  }

  private async process(items: DispatchItem<Msg>[]): Promise<void> {
    if (this.batch) return this.handleBatch(items);
    for (const item of items) await this.handle(item);
  }

  private part({ msg, attempt }: DispatchItem<Msg>): DeliveryPart {
    return {
      message: { subject: msg.subject, data: msg.data, headers: msg.headers, raw: msg, attempt },
      context: new NatsContext({ kind: 'core', connection: this.entry.name, raw: msg }),
    };
  }

  private delivery(parts: DeliveryPart[], batch: boolean): InternalDelivery {
    const d: InternalDelivery = createDelivery({
      definition: this.def,
      handler: this.info,
      connection: this.entry.name,
      batch,
      parts,
      codec: this.codec,
      logger: this.entry.logger,
      retry: (msg: InboundMessage, delayMs: number) => this.retryLater(d, msg, delayMs),
    });
    return d;
  }

  /**
   * Schedules the in-process retry of `msg`. With idempotency, a retry while the timed-out run
   * of `d` still goes on would find the key in progress and be dropped: it is scheduled once
   * that run ends, and only if it failed (when it succeeded, the message is done).
   */
  private retryLater(d: InternalDelivery, msg: InboundMessage, delayMs: number): void {
    const retry = () =>
      this.dispatcher.scheduleRetry({ msg: msg.raw as Msg, attempt: msg.attempt }, delayMs);
    const late = d[LINGERING];
    if (!this.idempotent || !late) {
      retry();
      return;
    }
    void late.then((ok) => {
      if (!ok) retry();
    });
  }

  /**
   * A batch handler cannot answer a request: the request is not handled and gets a `400`
   * at once, instead of its requester waiting for a reply that never comes.
   */
  private rejectBatchRequest(msg: Msg): void {
    this.respondWith(msg, batchRequestError(this.def));
    if (this.warnedBatchRequest) return;
    this.warnedBatchRequest = true;
    this.entry.logger.warn(
      `Rejected a request on "${msg.subject}" for ${this.label} with 400: batch handlers do not reply to requests`,
    );
  }

  private async handleBatch(items: DispatchItem<Msg>[]): Promise<void> {
    const parts = items.map((item) => this.part(item));
    try {
      await this.run(this.delivery(parts, true));
    } catch (err) {
      // Reached only when the pipeline failed outside settlement (e.g. a plugin threw).
      this.entry.logger.error(`Handler ${this.label} failed`, errorTrace(err));
    }
  }

  private async handle(item: DispatchItem<Msg>): Promise<void> {
    const { msg } = item;
    const part = this.part(item);
    const { context } = part;
    const d = this.delivery([part], false);
    try {
      await this.run(d);
    } catch (err) {
      if (context.replyTo) {
        const { code } = this.respondError(msg, err);
        if (code >= 500) {
          const { className, methodName } = this.def;
          this.entry.logger.warn(
            `Replied ${code} to request on "${msg.subject}" for ${className}.${methodName}: ${errorMessage(err)}`,
          );
        }
      } else {
        // Reached only when the pipeline failed outside settlement (e.g. a plugin threw).
        this.entry.logger.error(`Handler ${this.label} failed`, errorTrace(err));
      }
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

  private respondError(msg: Msg, err: unknown): ServiceError {
    const error = toServiceError(err);
    this.respondWith(msg, error);
    return error;
  }

  private respondWith(msg: Msg, { code, description }: ServiceError): void {
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
