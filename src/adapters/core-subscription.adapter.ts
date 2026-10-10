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
} from '../pipeline/delivery.js';
import { type ServiceError, toServiceError } from '../pipeline/reply.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import type { ForceStopResult, HandlerAdapter } from './adapter.js';
import { Dispatcher, type DispatchItem } from './dispatcher.js';
import { createHandlerPipeline, defaultPipelineDeps } from './handler-pipeline.js';

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
  private sub?: Subscription;
  private loop?: Promise<void>;

  constructor(
    private readonly def: SubscribeDefinition,
    private readonly entry: ConnectionEntry,
  ) {
    const pipeline = createHandlerPipeline(def, entry, defaultPipelineDeps);
    this.codec = pipeline.codec;
    this.info = pipeline.info;
    this.run = pipeline.run;
    this.label = `${def.className}.${def.methodName} on "${def.subject}"`;
    this.batch = resolveBatchOptions(def.options.batch);
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
    await this.dispatcher.idle();
  }

  forceStop(): ForceStopResult {
    // Core messages cannot be handed back to the server: collected ones are lost.
    const dropped = this.dispatcher.abandon().length;
    if (dropped > 0) {
      this.entry.logger.warn(
        `Dropped ${dropped} collected message${dropped === 1 ? '' : 's'} for ${this.label} at forced stop`,
      );
    }
    return { nakked: 0, cancelledRetries: this.dispatcher.cancelRetries() };
  }

  private async consume(sub: Subscription): Promise<void> {
    try {
      for await (const msg of sub) await this.dispatcher.push(msg);
    } catch (err) {
      this.entry.logger.error(`Subscription of ${this.label} failed`, errorTrace(err));
    } finally {
      // The subscription has yielded its last message; nothing more will be pushed. The
      // batch being collected is handed to the handler; `idle()` waits for it.
      void this.dispatcher.stop();
    }
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
    return createDelivery({
      definition: this.def,
      handler: this.info,
      connection: this.entry.name,
      batch,
      parts,
      codec: this.codec,
      logger: this.entry.logger,
      retry: (msg: InboundMessage, delayMs: number) =>
        this.dispatcher.scheduleRetry({ msg: msg.raw as Msg, attempt: msg.attempt }, delayMs),
    });
  }

  /** A batch never replies, even to requests. */
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
    const { code, description } = error;
    this.respond(msg, () => {
      const h = headers();
      // Header values must not contain CR/LF.
      h.set(ServiceErrorHeader, description.replace(/[\r\n]+/g, ' '));
      h.set(ServiceErrorCodeHeader, String(code));
      return msg.respond(EMPTY, { headers: h });
    });
    return error;
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
