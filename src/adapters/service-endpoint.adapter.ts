import { type Service, type ServiceConfig, type ServiceMsg, Svcm } from '@nats-io/services';
import type { QueuedIterator } from '@nats-io/transport-node';
import { type Codec, encodePayload, jsonCodec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsContext } from '../context/nats-context.js';
import type { NatsServiceOptions } from '../decorators/handler.decorators.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
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
import { errorMessage, errorTrace } from '../util/errors.js';
import type { ForceStopResult, HandlerAdapter } from './adapter.js';
import { createLimiter, type Limiter } from './limiter.js';

export type ServiceEndpointDefinition = HandlerDefinition & { kind: 'service-endpoint' };

interface EndpointRuntime {
  def: ServiceEndpointDefinition;
  info: HandlerInfo;
  limiter: Limiter;
  codec: Codec;
  label: string;
}

/**
 * Registers one NATS service for a `@NatsService` class and runs each of its
 * `@ServiceEndpoint` handlers (one at a time per endpoint) on the service's endpoints.
 */
export class NatsServiceAdapter implements HandlerAdapter {
  private readonly run: (d: InternalDelivery) => Promise<void>;
  private readonly endpoints: EndpointRuntime[];
  private service?: Service;
  private loops: Array<Promise<void>> = [];
  private stopping = false;

  constructor(
    private readonly options: NatsServiceOptions,
    endpoints: ServiceEndpointDefinition[],
    private readonly entry: ConnectionEntry,
  ) {
    this.run = buildInboundPipeline(
      { plugins: entry.options.plugins ?? [], core: [basicSettlement] },
      invokeHandler,
    );
    this.endpoints = endpoints.map((def) => ({
      def,
      info: toHandlerInfo(def, options),
      limiter: createLimiter(1),
      codec: def.options.codec ?? entry.options.codec ?? jsonCodec(),
      label: `${def.className}.${def.methodName} on service "${options.name}" endpoint "${def.endpoint}"`,
    }));
  }

  async start(): Promise<void> {
    if (this.service) return;
    const { name, version, description, metadata, queue } = this.options;
    // Optional fields are only passed when set: the library validates what it receives.
    const config: ServiceConfig = {
      name,
      version,
      ...(description !== undefined ? { description } : {}),
      ...(metadata !== undefined ? { metadata } : {}),
      ...(queue !== undefined ? { queue } : {}),
    };
    const service = await new Svcm(this.entry.nc).add(config);
    // Assigned before adding endpoints so a failure below still stops the service.
    this.service = service;
    if (this.stopping) {
      await this.stopService(service);
      return;
    }
    for (const endpoint of this.endpoints) {
      const { subject, queue: endpointQueue, metadata: endpointMetadata } = endpoint.def.options;
      let iterator: QueuedIterator<ServiceMsg>;
      try {
        iterator = service.addEndpoint(endpoint.def.endpoint, {
          ...(subject !== undefined ? { subject } : {}),
          ...(endpointQueue !== undefined ? { queue: endpointQueue } : {}),
          ...(endpointMetadata !== undefined ? { metadata: endpointMetadata } : {}),
        });
      } catch (err) {
        // `this.service` is set, so the bootstrap cleanup stops the service.
        const { className, methodName } = endpoint.def;
        throw new NatsConfigError(
          `Invalid @ServiceEndpoint ${className}.${methodName} ("${endpoint.def.endpoint}") of service "${name}": ${errorMessage(err)}`,
          { cause: err },
        );
      }
      this.loops.push(this.consume(iterator, endpoint));
    }
    // Make sure the server knows about the subscriptions before bootstrap completes.
    await this.entry.nc.flush();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    const service = this.service;
    if (!service) return;
    await this.stopService(service);
  }

  async idle(): Promise<void> {
    // Loops end once the stopped service's subscriptions have yielded their buffered messages.
    await Promise.all(this.loops);
    await Promise.all(this.endpoints.map((e) => e.limiter.idle()));
  }

  forceStop(): ForceStopResult {
    // Requests are not redelivered and are never retried in-process: nothing to force.
    return { nakked: 0, cancelledRetries: 0 };
  }

  private async stopService(service: Service): Promise<void> {
    if (this.entry.nc.isClosed()) return;
    try {
      const err = await service.stop();
      if (err) throw err;
    } catch (err) {
      this.entry.logger.error(`Failed to stop service "${this.options.name}"`, errorTrace(err));
    }
  }

  private async consume(
    iterator: QueuedIterator<ServiceMsg>,
    endpoint: EndpointRuntime,
  ): Promise<void> {
    try {
      for await (const msg of iterator) {
        await endpoint.limiter.acquire();
        void this.handle(msg, endpoint)
          .catch((err: unknown) =>
            this.entry.logger.error(
              `Failed to process message for ${endpoint.label}`,
              errorTrace(err),
            ),
          )
          .finally(() => endpoint.limiter.release());
      }
    } catch (err) {
      this.entry.logger.error(`Endpoint of ${endpoint.label} failed`, errorTrace(err));
      return;
    }
    if (!this.stopping) {
      this.entry.logger.error(
        `Endpoint of ${endpoint.label} ended unexpectedly; no more requests will be received`,
      );
    }
  }

  private async handle(msg: ServiceMsg, endpoint: EndpointRuntime): Promise<void> {
    const context = new NatsContext({ kind: 'service', connection: this.entry.name, raw: msg });
    const d: InternalDelivery = {
      handler: endpoint.info,
      [HANDLER_DEFINITION]: endpoint.def,
      messages: [
        { subject: msg.subject, data: msg.data, headers: msg.headers, raw: msg, attempt: 1 },
      ],
      context,
      codec: endpoint.codec,
      logger: this.entry.logger,
    };
    try {
      await this.run(d);
    } catch (err) {
      this.respondError(msg, endpoint, err);
      return;
    }
    let payload: Uint8Array;
    try {
      payload = encodePayload(endpoint.codec, d.result);
    } catch (err) {
      this.entry.logger.error(`Failed to encode reply of ${endpoint.label}`, errorTrace(err));
      this.respondError(msg, endpoint, err);
      return;
    }
    this.respond(msg, endpoint, () => msg.respond(payload));
  }

  private respondError(msg: ServiceMsg, endpoint: EndpointRuntime, err: unknown): void {
    const { code, description } = toServiceError(err);
    // Header values must not contain CR/LF.
    const safe = description.replace(/[\r\n]+/g, ' ');
    this.respond(msg, endpoint, () => msg.respondError(code, safe));
  }

  private respond(msg: ServiceMsg, endpoint: EndpointRuntime, send: () => boolean): void {
    try {
      send();
    } catch (err) {
      this.entry.logger.error(
        `Failed to reply to "${msg.reply}" for ${endpoint.label}`,
        errorTrace(err),
      );
    }
  }
}
