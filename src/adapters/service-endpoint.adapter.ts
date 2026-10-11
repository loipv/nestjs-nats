import { type Service, type ServiceConfig, type ServiceMsg, Svcm } from '@nats-io/services';
import type { QueuedIterator } from '@nats-io/transport-node';
import { type Codec, encodePayload } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsContext } from '../context/nats-context.js';
import type { NatsServiceOptions } from '../decorators/handler.decorators.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
import { createDelivery, type HandlerInfo, type InternalDelivery } from '../pipeline/delivery.js';
import { type ServiceError, toServiceError } from '../pipeline/reply.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import {
  type ForceStopResult,
  type HandlerAdapter,
  NO_RESTART,
  type RestartBinding,
} from './adapter.js';
import { createHandlerPipeline, defaultPipelineDeps } from './handler-pipeline.js';
import { createLimiter, type Limiter } from './limiter.js';
import { RestartSupervisor } from './restart-supervisor.js';

export type ServiceEndpointDefinition = HandlerDefinition & { kind: 'service-endpoint' };

interface EndpointRuntime {
  def: ServiceEndpointDefinition;
  info: HandlerInfo;
  run: (d: InternalDelivery) => Promise<void>;
  limiter: Limiter;
  codec: Codec;
  label: string;
}

/** One registration of the service: a restart creates a new one. */
interface ServiceInstance {
  service: Service;
  iterators: Array<QueuedIterator<ServiceMsg>>;
  /** The service stopped by itself because of an error (already logged). */
  stoppedByItself: boolean;
  /** This adapter asked the service to stop. */
  closed: boolean;
}

/**
 * Registers one NATS service for a `@NatsService` class and runs each of its
 * `@ServiceEndpoint` handlers (one at a time per endpoint) on the service's endpoints.
 */
export class NatsServiceAdapter implements HandlerAdapter {
  private readonly endpoints: EndpointRuntime[];
  private instance?: ServiceInstance;
  /** Endpoint loops still running, of the current service and of replaced ones. */
  private readonly loops = new Set<Promise<void>>();
  private stopping = false;
  private readonly supervisor: RestartSupervisor;

  constructor(
    private readonly options: NatsServiceOptions,
    endpoints: ServiceEndpointDefinition[],
    private readonly entry: ConnectionEntry,
    restart: RestartBinding = NO_RESTART,
  ) {
    this.endpoints = endpoints.map((def) => {
      // Requests are never retried: errors are answered with an error reply.
      const { run, info, codec } = createHandlerPipeline(
        { ...def, service: options },
        entry,
        defaultPipelineDeps,
      );
      return {
        def,
        info,
        run,
        limiter: createLimiter(1),
        codec,
        label: `${def.className}.${def.methodName} on service "${options.name}" endpoint "${def.endpoint}"`,
      };
    });
    const label = `service "${options.name}"`;
    this.supervisor = new RestartSupervisor({
      policy: restart.policy,
      label,
      logger: entry.logger,
      isConnectionClosed: () => entry.nc.isClosed(),
      reopen: () => this.open(),
      onState: restart.track(label),
    });
  }

  async start(): Promise<void> {
    if (this.instance) return;
    await this.open();
    this.supervisor.started();
  }

  /** Registers the service and its endpoints; a restart replaces the previous service. */
  private async open(): Promise<void> {
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
    const instance: ServiceInstance = {
      service,
      iterators: [],
      stoppedByItself: false,
      closed: false,
    };
    // Assigned before adding endpoints so `stop()` stops it even if what follows fails.
    this.instance = instance;
    if (this.stopping) {
      await this.stopService(instance);
      return;
    }
    void service.stopped.then((err) => this.onServiceStopped(instance, err));
    try {
      for (const endpoint of this.endpoints) {
        instance.iterators.push(this.addEndpoint(instance, endpoint));
      }
      // Make sure the server knows about the subscriptions before bootstrap completes.
      await this.entry.nc.flush();
    } catch (err) {
      // A partly registered service would serve some endpoints only.
      await this.stopService(instance);
      throw err;
    }
  }

  /** Adds `endpoint` to the service of `instance` and starts serving it. */
  private addEndpoint(
    instance: ServiceInstance,
    endpoint: EndpointRuntime,
  ): QueuedIterator<ServiceMsg> {
    const { subject, queue, metadata } = endpoint.def.options;
    let iterator: QueuedIterator<ServiceMsg>;
    try {
      iterator = instance.service.addEndpoint(endpoint.def.endpoint, {
        ...(subject !== undefined ? { subject } : {}),
        ...(queue !== undefined ? { queue } : {}),
        ...(metadata !== undefined ? { metadata } : {}),
      });
    } catch (err) {
      const { className, methodName } = endpoint.def;
      throw new NatsConfigError(
        `Invalid @ServiceEndpoint ${className}.${methodName} ("${endpoint.def.endpoint}") of service "${this.options.name}": ${errorMessage(err)}`,
        { cause: err },
      );
    }
    const loop = this.consume(instance, iterator, endpoint).finally(() => this.loops.delete(loop));
    this.loops.add(loop);
    return iterator;
  }

  async stop(): Promise<void> {
    this.supervisor.stop();
    this.stopping = true;
    const instance = this.instance;
    if (!instance) return;
    await this.stopService(instance);
  }

  async idle(): Promise<void> {
    await this.supervisor.settled();
    // Loops end once the stopped service's subscriptions have yielded their buffered messages.
    await Promise.all(this.loops);
    await Promise.all(this.endpoints.map((e) => e.limiter.idle()));
  }

  forceStop(): ForceStopResult {
    // Requests are not redelivered and are never retried in-process: nothing to force.
    return { nakked: 0, cancelledRetries: 0 };
  }

  /**
   * A service also stops by itself, e.g. when one of its subscriptions fails (a permissions
   * violation). The failed endpoint's iterator is then never ended by `@nats-io/services`,
   * so end them all here: otherwise shutdown would wait for it until `drainTimeout`.
   */
  private onServiceStopped(instance: ServiceInstance, err: Error | null): void {
    const byItself =
      err instanceof Error && instance === this.instance && !instance.closed && !this.stopping;
    if (byItself) {
      instance.stoppedByItself = true;
      const tail = this.supervisor.restartable ? '' : '; no more requests will be received';
      this.entry.logger.error(
        `NATS service "${this.options.name}" stopped: ${errorMessage(err)}${tail}`,
        errorTrace(err),
      );
    }
    for (const iterator of instance.iterators) iterator.stop();
    if (byItself) this.supervisor.ended(err);
  }

  private async stopService(instance: ServiceInstance): Promise<void> {
    if (instance.closed) return;
    instance.closed = true;
    // `stop()` on a service that stopped by itself resolves with the error it stopped with.
    if (this.entry.nc.isClosed() || instance.service.isStopped) return;
    try {
      const err = await instance.service.stop();
      if (err) throw err;
    } catch (err) {
      this.entry.logger.error(`Failed to stop service "${this.options.name}"`, errorTrace(err));
    }
  }

  private async consume(
    instance: ServiceInstance,
    iterator: QueuedIterator<ServiceMsg>,
    endpoint: EndpointRuntime,
  ): Promise<void> {
    let error: unknown;
    let failed = false;
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
      failed = true;
      error = err;
    }
    // A service that closes itself (a subscription failed) ends the endpoints' iterators
    // before `stopped` reports why: let `onServiceStopped` handle it, with the reason.
    if (instance.service.isStopped && !instance.closed) await instance.service.stopped;
    // A service that was stopped or replaced is not this handler's intake anymore; one that
    // stopped by itself was already reported.
    if (
      this.stopping ||
      instance !== this.instance ||
      instance.closed ||
      instance.stoppedByItself
    ) {
      return;
    }
    const restartable = this.supervisor.restartable;
    const tail = restartable ? '' : '; no more requests will be received';
    if (failed) {
      this.entry.logger.error(`Endpoint of ${endpoint.label} failed${tail}`, errorTrace(error));
    } else {
      this.entry.logger.error(`Endpoint of ${endpoint.label} ended unexpectedly${tail}`);
    }
    // The other endpoints keep serving unless the service is recreated with all of them.
    if (restartable) void this.stopService(instance);
    this.supervisor.ended(error);
  }

  private async handle(msg: ServiceMsg, endpoint: EndpointRuntime): Promise<void> {
    const context = new NatsContext({ kind: 'service', connection: this.entry.name, raw: msg });
    const d = createDelivery({
      definition: endpoint.def,
      handler: endpoint.info,
      connection: this.entry.name,
      batch: false,
      parts: [
        {
          message: {
            subject: msg.subject,
            data: msg.data,
            headers: msg.headers,
            raw: msg,
            attempt: 1,
          },
          context,
        },
      ],
      codec: endpoint.codec,
      logger: this.entry.logger,
    });
    try {
      await endpoint.run(d);
    } catch (err) {
      const { code } = this.respondError(msg, endpoint, err);
      if (code >= 500) {
        this.entry.logger.warn(
          `Replied ${code} to request on "${msg.subject}" for ${endpoint.label}: ${errorMessage(err)}`,
        );
      }
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

  private respondError(msg: ServiceMsg, endpoint: EndpointRuntime, err: unknown): ServiceError {
    const error = toServiceError(err);
    const { code, description } = error;
    // Header values must not contain CR/LF.
    const safe = description.replace(/[\r\n]+/g, ' ');
    this.respond(msg, endpoint, () => msg.respondError(code, safe));
    return error;
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
