import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { createTypeRouter } from '../cloudevents/matcher.js';
import type { CloudEventRouterOptions } from '../decorators/cloudevent.decorators.js';
import { NATS_ON_CLOUD_EVENT_METADATA } from '../decorators/constants.js';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { withoutUndefined } from '../util/objects.js';
import {
  type CloudEventRouting,
  type HandlerDefinition,
  type HandlerKind,
  type HandlerSpec,
  type Invoke,
  ROUTER_METHOD_NAME,
} from './handler-definition.js';

const UNMATCHED_POLICIES: ReadonlyArray<CloudEventRouting['unmatched']> = ['ack', 'term', 'dlq'];

const ORDERED_KEYS: readonly string[] = [
  'stream',
  'filterSubjects',
  'deliverPolicy',
  'headersOnly',
];

/** The `@OnCloudEvent` patterns of a method. */
export function cloudEventPatterns(method: unknown): string[] {
  if (typeof method !== 'function') return [];
  return Reflect.getMetadata(NATS_ON_CLOUD_EVENT_METADATA, method) ?? [];
}

/** The single subscribe/consume/ordered definition of a `@CloudEventRouter` class. */
export function defineRouter(
  wrapper: InstanceWrapper,
  instance: object,
  options: CloudEventRouterOptions,
  routes: Array<{ pattern: string; methodName: string }>,
  /** Wraps a route method with Nest's enhancers. */
  wrap: (instance: object, methodName: string) => Invoke,
): HandlerDefinition {
  const className = instance.constructor.name;
  const label = `@CloudEventRouter ${className}`;
  if (routes.length === 0) throw new NatsConfigError(`${label} has no @OnCloudEvent method`);
  const unmatched = options.unmatched ?? 'ack';
  if (!UNMATCHED_POLICIES.includes(unmatched)) {
    throw new NatsConfigError(
      `${label} has invalid unmatched ${JSON.stringify(unmatched)}; expected 'ack', 'term' or 'dlq'`,
    );
  }
  // Not part of the options type; rejected for untyped callers (spec §5: no batch).
  for (const key of ['batch', 'ack'] as const) {
    if (key in options && (options as Record<string, unknown>)[key] !== undefined) {
      throw new NatsConfigError(
        `${label} does not support ${key}: a router handles one CloudEvent at a time and settles it automatically`,
      );
    }
  }
  const set = (['subscribe', 'consume', 'consumePush', 'consumeOrdered'] as const).filter(
    (key) => key in options && (options as Record<string, unknown>)[key] !== undefined,
  );
  if (set.length !== 1) {
    throw new NatsConfigError(
      `${label} must set exactly one of subscribe, consume, consumePush or consumeOrdered`,
    );
  }
  const { connection, concurrency, codec, retry, dlq, idempotency, timeout } = options;
  if (set[0] !== 'subscribe' && 'queue' in options && options.queue !== undefined) {
    throw new NatsConfigError(`${label} sets queue, which only applies to subscribe`);
  }
  if (set[0] === 'consumeOrdered' && concurrency !== undefined) {
    throw new NatsConfigError(`${label} with consumeOrdered does not support concurrency`);
  }
  const common = withoutUndefined({
    connection,
    concurrency,
    codec,
    retry,
    dlq,
    idempotency,
    timeout,
  });
  let spec: HandlerSpec;
  if (set[0] === 'subscribe') {
    const { subscribe, queue } = options as Extract<CloudEventRouterOptions, { subscribe: string }>;
    spec = {
      kind: 'subscribe',
      subject: subscribe,
      options: { ...common, ...withoutUndefined({ queue }) },
    };
  } else if (set[0] === 'consume') {
    const { consume } = options as Extract<CloudEventRouterOptions, { consume: unknown }>;
    spec = {
      kind: 'consume',
      delivery: 'pull',
      options: {
        ...common,
        stream: consume.stream,
        durable: consume.durable,
        ...withoutUndefined({ config: consume.config }),
      },
    };
  } else if (set[0] === 'consumePush') {
    const { consumePush } = options as Extract<CloudEventRouterOptions, { consumePush: unknown }>;
    spec = {
      kind: 'consume',
      delivery: 'push',
      options: {
        ...common,
        stream: consumePush.stream,
        durable: consumePush.durable,
        ...withoutUndefined({
          config: consumePush.config,
          deliverGroup: consumePush.deliverGroup,
        }),
      },
    };
  } else {
    const { consumeOrdered } = options as Extract<
      CloudEventRouterOptions,
      { consumeOrdered: unknown }
    >;
    // Not part of the options type; rejected for untyped callers.
    for (const key of Object.keys(consumeOrdered)) {
      if (
        !ORDERED_KEYS.includes(key) &&
        (consumeOrdered as Record<string, unknown>)[key] !== undefined
      ) {
        throw new NatsConfigError(`${label}: consumeOrdered does not support ${key}`);
      }
    }
    spec = {
      kind: 'ordered',
      options: {
        ...common,
        stream: consumeOrdered.stream,
        ...withoutUndefined({
          filterSubjects: consumeOrdered.filterSubjects,
          deliverPolicy: consumeOrdered.deliverPolicy,
          headersOnly: consumeOrdered.headersOnly,
        }),
      },
    };
  }
  // One Nest-wrapped invoker per method, shared by its patterns.
  const targets = new Map<string, { methodName: string; invoke: Invoke }>();
  for (const { methodName } of routes) {
    if (!targets.has(methodName)) {
      targets.set(methodName, { methodName, invoke: wrap(instance, methodName) });
    }
  }
  let route: CloudEventRouting['route'];
  try {
    route = createTypeRouter(
      routes.map(({ pattern, methodName }) => ({
        pattern,
        target: targets.get(methodName) as { methodName: string; invoke: Invoke },
      })),
    );
  } catch (err) {
    // Name the router in the invalid/duplicate pattern error.
    if (err instanceof NatsConfigError) {
      throw new NatsConfigError(`${label}: ${err.message}`, { cause: err });
    }
    throw err;
  }
  return {
    ...spec,
    connection: connection ?? DEFAULT_CONNECTION,
    className,
    methodName: ROUTER_METHOD_NAME,
    instance,
    wrapper,
    invoke: () =>
      Promise.reject(new NatsModuleError(`${label} is invoked through its @OnCloudEvent routes`)),
    router: { route, unmatched },
  };
}

/** The kind of the definition a router would have; `'subscribe'` when it sets no source. */
export function routerKind(options: CloudEventRouterOptions): HandlerKind {
  if ('consume' in options || 'consumePush' in options) return 'consume';
  if ('consumeOrdered' in options) return 'ordered';
  return 'subscribe';
}
