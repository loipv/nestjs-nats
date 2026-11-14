import 'reflect-metadata';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { describe, expect, it, vi } from 'vitest';
import {
  type CloudEventRouterOptions,
  OnCloudEvent,
} from '../../../src/decorators/cloudevent.decorators.js';
import {
  cloudEventPatterns,
  defineRouter,
  routerKind,
} from '../../../src/discovery/cloud-event-router.js';
import type { HandlerDefinition, Invoke } from '../../../src/discovery/handler-definition.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

class R {
  created() {}
  any() {}
}

const wrapper = { name: 'R' } as unknown as InstanceWrapper;
const ROUTES = [{ pattern: 'com.o.created', methodName: 'created' }];

/** The definition of router `R` with `options` and `routes`, its methods wrapped by `wrap`. */
function routerDef(
  options: CloudEventRouterOptions,
  routes = ROUTES,
  wrap: (instance: object, methodName: string) => Invoke = () => vi.fn(),
): HandlerDefinition {
  return defineRouter(wrapper, new R(), options, routes, wrap);
}

function errorOf(run: () => unknown): NatsConfigError {
  try {
    run();
  } catch (err) {
    expect(err).toBeInstanceOf(NatsConfigError);
    return err as NatsConfigError;
  }
  throw new Error('expected a NatsConfigError');
}

describe('defineRouter', () => {
  it('defines a subscribe router named after its class, acking unmatched events by default', () => {
    const instance = new R();
    const def = defineRouter(wrapper, instance, { subscribe: 'events.>' }, ROUTES, () => vi.fn());
    expect(def).toMatchObject({
      kind: 'subscribe',
      subject: 'events.>',
      connection: 'default',
      className: 'R',
      methodName: '@CloudEventRouter',
      options: {},
    });
    expect(def.instance).toBe(instance);
    expect(def.wrapper).toBe(wrapper);
    expect(def.router?.unmatched).toBe('ack');
  });

  it('is invoked only through its routes', async () => {
    await expect(routerDef({ subscribe: 'x' }).invoke(undefined, {} as never)).rejects.toThrow(
      '@CloudEventRouter R is invoked through its @OnCloudEvent routes',
    );
  });

  it('routes each type to its method, wrapping a method once for all its patterns', () => {
    const wrap = vi.fn((_instance: object, _methodName: string) => vi.fn() as unknown as Invoke);
    const def = routerDef(
      { subscribe: 'x' },
      [
        { pattern: 'com.o.created', methodName: 'created' },
        { pattern: 'com.o.*', methodName: 'any' },
        { pattern: 'com.p.*', methodName: 'any' },
      ],
      wrap,
    );
    expect(wrap.mock.calls.map(([, m]) => m)).toEqual(['created', 'any']);
    const route = def.router?.route;
    expect(route?.('com.o.created')?.methodName).toBe('created');
    expect(route?.('com.o.updated')?.methodName).toBe('any');
    expect(route?.('com.p.x')?.invoke).toBe(route?.('com.o.updated')?.invoke);
    expect(route?.('com.q.x')).toBeUndefined();
  });

  it('maps consume routers to consume definitions', () => {
    const def = routerDef({
      consume: { stream: 'S', durable: 'd', config: { ack_wait: 1 } },
      connection: 'c',
      concurrency: 2,
      unmatched: 'term',
    });
    expect(def).toMatchObject({
      kind: 'consume',
      delivery: 'pull',
      connection: 'c',
      methodName: '@CloudEventRouter',
      options: { stream: 'S', durable: 'd', config: { ack_wait: 1 }, concurrency: 2 },
    });
    expect(def.router?.unmatched).toBe('term');
  });

  it('passes its timeout on to the definition', () => {
    expect(routerDef({ subscribe: 'events', timeout: 100 }).options.timeout).toBe(100);
  });

  it('builds a push consumer definition from consumePush', () => {
    const def = routerDef({
      consumePush: { stream: 'S', durable: 'd', deliverGroup: 'g', config: { ack_wait: 1 } },
    });
    expect(def).toMatchObject({
      kind: 'consume',
      delivery: 'push',
      options: { stream: 'S', durable: 'd', deliverGroup: 'g', config: { ack_wait: 1 } },
    });
  });

  it('builds an ordered consumer definition from consumeOrdered', () => {
    const def = routerDef({
      consumeOrdered: {
        stream: 'S',
        filterSubjects: 'a.>',
        deliverPolicy: 'new',
        headersOnly: true,
      },
    });
    expect(def).toMatchObject({
      kind: 'ordered',
      options: { stream: 'S', filterSubjects: 'a.>', deliverPolicy: 'new', headersOnly: true },
    });
  });

  it.each([
    ['subscribe and consume', { subscribe: 'x', consume: { stream: 'S', durable: 'd' } }],
    ['subscribe and consumePush', { subscribe: 'x', consumePush: { stream: 'S', durable: 'd' } }],
    [
      'consumePush and consumeOrdered',
      { consumePush: { stream: 'S', durable: 'd' }, consumeOrdered: { stream: 'S' } },
    ],
    ['no source', {}],
  ])('rejects a router with %s', (_, options) => {
    expect(errorOf(() => routerDef(options as never)).message).toBe(
      '@CloudEventRouter R must set exactly one of subscribe, consume, consumePush or consumeOrdered',
    );
  });

  it('counts a source set to undefined as unset', () => {
    const def = routerDef({
      consume: undefined,
      consumePush: { stream: 'S', durable: 'd' },
    } as never);
    expect(def).toMatchObject({ kind: 'consume', delivery: 'push' });
  });

  it('rejects concurrency with consumeOrdered', () => {
    expect(
      errorOf(() => routerDef({ consumeOrdered: { stream: 'S' }, concurrency: 2 })).message,
    ).toBe('@CloudEventRouter R with consumeOrdered does not support concurrency');
  });

  it('rejects queue unless the source is subscribe', () => {
    expect(
      errorOf(() => routerDef({ consume: { stream: 'S', durable: 'd' }, queue: 'q' } as never))
        .message,
    ).toBe('@CloudEventRouter R sets queue, which only applies to subscribe');
    expect(routerDef({ subscribe: 'a.>', queue: 'q' })).toMatchObject({
      kind: 'subscribe',
      options: { queue: 'q' },
    });
  });

  it.each(['durable', 'config', 'ack', 'batch'])('rejects %s inside consumeOrdered', (key) => {
    expect(
      errorOf(() => routerDef({ consumeOrdered: { stream: 'S', [key]: {} } } as never)).message,
    ).toBe(`@CloudEventRouter R: consumeOrdered does not support ${key}`);
  });

  it('ignores an undefined unsupported key inside consumeOrdered', () => {
    const def = routerDef({ consumeOrdered: { stream: 'S', durable: undefined } } as never);
    expect(def).toMatchObject({ kind: 'ordered' });
  });

  it.each(['batch', 'ack'])('rejects %s', (key) => {
    expect(errorOf(() => routerDef({ subscribe: 'x', [key]: 'manual' } as never)).message).toBe(
      `@CloudEventRouter R does not support ${key}: a router handles one CloudEvent at a time and settles it automatically`,
    );
  });

  it('rejects an invalid unmatched policy', () => {
    expect(errorOf(() => routerDef({ subscribe: 'x', unmatched: 'nack' } as never)).message).toBe(
      `@CloudEventRouter R has invalid unmatched "nack"; expected 'ack', 'term' or 'dlq'`,
    );
  });

  it('rejects a router without routes', () => {
    expect(errorOf(() => routerDef({ subscribe: 'x' }, [])).message).toBe(
      '@CloudEventRouter R has no @OnCloudEvent method',
    );
  });

  it.each([
    [
      'a duplicate pattern',
      [
        { pattern: 'a.b', methodName: 'created' },
        { pattern: 'a.b', methodName: 'any' },
      ],
      '@CloudEventRouter R: Duplicate CloudEvent type pattern "a.b"',
    ],
    [
      'an invalid pattern',
      [{ pattern: 'a.>.b', methodName: 'created' }],
      '@CloudEventRouter R: Invalid CloudEvent type pattern "a.>.b": ">" is only allowed as the last segment',
    ],
  ])('names the router in the error for %s', (_, routes, message) => {
    const err = errorOf(() => routerDef({ subscribe: 'x' }, routes));
    expect(err.message).toBe(message);
    expect(err.cause).toBeInstanceOf(NatsConfigError);
  });
});

describe('routerKind', () => {
  it.each([
    [{ subscribe: 'x' }, 'subscribe'],
    [{ consume: { stream: 'S', durable: 'd' } }, 'consume'],
    [{ consumePush: { stream: 'S', durable: 'd' } }, 'consume'],
    [{ consumeOrdered: { stream: 'S' } }, 'ordered'],
    [{}, 'subscribe'],
  ])('is the kind of %j', (options, kind) => {
    expect(routerKind(options as CloudEventRouterOptions)).toBe(kind);
  });
});

describe('cloudEventPatterns', () => {
  it('lists the @OnCloudEvent patterns of a method, none for others', () => {
    class Events {
      @OnCloudEvent('a.b')
      @OnCloudEvent('a.*')
      on() {}

      plain() {}
    }
    expect(cloudEventPatterns(Events.prototype.on)).toEqual(['a.*', 'a.b']);
    expect(cloudEventPatterns(Events.prototype.plain)).toEqual([]);
    expect(cloudEventPatterns('not a method')).toEqual([]);
  });
});
