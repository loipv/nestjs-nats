import 'reflect-metadata';
import { Injectable, Module, type Provider, Scope, type Type } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { describe, expect, it, vi } from 'vitest';
import { CloudEventRouter, OnCloudEvent } from '../../../src/decorators/cloudevent.decorators.js';
import {
  Consume,
  ConsumeOrdered,
  ConsumePush,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../../src/decorators/handler.decorators.js';
import type { StartupContext } from '../../../src/decorators/startup-value.js';
import {
  type HandlerDefinition,
  HandlerExplorer,
} from '../../../src/discovery/handler-explorer.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

interface Config {
  prefix: string;
  stream: string;
}

const CONFIG: Provider = { provide: 'CONFIG', useValue: { prefix: 'prod', stream: 'ORDERS' } };

const config = ({ get }: StartupContext) => get<Config>('CONFIG');

/** A compiled application with `HandlerExplorer`, the `CONFIG` provider and `providers`. */
async function app(providers: Array<Type | Provider>, imports: Type[] = []) {
  return Test.createTestingModule({
    imports: [DiscoveryModule, ...imports],
    providers: [HandlerExplorer, CONFIG, ...providers],
  }).compile();
}

/** The definitions `HandlerExplorer` finds among `providers`. */
async function explore(...providers: Type[]): Promise<HandlerDefinition[]> {
  const moduleRef = await app(providers);
  try {
    return moduleRef.get(HandlerExplorer).explore();
  } finally {
    await moduleRef.close();
  }
}

/** The error `explore()` throws for `providers`. */
async function exploreError(...providers: Type[]): Promise<NatsConfigError> {
  const err = await explore(...providers).then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(err).toBeInstanceOf(NatsConfigError);
  return err as NatsConfigError;
}

describe('HandlerExplorer startup values of method decorators', () => {
  it('resolves the subject and options of @Subscribe', async () => {
    @Injectable()
    class H {
      @Subscribe(
        (ctx) => `${config(ctx).prefix}.orders`,
        () => ({ queue: 'q' }),
      )
      on() {}
    }
    const [def] = await explore(H);
    expect(def).toMatchObject({
      kind: 'subscribe',
      subject: 'prod.orders',
      options: { queue: 'q' },
    });
  });

  it('resolves the options of @Consume, @ConsumePush and @ConsumeOrdered', async () => {
    @Injectable()
    class H {
      @Consume((ctx) => ({ stream: config(ctx).stream, durable: 'pull' }))
      pull() {}

      @ConsumePush((ctx) => ({ stream: config(ctx).stream, durable: 'push', deliverGroup: 'g' }))
      push() {}

      @ConsumeOrdered((ctx) => ({ stream: config(ctx).stream }))
      ordered() {}
    }
    const defs = await explore(H);
    const by = (m: string) => defs.find((d) => d.methodName === m);
    expect(by('pull')).toMatchObject({
      kind: 'consume',
      delivery: 'pull',
      options: { stream: 'ORDERS', durable: 'pull' },
    });
    expect(by('push')).toMatchObject({
      kind: 'consume',
      delivery: 'push',
      options: { stream: 'ORDERS', durable: 'push', deliverGroup: 'g' },
    });
    expect(by('ordered')).toMatchObject({ kind: 'ordered', options: { stream: 'ORDERS' } });
  });

  it('resolves the name and options of @ServiceEndpoint', async () => {
    @Injectable()
    @NatsService({ name: 'svc', version: '1.0.0' })
    class H {
      @ServiceEndpoint(
        () => 'get',
        () => ({ subject: 'svc.get' }),
      )
      get() {}
    }
    const [def] = await explore(H);
    expect(def).toMatchObject({
      kind: 'service-endpoint',
      endpoint: 'get',
      options: { subject: 'svc.get' },
      service: { name: 'svc' },
    });
  });

  it('gets a provider declared in another module', async () => {
    @Module({ providers: [{ provide: 'OTHER', useValue: 'other.subject' }] })
    class OtherModule {}
    @Injectable()
    class H {
      @Subscribe(({ get }) => get<string>('OTHER'))
      on() {}
    }
    const moduleRef = await app([H], [OtherModule]);
    const [def] = moduleRef.get(HandlerExplorer).explore();
    await moduleRef.close();
    expect(def).toMatchObject({ subject: 'other.subject' });
  });

  it('accepts undefined from the options function of @Subscribe as no options', async () => {
    @Injectable()
    class H {
      @Subscribe('a', () => undefined)
      on() {}
    }
    const [def] = await explore(H);
    expect(def).toMatchObject({ subject: 'a' });
    expect(def?.options).toStrictEqual({});
  });

  it('calls a function once per application, however often explore() runs', async () => {
    const subject = vi.fn(() => 'once');
    @Injectable()
    class H {
      @Subscribe(subject)
      on() {}
    }
    const first = await app([H]);
    const explorer = first.get(HandlerExplorer);
    explorer.explore();
    explorer.explore();
    expect(explorer.explore()[0]).toMatchObject({ subject: 'once' });
    expect(subject).toHaveBeenCalledOnce();
    const second = await app([H]);
    second.get(HandlerExplorer).explore();
    second.get(HandlerExplorer).explore();
    expect(subject).toHaveBeenCalledTimes(2);
    await first.close();
    await second.close();
  });

  it('calls a function shared by two decorators once and gives both its value', async () => {
    const shared = vi.fn(() => ({ stream: 'ORDERS', durable: 'billing' }));
    @Injectable()
    class H {
      @Consume(shared)
      a() {}

      @Consume(shared)
      b() {}
    }
    const defs = await explore(H);
    expect(defs).toHaveLength(2);
    for (const def of defs) {
      expect(def.options).toMatchObject({ stream: 'ORDERS', durable: 'billing' });
    }
    expect(shared).toHaveBeenCalledOnce();
  });

  describe('fails startup', () => {
    it.each([
      ['a request-scoped provider', 'SCOPED'],
      ['a missing provider', 'MISSING'],
    ])('when a function gets %s', async (_, token) => {
      @Injectable({ scope: Scope.REQUEST })
      class Scoped {}
      @Injectable()
      class H {
        @Consume(({ get }) => ({ stream: get<string>(token), durable: 'd' }))
        on() {}
      }
      const moduleRef = await app([H, { provide: 'SCOPED', useClass: Scoped }]);
      let err: unknown;
      try {
        moduleRef.get(HandlerExplorer).explore();
      } catch (e) {
        err = e;
      }
      await moduleRef.close();
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toMatch(
        /^NATS handler H\.on failed to resolve its @Consume options: /,
      );
      expect((err as Error).cause).toBeDefined();
    });

    it('with the same error each time, without calling a failed function again', async () => {
      const failing = vi.fn((): string => {
        throw new Error('no config');
      });
      @Injectable()
      class H {
        @Subscribe(failing)
        on() {}
      }
      const moduleRef = await app([H]);
      const explorer = moduleRef.get(HandlerExplorer);
      const message = 'NATS handler H.on failed to resolve its @Subscribe subject: no config';
      expect(() => explorer.explore()).toThrow(message);
      expect(() => explorer.explore()).toThrow(message);
      expect(failing).toHaveBeenCalledOnce();
      await moduleRef.close();
    });

    it.each([
      [42, '42'],
      ['', '""'],
      [undefined, 'undefined'],
    ])('when the subject function returns %j', async (value, shown) => {
      @Injectable()
      class H {
        @Subscribe(() => value as string)
        on() {}
      }
      expect((await exploreError(H)).message).toBe(
        `NATS handler H.on: @Subscribe subject resolved to ${shown}; expected a non-empty string`,
      );
    });

    it('when the endpoint name function returns no string', async () => {
      @Injectable()
      @NatsService({ name: 'svc', version: '1.0.0' })
      class H {
        @ServiceEndpoint(() => null as unknown as string)
        get() {}
      }
      expect((await exploreError(H)).message).toBe(
        'NATS handler H.get: @ServiceEndpoint name resolved to null; expected a non-empty string',
      );
    });

    it.each([
      [null, 'null'],
      [undefined, 'undefined'],
      ['S', '"S"'],
      [['S'], '["S"]'],
    ])('when the @Consume options function returns %j', async (value, shown) => {
      @Injectable()
      class H {
        @Consume(() => value as never)
        on() {}
      }
      expect((await exploreError(H)).message).toBe(
        `NATS handler H.on: @Consume options resolved to ${shown}; expected an object`,
      );
    });

    it('when the @ServiceEndpoint options function returns null', async () => {
      @Injectable()
      @NatsService({ name: 'svc', version: '1.0.0' })
      class H {
        @ServiceEndpoint('get', () => null as never)
        get() {}
      }
      expect((await exploreError(H)).message).toBe(
        'NATS handler H.get: @ServiceEndpoint options resolved to null; expected an object',
      );
    });
  });
});

describe('HandlerExplorer startup values of class decorators', () => {
  it('resolves @NatsService options once for every endpoint of the class', async () => {
    const service = vi.fn((ctx: StartupContext) => ({
      name: `${config(ctx).prefix}-billing`,
      version: '1.0.0',
      connection: 'svcconn',
    }));
    @Injectable()
    @NatsService(service)
    class Billing {
      @ServiceEndpoint('a') a() {}
      @ServiceEndpoint('b') b() {}
      @ServiceEndpoint('c') c() {}
    }
    const defs = await explore(Billing);
    expect(defs).toHaveLength(3);
    for (const def of defs) {
      expect(def).toMatchObject({
        kind: 'service-endpoint',
        connection: 'svcconn',
        service: { name: 'prod-billing', connection: 'svcconn' },
      });
    }
    expect(service).toHaveBeenCalledOnce();
  });

  it('resolves @CloudEventRouter options', async () => {
    @Injectable()
    @CloudEventRouter(() => ({ subscribe: 'events', unmatched: 'term' }))
    class Events {
      @OnCloudEvent('order.created') on() {}
    }
    const [def] = await explore(Events);
    expect(def).toMatchObject({
      kind: 'subscribe',
      subject: 'events',
      methodName: '@CloudEventRouter',
      router: { unmatched: 'term' },
    });
  });

  it('checks resolved @CloudEventRouter options like written ones', async () => {
    @Injectable()
    @CloudEventRouter(() => ({ subscribe: 'a', unmatched: 'drop' }) as never)
    class Events {
      @OnCloudEvent('order.created') on() {}
    }
    expect((await exploreError(Events)).message).toBe(
      `@CloudEventRouter Events has invalid unmatched "drop"; expected 'ack', 'term' or 'dlq'`,
    );
  });

  it('rejects @NatsService options resolved to no object', async () => {
    @Injectable()
    @NatsService(() => null as never)
    class Billing {
      @ServiceEndpoint('a') a() {}
    }
    expect((await exploreError(Billing)).message).toBe(
      '@NatsService Billing: @NatsService options resolved to null; expected an object',
    );
  });

  it('rejects @CloudEventRouter options resolved to no object', async () => {
    @Injectable()
    @CloudEventRouter(() => null as never)
    class Events {
      @OnCloudEvent('order.created') on() {}
    }
    expect((await exploreError(Events)).message).toBe(
      '@CloudEventRouter Events: @CloudEventRouter options resolved to null; expected an object',
    );
  });

  it('names the class when a class options function throws', async () => {
    @Injectable()
    @NatsService(() => {
      throw new Error('no config');
    })
    class Billing {
      @ServiceEndpoint('a') a() {}
    }
    expect((await exploreError(Billing)).message).toBe(
      '@NatsService Billing failed to resolve its @NatsService options: no config',
    );
  });
});
