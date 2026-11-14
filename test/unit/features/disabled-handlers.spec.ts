import 'reflect-metadata';
import { Injectable } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { describe, expect, it, vi } from 'vitest';
import type { ConnectionRegistry } from '../../../src/connection/connection-registry.js';
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
import type { DisabledHandler, ExploreOptions } from '../../../src/discovery/handler-definition.js';
import { HandlerExplorer } from '../../../src/discovery/handler-explorer.js';
import { validateHandlers } from '../../../src/discovery/handler-validator.js';
import { NatsConfigError } from '../../../src/errors/errors.js';
import { NatsLifecycle } from '../../../src/module/nats-lifecycle.js';

async function explore(
  providers: Array<new (...args: never[]) => unknown>,
  options?: ExploreOptions,
) {
  const moduleRef = await Test.createTestingModule({
    imports: [DiscoveryModule],
    providers: [HandlerExplorer, ...providers],
  }).compile();
  try {
    return moduleRef.get(HandlerExplorer).explore(options);
  } finally {
    await moduleRef.close();
  }
}

const names = (defs: Array<{ className: string; methodName: string }>) =>
  defs.map((d) => `${d.className}.${d.methodName}`).sort();

describe('disabled handlers', () => {
  it('skips every method handler kind disabled with true, keeps false and unset', async () => {
    @Injectable()
    class Handlers {
      @Subscribe('a', { disabled: true }) subOff() {}
      @Subscribe('b', { disabled: false }) subOn() {}
      @Subscribe('c') subDefault() {}
      @Consume({ stream: 'S', durable: 'pull', disabled: true }) pullOff() {}
      @ConsumePush({ stream: 'S', durable: 'push', disabled: true }) pushOff() {}
      @ConsumeOrdered({ stream: 'S', disabled: true }) orderedOff() {}
    }
    expect(names(await explore([Handlers]))).toEqual(['Handlers.subDefault', 'Handlers.subOn']);
  });

  it('calls a disabled function at explore time', async () => {
    const flag = vi.fn(() => true);
    @Injectable()
    class Handlers {
      @Subscribe('a', { disabled: flag }) off() {}
      @Consume({ stream: 'S', durable: 'd', disabled: () => false }) on() {}
    }
    expect(flag).not.toHaveBeenCalled();
    expect(names(await explore([Handlers]))).toEqual(['Handlers.on']);
    expect(flag).toHaveBeenCalledOnce();
  });

  it('skips a disabled service endpoint, and every endpoint of a disabled service', async () => {
    @NatsService({ name: 'on', version: '1.0.0' })
    class On {
      @ServiceEndpoint('a', { disabled: true }) a() {}
      @ServiceEndpoint('b') b() {}
    }
    @NatsService({ name: 'off', version: '1.0.0', disabled: () => true })
    class Off {
      @ServiceEndpoint('c') c() {}
      @ServiceEndpoint('d', { disabled: false }) d() {}
    }
    expect(names(await explore([On, Off]))).toEqual(['On.b']);
  });

  it('skips a disabled endpoint even in a class without @NatsService', async () => {
    @Injectable()
    class Orphan {
      @ServiceEndpoint('a', { disabled: true }) a() {}
    }
    expect(await explore([Orphan])).toEqual([]);
  });

  it('skips a disabled @CloudEventRouter without validating its options', async () => {
    @CloudEventRouter({ subscribe: 'ce', disabled: true, unmatched: 'bogus' as never })
    class Router {
      @OnCloudEvent('a.b') on() {}
    }
    expect(await explore([Router])).toEqual([]);
  });

  it('still rejects an @OnCloudEvent outside a router when the method is disabled', async () => {
    @Injectable()
    class Bad {
      @OnCloudEvent('a.b')
      @Subscribe('a', { disabled: true })
      on() {}
    }
    await expect(explore([Bad])).rejects.toThrow(/without @CloudEventRouter/);
  });

  it('reports each disabled handler with its connection', async () => {
    @NatsService({ name: 'svc', version: '1.0.0', connection: 'other' })
    class Svc {
      @ServiceEndpoint('e', { disabled: true }) e() {}
    }
    @CloudEventRouter({ consume: { stream: 'S', durable: 'ce' }, disabled: true })
    class Router {
      @OnCloudEvent('a.b') on() {}
    }
    @Injectable()
    class Handlers {
      @Subscribe('a', { disabled: true, connection: 'x' }) a() {}
      @Consume({ stream: 'S', durable: 'd', disabled: true }) b() {}
    }
    const disabled: DisabledHandler[] = [];
    await explore([Svc, Router, Handlers], { onDisabled: (d) => disabled.push(d) });
    expect(disabled.sort((l, r) => l.className.localeCompare(r.className))).toEqual([
      { kind: 'subscribe', className: 'Handlers', methodName: 'a', connection: 'x' },
      { kind: 'consume', className: 'Handlers', methodName: 'b', connection: 'default' },
      {
        kind: 'consume',
        className: 'Router',
        methodName: '@CloudEventRouter',
        connection: 'default',
      },
      { kind: 'service-endpoint', className: 'Svc', methodName: 'e', connection: 'other' },
    ]);
  });

  it('lets a disabled consumer share the durable of the one replacing it', async () => {
    @Injectable()
    class Handlers {
      @Consume({ stream: 'S', durable: 'd', disabled: true }) old() {}
      @Consume({ stream: 'S', durable: 'd' }) replacement() {}
    }
    const defs = await explore([Handlers]);
    const mgr = { has: () => true, get: () => ({ options: {} }) } as unknown as ConnectionRegistry;
    expect(() => validateHandlers(defs, mgr)).not.toThrow();
    expect(names(defs)).toEqual(['Handlers.replacement']);
  });

  it('gives a disabled function the providers of the application, calling it once', async () => {
    const off = vi.fn(({ get }: StartupContext) => get<{ off: boolean }>('FLAGS').off);
    @Injectable()
    class Handlers {
      @Subscribe('a', { disabled: off }) a() {}
      @Subscribe('b') b() {}
    }
    const moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [HandlerExplorer, Handlers, { provide: 'FLAGS', useValue: { off: true } }],
    }).compile();
    const explorer = moduleRef.get(HandlerExplorer);
    expect(names(explorer.explore())).toEqual(['Handlers.b']);
    expect(names(explorer.explore())).toEqual(['Handlers.b']);
    expect(off).toHaveBeenCalledOnce();
    await moduleRef.close();
  });

  it('skips a handler disabled by its options function without validating the rest', async () => {
    @Injectable()
    class Handlers {
      @Consume(() => ({ stream: '', durable: '', concurrency: -1, disabled: true })) off() {}
    }
    const onDisabled = vi.fn();
    const defs = await explore([Handlers], { onDisabled });
    expect(defs).toEqual([]);
    expect(onDisabled).toHaveBeenCalledExactlyOnceWith({
      kind: 'consume',
      className: 'Handlers',
      methodName: 'off',
      connection: 'default',
    });
    const mgr = { has: () => true, get: () => ({ options: {} }) } as unknown as ConnectionRegistry;
    expect(() => validateHandlers(defs, mgr)).not.toThrow();
  });

  it('rejects a disabled function that throws, naming the handler', async () => {
    @Injectable()
    class Handlers {
      @Subscribe('a', {
        disabled: () => {
          throw new Error('no config');
        },
      })
      on() {}
    }
    const err = await explore([Handlers]).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsConfigError);
    expect((err as Error).message).toMatch(/Handlers\.on.*disabled.*no config/);
    expect((err as Error).cause).toBeInstanceOf(Error);
  });

  it.each([
    ['a string', 'true'],
    ['a function returning a string', () => 'yes'],
    ['null', null],
  ])('rejects disabled set to %s', async (_label, value) => {
    @Injectable()
    class Handlers {
      @Subscribe('a', { disabled: value as never }) on() {}
    }
    await expect(explore([Handlers])).rejects.toThrow(
      /NATS handler Handlers\.on has invalid disabled .*expected a boolean or a function returning one/,
    );
  });

  it('rejects an invalid disabled of a router or a service', async () => {
    @CloudEventRouter({ subscribe: 'ce', disabled: 1 as never })
    class Router {
      @OnCloudEvent('a.b') on() {}
    }
    await expect(explore([Router])).rejects.toThrow(
      /@CloudEventRouter Router has invalid disabled/,
    );
    @NatsService({ name: 'svc', version: '1.0.0', disabled: 'no' as never })
    class Svc {
      @ServiceEndpoint('e') e() {}
    }
    await expect(explore([Svc])).rejects.toThrow(/@NatsService Svc has invalid disabled/);
  });
});

describe('NatsLifecycle with disabled handlers', () => {
  function fakeLogger() {
    return { log: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
  }

  it('logs each disabled handler at bootstrap through its connection logger', async () => {
    const entries = new Map(
      ['default', 'other'].map((name) => [name, { name, options: {}, logger: fakeLogger() }]),
    );
    const manager = {
      names: () => [...entries.keys()],
      has: (name: string) => entries.has(name),
      get: (name: string) => entries.get(name),
    };
    const explorer = {
      explore: (options?: ExploreOptions) => {
        options?.onDisabled?.({
          kind: 'subscribe',
          className: 'A',
          methodName: 'on',
          connection: 'other',
        });
        // An unknown connection falls back to the default connection's logger.
        options?.onDisabled?.({
          kind: 'consume',
          className: 'B',
          methodName: 'on',
          connection: 'missing',
        });
        return [];
      },
    };
    const lifecycle = new NatsLifecycle(manager as never, {} as never, explorer as never);
    await lifecycle.onApplicationBootstrap();
    expect(entries.get('other')?.logger.log).toHaveBeenCalledWith(
      'NATS handler A.on (subscribe) is disabled',
    );
    expect(entries.get('default')?.logger.log).toHaveBeenCalledWith(
      'NATS handler B.on (consume) is disabled',
    );
  });
});
