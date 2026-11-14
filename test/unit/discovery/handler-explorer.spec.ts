import 'reflect-metadata';
import { Controller, Injectable, Scope, type Type } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsContext } from '../../../src/context/nats-context.js';
import { CloudEventRouter, OnCloudEvent } from '../../../src/decorators/cloudevent.decorators.js';
import {
  Consume,
  ConsumeOrdered,
  ConsumePush,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../../src/decorators/handler.decorators.js';
import { Payload } from '../../../src/decorators/param.decorators.js';
import type { HandlerDefinition } from '../../../src/discovery/handler-definition.js';
import { HandlerExplorer } from '../../../src/discovery/handler-explorer.js';
import { NatsConfigError } from '../../../src/errors/errors.js';
import { toHandlerInfo } from '../../../src/pipeline/delivery.js';

@Injectable()
class Consumers {
  @Consume({ stream: 'S', durable: 'pull' })
  pull() {}

  @ConsumePush({ stream: 'S', durable: 'push', deliverGroup: 'g' })
  push() {}

  @ConsumeOrdered({ stream: 'S', filterSubjects: 'orders.>' })
  ordered() {}
}

describe('HandlerExplorer consumers', () => {
  it('defines pull, push and ordered handlers', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [HandlerExplorer, Consumers],
    }).compile();
    const defs = moduleRef.get(HandlerExplorer).explore();
    await moduleRef.close();
    const by = (m: string) => defs.find((d) => d.methodName === m);
    expect(by('pull')).toMatchObject({ kind: 'consume', delivery: 'pull' });
    expect(by('push')).toMatchObject({
      kind: 'consume',
      delivery: 'push',
      options: { stream: 'S', durable: 'push', deliverGroup: 'g' },
    });
    const ordered = by('ordered') as HandlerDefinition;
    expect(ordered).toMatchObject({ kind: 'ordered', options: { stream: 'S' } });
    expect(toHandlerInfo(ordered)).toStrictEqual({
      kind: 'ordered',
      stream: 'S',
      filterSubjects: ['orders.>'],
      connection: 'default',
      className: 'Consumers',
      methodName: 'ordered',
    });
  });
});

/** The definitions `HandlerExplorer` finds among `providers`. */
async function explore(...providers: Type[]): Promise<HandlerDefinition[]> {
  const moduleRef = await Test.createTestingModule({
    imports: [DiscoveryModule],
    providers: [HandlerExplorer, ...providers],
  }).compile();
  try {
    return moduleRef.get(HandlerExplorer).explore();
  } finally {
    await moduleRef.close();
  }
}

describe('HandlerExplorer.explore', () => {
  it('defines one handler per decorator of a method', async () => {
    @Injectable()
    class Both {
      @Subscribe('orders.created')
      @Consume({ stream: 'ORDERS', durable: 'audit' })
      on() {}
    }
    const defs = await explore(Both);
    expect(defs.map((d) => d.kind).sort()).toEqual(['consume', 'subscribe']);
    expect(new Set(defs.map((d) => d.methodName))).toEqual(new Set(['on']));
  });

  it('finds nothing in providers without handlers', async () => {
    @Injectable()
    class Plain {
      on() {}
    }
    await expect(explore(Plain)).resolves.toEqual([]);
  });

  it('rejects an @OnCloudEvent method that is also a handler', async () => {
    @CloudEventRouter({ subscribe: 'events' })
    @Injectable()
    class Router {
      @OnCloudEvent('order.created')
      @Subscribe('orders.created')
      on() {}
    }
    const exploring = explore(Router);
    await expect(exploring).rejects.toThrow(NatsConfigError);
    await expect(exploring).rejects.toThrow(
      '@OnCloudEvent Router.on cannot also be a @Subscribe/@Consume/@ConsumePush/@ConsumeOrdered/@ServiceEndpoint handler',
    );
  });
});

@Injectable()
class Handlers {
  @Subscribe('y', { connection: 'other', queue: 'q' })
  fail() {}

  @Consume({ stream: 'S', durable: 'd', connection: 'js' })
  consume() {}
}

@NatsService({ name: 'svc', version: '1.0.0', connection: 'svcconn' })
@Injectable()
class Svc {
  @ServiceEndpoint('get', { subject: 'svc.get' })
  get() {}

  @Subscribe('not-endpoint')
  plain() {}
}

// @ServiceEndpoint outside a @NatsService class: returned for validation to reject.
@Injectable()
class Orphan {
  @ServiceEndpoint('orphan')
  orphan() {}
}

@Injectable({ scope: Scope.REQUEST })
class Scoped {
  @Subscribe('scoped')
  on() {}
}

@Controller()
class Ctrl {
  @Subscribe('from.controller')
  onCtrl(@Payload('v') v: number) {
    return v * 2;
  }
}

// Exposed under two tokens via factories returning the same instance.
class Shared {
  @Subscribe('shared')
  onShared() {}
}
const shared = new Shared();

describe('HandlerExplorer discovery', () => {
  let moduleRef: TestingModule;
  let defs: HandlerDefinition[];
  const find = (method: string) => {
    const d = defs.find((x) => x.methodName === method);
    if (!d) throw new Error(`no handler ${method}`);
    return d;
  };

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      controllers: [Ctrl],
      providers: [
        HandlerExplorer,
        Handlers,
        Svc,
        { provide: 'SVC_ALIAS', useExisting: Svc },
        Orphan,
        Scoped,
        { provide: 'SHARED_1', useFactory: () => shared },
        { provide: 'SHARED_2', useFactory: () => shared },
      ],
    }).compile();
    defs = moduleRef.get(HandlerExplorer).explore();
  });

  afterAll(() => moduleRef.close());

  it('discovers subscribe, consume and service-endpoint handlers', () => {
    expect(
      defs.map((d) => `${d.kind}:${d.className}.${d.methodName}@${d.connection}`).sort(),
    ).toEqual(
      [
        'subscribe:Handlers.fail@other',
        'consume:Handlers.consume@js',
        'service-endpoint:Svc.get@svcconn',
        'service-endpoint:Orphan.orphan@default',
        'subscribe:Svc.plain@svcconn',
        'subscribe:Scoped.on@default',
        'subscribe:Ctrl.onCtrl@default',
        'subscribe:Shared.onShared@default',
      ].sort(),
    );
  });

  it('ignores useExisting aliases (one definition per handler)', () => {
    expect(defs.filter((d) => d.className === 'Svc')).toHaveLength(2);
    expect(defs.filter((d) => d.methodName === 'get')).toHaveLength(1);
  });

  it('dedupes the same instance exposed under several tokens', () => {
    const found = defs.filter((d) => d.methodName === 'onShared');
    expect(found).toHaveLength(1);
    expect(found[0]?.instance).toBe(shared);
  });

  it('discovers handlers on controllers and wraps them for invocation', async () => {
    const ctrl = find('onCtrl');
    expect(ctrl).toMatchObject({ kind: 'subscribe', subject: 'from.controller' });
    expect(ctrl.instance).toBeInstanceOf(Ctrl);
    const ctx = new NatsContext({
      kind: 'core',
      connection: 'default',
      raw: { subject: 'x', reply: '', headers: undefined } as never,
    });
    await expect(ctrl.invoke({ v: 21 }, ctx)).resolves.toBe(42);
  });

  it('exposes handler metadata', () => {
    expect(find('fail')).toMatchObject({
      kind: 'subscribe',
      subject: 'y',
      options: { queue: 'q' },
    });
    const ep = find('get');
    expect(ep).toMatchObject({
      kind: 'service-endpoint',
      endpoint: 'get',
      options: { subject: 'svc.get' },
      service: { name: 'svc', version: '1.0.0' },
    });
    expect(ep.instance).toBeInstanceOf(Svc);
    expect(find('consume')).toMatchObject({
      kind: 'consume',
      options: { stream: 'S', durable: 'd' },
    });
    expect(find('plain')).toMatchObject({
      kind: 'subscribe',
      subject: 'not-endpoint',
      options: {},
    });
  });

  it('returns endpoints of classes without @NatsService for validation', () => {
    expect(find('orphan')).toMatchObject({ kind: 'service-endpoint', service: undefined });
  });

  it('returns request-scoped handlers instead of skipping them', () => {
    const scoped = defs.find((d) => d.className === 'Scoped');
    expect(scoped).toBeDefined();
    expect(scoped?.wrapper.isDependencyTreeStatic()).toBe(false);
  });
});
