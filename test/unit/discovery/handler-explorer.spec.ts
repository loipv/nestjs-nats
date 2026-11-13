import 'reflect-metadata';
import { Injectable, type Type } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { CloudEventRouter, OnCloudEvent } from '../../../src/decorators/cloudevent.decorators.js';
import {
  Consume,
  ConsumeOrdered,
  ConsumePush,
  Subscribe,
} from '../../../src/decorators/handler.decorators.js';
import {
  type HandlerDefinition,
  HandlerExplorer,
} from '../../../src/discovery/handler-explorer.js';
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
