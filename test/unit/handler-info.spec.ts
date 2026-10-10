import 'reflect-metadata';
import { Injectable } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { Consume, ConsumeOrdered, ConsumePush } from '../../src/decorators/handler.decorators.js';
import { type HandlerDefinition, HandlerExplorer } from '../../src/discovery/handler-explorer.js';
import { toHandlerInfo } from '../../src/pipeline/delivery.js';

const base = {
  connection: 'default',
  className: 'C',
  methodName: 'm',
  instance: {},
  wrapper: {},
  invoke: () => Promise.resolve(),
};

describe('toHandlerInfo', () => {
  it('describes a subscription without Nest internals', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'subscribe',
        subject: 'a.*',
        options: { queue: 'q', concurrency: 2 },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'subscribe',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      subject: 'a.*',
      queue: 'q',
    }));

  it('omits queue when the subscription has none', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'subscribe',
        subject: 'a',
        options: {},
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'subscribe',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      subject: 'a',
    }));

  it('describes a consumer', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'consume',
        delivery: 'pull',
        options: { stream: 'S', durable: 'd', ack: 'manual', config: {} },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'consume',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      stream: 'S',
      durable: 'd',
      delivery: 'pull',
    }));

  it('describes a push consumer', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'consume',
        delivery: 'push',
        options: { stream: 'S', durable: 'd', deliverGroup: 'g' },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'consume',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      stream: 'S',
      durable: 'd',
      delivery: 'push',
    }));

  it.each([
    ['a single subject', 'orders.>', ['orders.>']],
    ['several subjects', ['a', 'b'], ['a', 'b']],
    ['none', undefined, []],
  ])('describes an ordered consumer with %s', (_, filterSubjects, expected) =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'ordered',
        options: { stream: 'S', ...(filterSubjects !== undefined ? { filterSubjects } : {}) },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'ordered',
      stream: 'S',
      filterSubjects: expected,
      connection: 'default',
      className: 'C',
      methodName: 'm',
    }),
  );

  it('describes a service endpoint with its effective subject', () => {
    const def = {
      ...base,
      kind: 'service-endpoint',
      endpoint: 'get',
      options: {},
      service: { name: 'svc', version: '1.0.0', description: 'x' },
    } as unknown as HandlerDefinition;
    expect(toHandlerInfo(def)).toStrictEqual({
      kind: 'service-endpoint',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      service: { name: 'svc', version: '1.0.0' },
      endpoint: 'get',
      subject: 'get',
    });
    expect(
      toHandlerInfo({ ...def, options: { subject: 'svc.get' } } as HandlerDefinition),
    ).toMatchObject({ subject: 'svc.get' });
  });

  it('is frozen', () =>
    expect(
      Object.isFrozen(
        toHandlerInfo({
          ...base,
          kind: 'subscribe',
          subject: 'a',
          options: {},
        } as unknown as HandlerDefinition),
      ),
    ).toBe(true));
});

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
