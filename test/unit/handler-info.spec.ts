import { describe, expect, it } from 'vitest';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
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
        options: { stream: 'S', durable: 'd', ack: 'manual', config: {} },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'consume',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      stream: 'S',
      durable: 'd',
    }));

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
