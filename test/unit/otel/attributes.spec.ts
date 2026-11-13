import { describe, expect, it } from 'vitest';
import { consumerGroup } from '../../../src/otel/attributes.js';
import type { Delivery, HandlerInfo } from '../../../src/pipeline/delivery.js';

const base = { connection: 'default', className: 'H', methodName: 'on' };
const of = (handler: HandlerInfo) => ({ handler }) as Delivery;

describe('consumerGroup', () => {
  it('is the durable of a pull or push consumer', () => {
    for (const delivery of ['pull', 'push'] as const) {
      expect(
        consumerGroup(of({ kind: 'consume', ...base, stream: 'S', durable: 'billing', delivery })),
      ).toBe('billing');
    }
  });

  it('is the queue group of a subscription, if any', () => {
    expect(consumerGroup(of({ kind: 'subscribe', ...base, subject: 'a', queue: 'workers' }))).toBe(
      'workers',
    );
    expect(consumerGroup(of({ kind: 'subscribe', ...base, subject: 'a' }))).toBeUndefined();
  });

  it('is unset for ordered consumers and service endpoints', () => {
    expect(
      consumerGroup(of({ kind: 'ordered', ...base, stream: 'S', filterSubjects: [] })),
    ).toBeUndefined();
    expect(
      consumerGroup(
        of({
          kind: 'service-endpoint',
          ...base,
          service: { name: 'svc', version: '1.0.0' },
          endpoint: 'get',
          subject: 'get',
        }),
      ),
    ).toBeUndefined();
  });
});
