import { NatsTestHarness, NatsTestingModule } from '@loipv/nestjs-nats/testing';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { EventsRouter } from '../src/events.router.js';
import { OrdersHandlers } from '../src/orders.handlers.js';
import { OrdersProjection } from '../src/orders.projection.js';

async function setup() {
  const app = await Test.createTestingModule({
    imports: [NatsTestingModule.forRoot({ cloudEvents: { source: 'urn:example:test' } })],
    providers: [OrdersHandlers, OrdersProjection, EventsRouter],
  }).compile();
  app.useLogger(false); // the failing-order tests would otherwise print their expected errors
  await app.init();
  return { app, harness: NatsTestHarness.create(app) };
}

describe('OrdersHandlers.bill (@Consume billing)', () => {
  it('acks a good order', async () => {
    const { app, harness } = await setup();
    const res = await harness.dispatch(
      { handler: 'OrdersHandlers.bill' },
      { id: 'o-1', total: 42 },
      { subject: 'orders.created' },
    );
    expect(res.settlement).toBe('ack');
    expect(res.result).toEqual({ billed: true, id: 'o-1' });
    await app.close();
  });

  it('retries a failing order with the configured backoff', async () => {
    const { app, harness } = await setup();
    const res = await harness.dispatch(
      { handler: 'OrdersHandlers.bill' },
      { id: 'o-2', total: 10, fail: true },
      { deliveryCount: 1 },
    );
    expect(res.settlement).toBe('nak');
    expect(res.nakDelay).toBeGreaterThan(0);
    await app.close();
  });

  it('dead-letters a failing order at attempt 4 (maxRetries: 3)', async () => {
    const { app, harness } = await setup();
    const res = await harness.dispatch(
      { handler: 'OrdersHandlers.bill' },
      { id: 'o-2', total: 10, fail: true },
      { deliveryCount: 4 },
    );
    expect(res.settlement).toBe('dlq');
    expect(res.dlq[0]?.subject).toBe('dlq.ORDERS.billing');
    await app.close();
  });
});

describe('OrdersHandlers.get (@Subscribe request/reply)', () => {
  it('replies with a billed order, and with null for an unknown one', async () => {
    const { app, harness } = await setup();
    await harness.dispatch({ handler: 'OrdersHandlers.bill' }, { id: 'o-1', total: 42 });

    const found = await harness.dispatch(
      { handler: 'OrdersHandlers.get' },
      { id: 'o-1' },
      { request: true },
    );
    expect(found.reply).toEqual({ data: { id: 'o-1', total: 42, status: 'billed' } });

    const missing = await harness.dispatch(
      { handler: 'OrdersHandlers.get' },
      { id: 'nope' },
      { request: true },
    );
    expect(missing.reply).toEqual({ data: null });
    await app.close();
  });
});

describe('OrdersProjection.project (@ConsumeOrdered on orders.>)', () => {
  it('counts orders by status and acknowledges nothing', async () => {
    const { app, harness } = await setup();
    for (const [subject, streamSequence] of [
      ['orders.created', 1],
      ['orders.created', 2],
      ['orders.shipped', 3],
    ] as const) {
      const res = await harness.dispatch(
        { handler: 'OrdersProjection.project' },
        { id: `o-${streamSequence}` },
        { subject, streamSequence },
      );
      // An ordered consumer has nothing to settle: a success leaves the message as it is.
      expect(res.settlement).toBe('none');
    }

    const stats = await harness.dispatch(
      { handler: 'OrdersProjection.stats' },
      {},
      { request: true },
    );
    expect(stats.reply).toEqual({ data: { created: 2, shipped: 1 } });
    await app.close();
  });
});

describe('EventsRouter (@CloudEventRouter on events.>)', () => {
  it('routes a CloudEvent to the method matching its type', async () => {
    const { app, harness } = await setup();
    const created = await harness.dispatch(
      { handler: 'EventsRouter.@CloudEventRouter' },
      { orderId: 'o-1' },
      { cloudEvent: { type: 'com.example.order.created' } },
    );
    expect(created.result).toEqual({ handled: 'created', orderId: 'o-1' });

    const shipped = await harness.dispatch(
      { handler: 'EventsRouter.@CloudEventRouter' },
      { orderId: 'o-1' },
      { cloudEvent: { type: 'com.example.order.shipped' } },
    );
    expect(shipped.result).toEqual({
      handled: 'status-changed',
      type: 'com.example.order.shipped',
    });
    await app.close();
  });

  it('skips an event nobody handles (unmatched defaults to ack, nothing to settle on core)', async () => {
    const { app, harness } = await setup();
    const res = await harness.dispatch(
      { handler: 'EventsRouter.@CloudEventRouter' },
      {},
      { cloudEvent: { type: 'com.example.invoice.paid' } },
    );
    expect(res.settlement).toBe('none');
    expect(res.result).toBeUndefined();
    await app.close();
  });
});
