// Smoke test: a Nest 11 CommonJS app `require()`s the packed library (Node >= 22.12).
const assert = require('node:assert/strict');
require('reflect-metadata');
const { Injectable, Module } = require('@nestjs/common');
const { NestFactory } = require('@nestjs/core');
const { Subscribe, Payload } = require('@loipv/nestjs-nats');
const { tracing } = require('@loipv/nestjs-nats/otel');
const { NatsHealthIndicator } = require('@loipv/nestjs-nats/health');
const { KvIdempotencyStore } = require('@loipv/nestjs-nats/kv');
const { NatsTestingModule, NatsTestHarness } = require('@loipv/nestjs-nats/testing');

const received = [];

class Handlers {
  onOrder(order) {
    received.push(order);
  }
}
// Plain JS has no decorator syntax: apply the decorators by hand.
Subscribe('orders.created')(
  Handlers.prototype,
  'onOrder',
  Object.getOwnPropertyDescriptor(Handlers.prototype, 'onOrder'),
);
Payload()(Handlers.prototype, 'onOrder', 0);
Injectable()(Handlers);

class AppModule {}
Module({ imports: [NatsTestingModule.forRoot()], providers: [Handlers] })(AppModule);

async function main() {
  assert.equal(typeof tracing, 'function');
  assert.equal(typeof NatsHealthIndicator, 'function');
  assert.equal(typeof new KvIdempotencyStore().claim, 'function');

  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  let failure;
  try {
    const result = await NatsTestHarness.create(app).dispatch(
      { handler: 'Handlers.onOrder' },
      { id: 1 },
    );
    assert.equal(result.settlement, 'none');
    assert.deepEqual(received, [{ id: 1 }]);
  } catch (err) {
    failure = err;
  }
  // Close in every case, but a close error must not hide the assertion error.
  try {
    await app.close();
  } catch (closeErr) {
    if (failure === undefined) throw closeErr;
    console.error('app.close() failed as well:', closeErr);
  }
  if (failure !== undefined) throw failure;
  console.log('SMOKE OK');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
