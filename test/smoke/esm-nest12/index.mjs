// Smoke test: a Nest 12 ESM app imports the packed library.
import assert from 'node:assert/strict';
import 'reflect-metadata';
import { Payload, Subscribe } from '@loipv/nestjs-nats';
import { NatsHealthIndicator } from '@loipv/nestjs-nats/health';
import { KvIdempotencyStore } from '@loipv/nestjs-nats/kv';
import { metrics, tracing } from '@loipv/nestjs-nats/otel';
import { NatsTestHarness, NatsTestingModule } from '@loipv/nestjs-nats/testing';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

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

assert.equal(typeof tracing, 'function');
assert.equal(typeof metrics, 'function');
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
