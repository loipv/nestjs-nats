// Smoke test: a Nest 12 ESM app imports the packed library.
import assert from 'node:assert/strict';
import 'reflect-metadata';
import { Payload, Subscribe } from '@loipv/nestjs-nats';
import { NatsHealthIndicator } from '@loipv/nestjs-nats/health';
import { tracing } from '@loipv/nestjs-nats/otel';
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
assert.equal(typeof NatsHealthIndicator, 'function');

const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
try {
  const result = await NatsTestHarness.create(app).dispatch(
    { handler: 'Handlers.onOrder' },
    { id: 1 },
  );
  assert.equal(result.settlement, 'none');
  assert.deepEqual(received, [{ id: 1 }]);
} finally {
  await app.close();
}
console.log('SMOKE OK');
