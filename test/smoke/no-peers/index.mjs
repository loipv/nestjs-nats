// Smoke test: without the optional peers the main entry still loads, and the optional
// entry points fail with a clear module-not-found error naming the missing peer.
import assert from 'node:assert/strict';
import 'reflect-metadata';

const main = await import('@loipv/nestjs-nats');
assert.equal(typeof main.NatsModule, 'function');

await assert.rejects(import('@loipv/nestjs-nats/otel'), (err) => {
  assert.equal(err.code, 'ERR_MODULE_NOT_FOUND');
  assert.match(err.message, /@opentelemetry\/api/);
  return true;
});

console.log('SMOKE OK');
