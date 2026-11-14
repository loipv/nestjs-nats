import { headers } from '@nats-io/transport-node';
import { test } from 'vitest';
import { readCloudEvent, resolveCloudEvent, writeCloudEvent } from '../dist/cloudevents/codec.js';
import { jsonCodec } from '../dist/codec/codec.js';

const codec = jsonCodec();
const meta = resolveCloudEvent(
  { type: 'com.example.order.created', source: '/orders', extensions: { tenant: 't1' } },
  { contentType: codec.contentType },
);
const data = { id: 42, total: 99.5, lines: [{ sku: 'a', qty: 2 }] };

test('CloudEvents codec', async ({ bench }) => {
  const binaryHeaders = headers();
  const binary = writeCloudEvent(meta, data, codec, 'binary', binaryHeaders);
  const structuredHeaders = headers();
  const structured = writeCloudEvent(meta, data, codec, 'structured', structuredHeaders);
  await bench.compare(
    bench('write binary', () => {
      writeCloudEvent(meta, data, codec, 'binary', headers());
    }),
    bench('write structured', () => {
      writeCloudEvent(meta, data, codec, 'structured', headers());
    }),
    bench('read binary', () => {
      readCloudEvent({ data: binary, headers: binaryHeaders }, codec);
    }),
    bench('read structured', () => {
      readCloudEvent({ data: structured, headers: structuredHeaders }, codec);
    }),
  );
});
