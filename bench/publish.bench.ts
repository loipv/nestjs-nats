import { test } from 'vitest';
import { NatsClient } from '../dist/client/nats-client.js';
import type { NatsPlugin } from '../dist/pipeline/types.js';
import { benchEntry } from './support.js';

const enc = new TextEncoder();
const syncPlugin: NatsPlugin = { name: 'sync', outbound: { handle: (_m, next) => next() } };
const asyncPlugin: NatsPlugin = {
  name: 'async',
  outbound: {
    async handle(_m, next) {
      await next();
    },
  },
};

test('NatsClient.publish', async ({ bench }) => {
  const raw = benchEntry().nc;
  const plain = new NatsClient(benchEntry());
  const withSync = new NatsClient(benchEntry([syncPlugin]));
  const withAsync = new NatsClient(benchEntry([asyncPlugin]));
  let i = 0;
  await bench.compare(
    bench('nc.publish + JSON (baseline)', () => {
      raw.publish('a', enc.encode(JSON.stringify({ i: i++ })));
    }),
    bench('no plugin', () => {
      plain.publish('a', { i: i++ });
    }),
    bench('1 sync plugin', async () => {
      withSync.publish('a', { i: i++ });
      await withSync.flush();
    }),
    bench('1 async plugin', async () => {
      withAsync.publish('a', { i: i++ });
      await withAsync.flush();
    }),
  );
});
