import { isDeepStrictEqual } from 'node:util';
import { NatsConfigError } from '../errors/errors.js';
import type { ConsumerDeclaration, NatsModuleOptions, StreamDeclaration } from './options.js';

/**
 * Whether a connection resets fields removed from its declarations. Throws a
 * `NatsConfigError` for an invalid `jetstream.removedFields`.
 */
export function resetsRemovedFields(connection: string, options: NatsModuleOptions): boolean {
  const mode: unknown = options.jetstream?.removedFields;
  if (mode === undefined || mode === 'ignore') return false;
  if (mode === 'reset') return true;
  throw new NatsConfigError(
    `NATS connection "${connection}" jetstream.removedFields must be 'reset' or 'ignore', got ${JSON.stringify(mode)}`,
  );
}

/**
 * Merges the declarations of one stream into one (`forRoot` and every `forFeature` of a
 * connection), in the order the names first appear: with removed fields reset, separate
 * declarations would reset each other's fields on every boot.
 */
export function mergeStreams(streams: StreamDeclaration[]): StreamDeclaration[] {
  return mergeBy(
    streams,
    (s) => s.name,
    (s) => `stream "${s.name}"`,
    (s) => s,
    (_, config) => config as StreamDeclaration,
  );
}

/** Merges the module declarations of one consumer (same stream and durable) into one. */
export function mergeConsumers(consumers: ConsumerDeclaration[]): ConsumerDeclaration[] {
  return mergeBy(
    consumers,
    consumerKey,
    (c) => `consumer "${c.stream}/${c.config.durable_name}"`,
    (c) => c.config,
    (c, config) => ({ stream: c.stream, config: config as ConsumerDeclaration['config'] }),
  );
}

/** Identifies a consumer of a connection. */
export function consumerKey(c: { stream: string; config: { durable_name: string } }): string {
  return JSON.stringify([c.stream, c.config.durable_name]);
}

function mergeBy<T>(
  items: T[],
  key: (item: T) => string,
  label: (item: T) => string,
  configOf: (item: T) => object,
  rebuild: (first: T, config: Record<string, unknown>) => T,
): T[] {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const group = groups.get(key(item));
    if (group) group.push(item);
    else groups.set(key(item), [item]);
  }
  return [...groups.values()].map((group) => {
    const [first] = group as [T, ...T[]];
    if (group.length === 1) return first;
    return rebuild(first, mergeConfigs(label(first), group.map(configOf)));
  });
}

/**
 * The union of the fields of several declarations; `metadata` is merged by key. A field (or
 * metadata key) declared with different values throws. `null` and `undefined` mean left out.
 */
function mergeConfigs(label: string, configs: object[]): Record<string, unknown> {
  const merged: Record<string, unknown> = {};
  const metadata = new Map<string, unknown>();
  let hasMetadata = false;
  const conflict = (what: string) =>
    new NatsConfigError(`JetStream ${label} is declared more than once with different ${what}`);
  for (const config of configs) {
    for (const [field, value] of Object.entries(config)) {
      if (value === undefined || value === null) continue;
      if (field === 'metadata') {
        hasMetadata = true;
        for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
          if (metadata.has(k) && metadata.get(k) !== v) throw conflict(`metadata key "${k}"`);
          metadata.set(k, v);
        }
        continue;
      }
      if (Object.hasOwn(merged, field) && !isDeepStrictEqual(merged[field], value))
        throw conflict(field);
      merged[field] = value;
    }
  }
  return hasMetadata ? { ...merged, metadata: Object.fromEntries(metadata) } : merged;
}
