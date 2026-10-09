import { AckPolicy, type ConsumerConfig } from '@nats-io/jetstream';

export const STREAM_IMMUTABLE_FIELDS = ['storage', 'retention'] as const;

export const CONSUMER_IMMUTABLE_FIELDS = [
  'ack_policy',
  'deliver_policy',
  'opt_start_seq',
  'opt_start_time',
  'replay_policy',
] as const;

export interface ConfigDiff {
  field: string;
  current: unknown;
  declared: unknown;
  immutable: boolean;
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => deepEqual(v, b[i]));
  }
  const ao = a as Record<string, unknown>;
  const bo = b as Record<string, unknown>;
  const keys = Object.keys(ao).filter((k) => ao[k] !== undefined);
  const otherKeys = Object.keys(bo).filter((k) => bo[k] !== undefined);
  return keys.length === otherKeys.length && keys.every((k) => deepEqual(ao[k], bo[k]));
}

/** Compares only the keys present in `declared`; arrays are compared in order. */
export function diffConfig(
  current: object,
  declared: object,
  immutable: readonly string[],
): ConfigDiff[] {
  const cur = current as Record<string, unknown>;
  const diffs: ConfigDiff[] = [];
  for (const [field, value] of Object.entries(declared)) {
    if (value === undefined) continue;
    if (!deepEqual(cur[field], value)) {
      diffs.push({
        field,
        current: cur[field],
        declared: value,
        immutable: immutable.includes(field),
      });
    }
  }
  return diffs;
}

export function withConsumerDefaults(cfg: Partial<ConsumerConfig>): Partial<ConsumerConfig> {
  return {
    ...cfg,
    ack_policy: cfg.ack_policy ?? AckPolicy.Explicit,
    max_deliver: cfg.max_deliver ?? -1,
  };
}
