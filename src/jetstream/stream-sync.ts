import { AckPolicy, type ConsumerConfig } from '@nats-io/jetstream';

export const STREAM_IMMUTABLE_FIELDS = ['storage', 'retention'] as const;

export const CONSUMER_IMMUTABLE_FIELDS = [
  'ack_policy',
  'deliver_policy',
  'opt_start_seq',
  'opt_start_time',
  'replay_policy',
] as const;

/**
 * Stream fields kept, with a warning, when removed from a declaration: the immutable ones,
 * the ones the server refuses to turn off (`deny_delete`, `deny_purge`, `allow_msg_ttl`,
 * `sealed`, `allow_msg_counter`, `allow_msg_schedules`), and `mirror`, whose removal would
 * promote the mirror for good.
 */
export const STREAM_KEPT_FIELDS = [
  ...STREAM_IMMUTABLE_FIELDS,
  'deny_delete',
  'deny_purge',
  'allow_msg_ttl',
  'sealed',
  'mirror',
  'allow_msg_counter',
  'allow_msg_schedules',
] as const;

/**
 * Consumer fields kept, with a warning, when removed from a declaration: the immutable ones
 * and the ones the server refuses to update (`deliver_subject`, `flow_control`,
 * `idle_heartbeat`, `max_waiting`).
 */
export const CONSUMER_KEPT_FIELDS = [
  ...CONSUMER_IMMUTABLE_FIELDS,
  'deliver_subject',
  'flow_control',
  'idle_heartbeat',
  'max_waiting',
] as const;

export interface ConfigDiff {
  field: string;
  current: unknown;
  declared: unknown;
  immutable: boolean;
}

const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);

/** The zero value the server omits from its JSON for a declared value of the same type. */
function zeroValueOf(declared: unknown): unknown {
  if (typeof declared === 'boolean') return false;
  if (typeof declared === 'number') return 0;
  if (typeof declared === 'string') return '';
  if (Array.isArray(declared)) return [];
  if (isPlainObject(declared)) return {};
  return undefined;
}

/**
 * Whether the server's `current` value satisfies the `declared` one. The server omits
 * zero values (`false`, `0`, `''`, `[]`, `{}`) and fills in defaults, so an absent current
 * value matches a declared zero value, and objects (also inside arrays) only need to
 * contain the declared keys. Arrays are compared in order.
 */
function matches(current: unknown, declared: unknown): boolean {
  if (declared === undefined) return true;
  const cur = current === undefined ? zeroValueOf(declared) : current;
  if (Object.is(cur, declared)) return true;
  if (Array.isArray(declared)) {
    return (
      Array.isArray(cur) &&
      cur.length === declared.length &&
      declared.every((v, i) => matches(cur[i], v))
    );
  }
  if (isPlainObject(declared)) {
    return isPlainObject(cur) && Object.keys(declared).every((k) => matches(cur[k], declared[k]));
  }
  return false;
}

/** Metadata keys the server adds to every stream/consumer (`_nats.level`, `_nats.ver`, ...). */
const SERVER_METADATA_PREFIX = '_nats.';

/** Metadata key that remembers the declared top-level fields, so removed ones can be reset. */
export const SNAPSHOT_KEY = 'nestjs-nats.declared';

/**
 * Drops the server's own `_nats.` keys from metadata, and with `exact` also `SNAPSHOT_KEY`:
 * exactly compared metadata must not count the library's key as an extra one.
 */
function withoutServerMetadata(field: string, value: unknown, exact: boolean): unknown {
  if (field !== 'metadata' || !isPlainObject(value)) return value;
  return Object.fromEntries(
    Object.entries(value).filter(
      ([k]) => !k.startsWith(SERVER_METADATA_PREFIX) && !(exact && k === SNAPSHOT_KEY),
    ),
  );
}

/** Whether declared `metadata` equals the server's: same keys, same values, nothing extra. */
function metadataEquals(current: unknown, declared: Record<string, unknown>): boolean {
  const cur = isPlainObject(current) ? current : {};
  const keys = Object.keys(declared);
  return Object.keys(cur).length === keys.length && keys.every((k) => cur[k] === declared[k]);
}

/**
 * The declaration without its top-level `null` fields: `null` means omitted, so the server
 * keeps (or applies) its default instead of drifting from what it stores (`-1`, 2 min, ...).
 */
export function withoutNulls<T extends object>(declared: T): T {
  return Object.fromEntries(Object.entries(declared).filter(([, v]) => v !== null)) as T;
}

/**
 * The sorted top-level field names of a declaration, without `undefined` or `null` values,
 * `metadata` and the `exclude`d identity fields (`name`, `durable_name`).
 */
export function declaredFields(cfg: object, exclude: readonly string[]): string[] {
  return Object.entries(cfg)
    .filter(([k, v]) => v !== undefined && v !== null && k !== 'metadata' && !exclude.includes(k))
    .map(([k]) => k)
    .sort();
}

/** The `SNAPSHOT_KEY` value for a list of declared fields. */
export function snapshotOf(fields: readonly string[]): string {
  return fields.join(',');
}

/** The fields a previous sync recorded, or `undefined` when it recorded none. */
export function parseSnapshot(metadata: Record<string, string> | undefined): string[] | undefined {
  const raw = metadata?.[SNAPSHOT_KEY];
  if (raw === undefined) return undefined;
  return raw === '' ? [] : raw.split(',');
}

/**
 * Fields declared by the previous sync but not by this one, and still set on the server.
 * `reset` ones can go back to a default; `kept` ones cannot be changed.
 */
export function removedFields(
  current: object,
  previous: readonly string[] | undefined,
  declared: readonly string[],
  kept: readonly string[],
): { reset: string[]; kept: string[] } {
  const cur = current as Record<string, unknown>;
  const removed = (previous ?? []).filter(
    (f) => f !== 'metadata' && !declared.includes(f) && cur[f] !== undefined,
  );
  return {
    reset: removed.filter((f) => !kept.includes(f)),
    kept: removed.filter((f) => kept.includes(f)),
  };
}

/**
 * The first declared metadata key the library or the server owns (`_nats.*`, `SNAPSHOT_KEY`):
 * the server would never report the declared value back.
 */
export function reservedMetadataKey(
  metadata: Record<string, string> | undefined,
): string | undefined {
  return Object.keys(metadata ?? {}).find(
    (k) => k.startsWith(SERVER_METADATA_PREFIX) || k === SNAPSHOT_KEY,
  );
}

/**
 * The `metadata` to send: the declared one, or else what the server has (without its own
 * `_nats.` keys), plus the snapshot.
 */
export function syncMetadata(
  declared: Record<string, string> | undefined,
  current: Record<string, string> | undefined,
  snapshot: string,
): Record<string, string> {
  const base =
    declared ??
    Object.fromEntries(
      Object.entries(current ?? {}).filter(([k]) => !k.startsWith(SERVER_METADATA_PREFIX)),
    );
  return { ...base, [SNAPSHOT_KEY]: snapshot };
}

/**
 * Compares only the keys present in `declared` (see `matches` for the rules), ignoring
 * the server's own `_nats.` metadata keys. With `exactMetadata` (removed fields are reset),
 * declared `metadata` is compared exactly, also ignoring `SNAPSHOT_KEY`: a key the server has
 * and the declaration lacks is a diff. Otherwise it only needs the declared keys.
 */
export function diffConfig(
  current: object,
  declared: object,
  immutable: readonly string[],
  exactMetadata = false,
): ConfigDiff[] {
  const cur = current as Record<string, unknown>;
  const diffs: ConfigDiff[] = [];
  for (const [field, value] of Object.entries(declared)) {
    if (value === undefined) continue;
    const currentValue = withoutServerMetadata(field, cur[field], exactMetadata);
    const equal =
      exactMetadata && field === 'metadata' && isPlainObject(value)
        ? metadataEquals(currentValue, value)
        : matches(currentValue, value);
    if (!equal) {
      diffs.push({
        field,
        current: currentValue,
        declared: value,
        immutable: immutable.includes(field),
      });
    }
  }
  return diffs;
}

/** Defaults for consumers the library creates; never applied to existing consumers. */
export function withConsumerDefaults(cfg: Partial<ConsumerConfig>): Partial<ConsumerConfig> {
  return {
    ...cfg,
    ack_policy: cfg.ack_policy ?? AckPolicy.Explicit,
    max_deliver: cfg.max_deliver ?? -1,
  };
}

/** 5 s, in nanoseconds. */
const PUSH_IDLE_HEARTBEAT = 5_000_000_000;

/**
 * Defaults for push consumers the library creates (merged under the declared config):
 * a deliver subject of their own, flow control, idle heartbeats, and as many pending
 * messages as the handler can hold at once. With a deliver group, only the subject and
 * the group: the server supports neither flow control nor idle heartbeats there, and
 * `max_ack_pending` is shared by every instance of the group, so the server default
 * applies rather than one instance's capacity.
 */
export function pushConsumerDefaults(o: {
  stream: string;
  durable: string;
  deliverGroup?: string;
  maxAckPending: number;
}): Partial<ConsumerConfig> {
  const deliver_subject = `_nats.push.${o.stream}.${o.durable}`;
  if (o.deliverGroup !== undefined) return { deliver_subject, deliver_group: o.deliverGroup };
  return {
    deliver_subject,
    flow_control: true,
    idle_heartbeat: PUSH_IDLE_HEARTBEAT,
    max_ack_pending: o.maxAckPending,
  };
}
