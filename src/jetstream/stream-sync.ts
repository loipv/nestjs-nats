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

function withoutServerMetadata(field: string, value: unknown): unknown {
  if (field !== 'metadata' || !isPlainObject(value)) return value;
  return Object.fromEntries(
    Object.entries(value).filter(([k]) => !k.startsWith(SERVER_METADATA_PREFIX)),
  );
}

/**
 * Compares only the keys present in `declared` (see `matches` for the rules), ignoring
 * the server's own `_nats.` metadata keys.
 */
export function diffConfig(
  current: object,
  declared: object,
  immutable: readonly string[],
): ConfigDiff[] {
  const cur = current as Record<string, unknown>;
  const diffs: ConfigDiff[] = [];
  for (const [field, value] of Object.entries(declared)) {
    if (value === undefined) continue;
    const currentValue = withoutServerMetadata(field, cur[field]);
    if (!matches(currentValue, value)) {
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
