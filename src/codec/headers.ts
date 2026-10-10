import { headers, type MsgHdrs } from '@nats-io/transport-node';

export type NatsHeadersInit = Record<string, string | string[]>;

export function toMsgHdrs(init?: NatsHeadersInit, base?: MsgHdrs): MsgHdrs {
  const out = base ?? headers();
  if (!init) return out;
  for (const [key, value] of Object.entries(init)) {
    for (const v of Array.isArray(value) ? value : [value]) {
      out.append(key, v);
    }
  }
  return out;
}

export function fromMsgHdrs(h?: MsgHdrs): Record<string, string | string[]> {
  const out: Record<string, string | string[]> = {};
  if (!h) return out;
  for (const key of h.keys()) {
    const values = h.values(key);
    out[key] = values.length === 1 ? (values[0] as string) : values;
  }
  return out;
}
