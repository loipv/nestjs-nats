import type { MsgHdrs } from '@nats-io/transport-node';
import {
  type Context,
  defaultTextMapGetter,
  propagation,
  ROOT_CONTEXT,
  type TextMapGetter,
  type TextMapSetter,
} from '@opentelemetry/api';
import { cloudEventMode } from '../cloudevents/codec.js';
import type { NatsContext } from '../context/nats-context.js';
import type { InboundMessage } from '../pipeline/delivery.js';

const TRACEPARENT = 'traceparent';
const TRACESTATE = 'tracestate';

/** The values of header `name` of `h` (case-insensitive), in order; empty when absent. */
export function headerValues(h: MsgHdrs | undefined, name: string): string[] {
  if (!h) return [];
  const wanted = name.toLowerCase();
  const out: string[] = [];
  for (const key of h.keys()) {
    if (key.toLowerCase() === wanted) out.push(...h.values(key));
  }
  return out;
}

/** Reads propagation fields from NATS headers; header names match case-insensitively. */
export const msgHdrsGetter: TextMapGetter<MsgHdrs | undefined> = {
  keys: (carrier) => carrier?.keys() ?? [],
  get: (carrier, key) => {
    const values = headerValues(carrier, key);
    if (values.length === 0) return undefined;
    return values.length === 1 ? values[0] : values;
  },
};

/** Writes propagation fields into NATS headers, replacing any value under another case. */
export const msgHdrsSetter: TextMapSetter<MsgHdrs> = {
  set: (carrier, key, value) => {
    const wanted = key.toLowerCase();
    for (const existing of carrier.keys()) {
      if (existing !== key && existing.toLowerCase() === wanted) carrier.delete(existing);
    }
    carrier.set(key, value);
  },
};

/**
 * The context propagated with an inbound message: from its NATS headers, or, when they
 * carry no `traceparent` and the message is a structured CloudEvent, from the
 * `traceparent` / `tracestate` extensions of its envelope (other fields, like baggage,
 * still come from the headers).
 */
export function extractContext(message: InboundMessage, ctx: NatsContext): Context {
  const { headers } = message;
  const fromHeaders = propagation.extract(ROOT_CONTEXT, headers, msgHdrsGetter);
  if (headerValues(headers, TRACEPARENT).length > 0 || cloudEventMode(headers) !== 'structured') {
    return fromHeaders;
  }
  const extensions = ctx.cloudEvent?.extensions;
  const traceparent = extensions?.[TRACEPARENT];
  if (traceparent === undefined) return fromHeaders;
  const carrier: Record<string, string> = { [TRACEPARENT]: traceparent };
  const tracestate = extensions?.[TRACESTATE];
  if (tracestate !== undefined) carrier[TRACESTATE] = tracestate;
  return propagation.extract(fromHeaders, carrier, defaultTextMapGetter);
}
