import { randomUUID } from 'node:crypto';
import { headers as createHeaders, type MsgHdrs } from '@nats-io/transport-node';
import { type Codec, decodePayload, encodePayload } from '../codec/codec.js';
import { NatsConfigError, NatsDecodeError } from '../errors/errors.js';
import { CE_STRUCTURED_CONTENT_TYPE, type CloudEventInit, type CloudEventMeta } from './types.js';

type CeMessage = { data: Uint8Array; headers?: MsgHdrs };

const CE_PREFIX = 'ce-';
const REQUIRED = ['id', 'source', 'type', 'specversion'] as const;
const OPTIONAL = ['time', 'subject', 'dataschema', 'datacontenttype'] as const;
const RESERVED = new Set<string>([...REQUIRED, ...OPTIONAL, 'data', 'data_base64']);
const KNOWN = RESERVED;
const EXTENSION_NAME = /^[a-z0-9]+$/;

function isValidExtensionName(name: string): boolean {
  return EXTENSION_NAME.test(name) && !RESERVED.has(name);
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function getHeader(h: MsgHdrs | undefined, name: string): string | undefined {
  if (!h) return undefined;
  const wanted = name.toLowerCase();
  for (const key of h.keys()) {
    if (key.toLowerCase() === wanted) return h.values(key)[0];
  }
  return undefined;
}

export function cloudEventMode(headers?: MsgHdrs): 'binary' | 'structured' | undefined {
  if (!headers) return undefined;
  if (getHeader(headers, 'ce-specversion') !== undefined) return 'binary';
  const ct = getHeader(headers, 'content-type');
  if (ct?.trim().toLowerCase().startsWith(CE_STRUCTURED_CONTENT_TYPE)) return 'structured';
  return undefined;
}

function missing(attrs: Record<string, string | undefined>): string | undefined {
  return REQUIRED.find((a) => !attrs[a]);
}

function buildMeta(
  attrs: Record<string, string | undefined>,
  extensions: Record<string, string>,
): CloudEventMeta {
  const meta: CloudEventMeta = {
    id: attrs.id as string,
    source: attrs.source as string,
    type: attrs.type as string,
    specversion: attrs.specversion as string,
    extensions,
  };
  for (const key of OPTIONAL) {
    const v = attrs[key];
    if (v !== undefined) meta[key] = v;
  }
  return meta;
}

function binaryParts(h: MsgHdrs): {
  attrs: Record<string, string | undefined>;
  ext: Record<string, string>;
} {
  const attrs: Record<string, string | undefined> = {};
  const ext: Record<string, string> = {};
  for (const key of h.keys()) {
    const lower = key.toLowerCase();
    if (!lower.startsWith(CE_PREFIX)) continue;
    const name = lower.slice(CE_PREFIX.length);
    const value = h.values(key)[0];
    if (value === undefined || name === '') continue;
    if (
      (REQUIRED as readonly string[]).includes(name) ||
      name === 'time' ||
      name === 'subject' ||
      name === 'dataschema'
    ) {
      attrs[name] = value;
    } else if (name !== 'datacontenttype' && name !== 'data' && name !== 'data_base64') {
      ext[name] = value;
    }
  }
  const contentType = getHeader(h, 'content-type');
  if (contentType !== undefined) attrs.datacontenttype = contentType;
  return { attrs, ext };
}

/** The media type of a content type, lowercased and without parameters. */
function mediaType(contentType: string): string {
  return (contentType.split(';')[0] ?? '').trim().toLowerCase();
}

function isJsonMediaType(type: string): boolean {
  return type === 'application/json' || type === 'text/json' || type.endsWith('+json');
}

/**
 * Decodes the data of a CloudEvent with `codec`, unless the event declares a non-JSON
 * `datacontenttype` other than the codec's content type: such data is handed over as
 * its raw bytes, since the codec cannot read it. The codec gets the declared content type
 * as the `Content-Type` of `headers` (built when absent).
 */
function decodeData(
  codec: Codec,
  bytes: Uint8Array,
  datacontenttype: string | undefined,
  headers?: MsgHdrs,
): unknown {
  if (datacontenttype === undefined) return decodePayload(codec, bytes, headers);
  const declared = mediaType(datacontenttype);
  if (!isJsonMediaType(declared) && declared !== mediaType(codec.contentType)) return bytes;
  let h = headers;
  if (!h) {
    h = createHeaders();
    h.set('Content-Type', datacontenttype);
  }
  return decodePayload(codec, bytes, h);
}

function parseEnvelope(data: Uint8Array): Record<string, unknown> {
  let env: unknown;
  try {
    env = JSON.parse(textDecoder.decode(data));
  } catch (cause) {
    throw new NatsDecodeError('Invalid structured CloudEvent: body is not valid JSON', { cause });
  }
  if (typeof env !== 'object' || env === null || Array.isArray(env)) {
    throw new NatsDecodeError('Invalid structured CloudEvent: body is not a JSON object');
  }
  return env as Record<string, unknown>;
}

function envelopeParts(env: Record<string, unknown>): {
  attrs: Record<string, string | undefined>;
  ext: Record<string, string>;
} {
  const attrs: Record<string, string | undefined> = {};
  const ext: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) {
    if (KNOWN.has(key)) {
      if (key !== 'data' && key !== 'data_base64' && typeof value === 'string') attrs[key] = value;
    } else if (typeof value === 'string') {
      ext[key] = value;
    } else if (typeof value === 'number' || typeof value === 'boolean') {
      ext[key] = String(value);
    }
  }
  return { attrs, ext };
}

export function readCloudEventMeta(msg: CeMessage): CloudEventMeta | undefined {
  try {
    const mode = cloudEventMode(msg.headers);
    if (mode === 'binary') {
      const { attrs, ext } = binaryParts(msg.headers as MsgHdrs);
      return missing(attrs) ? undefined : buildMeta(attrs, ext);
    }
    if (mode === 'structured') {
      const { attrs, ext } = envelopeParts(parseEnvelope(msg.data));
      return missing(attrs) ? undefined : buildMeta(attrs, ext);
    }
  } catch {
    return undefined;
  }
  return undefined;
}

/**
 * Reads and validates the attributes of a CloudEvent (binary or structured) without
 * decoding its data; `data(codec)` decodes it on demand. Throws a `NatsDecodeError` when
 * the message is not a CloudEvent, a structured envelope is not a JSON object, or a
 * required attribute is missing.
 */
export function parseCloudEvent(msg: CeMessage): {
  meta: CloudEventMeta;
  data: (codec: Codec) => unknown;
} {
  const mode = cloudEventMode(msg.headers);
  if (mode === undefined) {
    throw new NatsDecodeError('Message is not a CloudEvent');
  }
  if (mode === 'binary') {
    const { attrs, ext } = binaryParts(msg.headers as MsgHdrs);
    const absent = missing(attrs);
    if (absent)
      throw new NatsDecodeError(`Invalid CloudEvent: missing required attribute "${absent}"`);
    const meta = buildMeta(attrs, ext);
    return {
      meta,
      data: (codec) => decodeData(codec, msg.data, meta.datacontenttype, msg.headers),
    };
  }
  const env = parseEnvelope(msg.data);
  const { attrs, ext } = envelopeParts(env);
  const absent = missing(attrs);
  if (absent)
    throw new NatsDecodeError(`Invalid CloudEvent: missing required attribute "${absent}"`);
  const meta = buildMeta(attrs, ext);
  return {
    meta,
    data: (codec) => {
      if (typeof env.data_base64 === 'string') {
        const bytes = new Uint8Array(Buffer.from(env.data_base64, 'base64'));
        return decodeData(codec, bytes, meta.datacontenttype);
      }
      // An inline JSON value: handed over as is.
      return env.data;
    },
  };
}

export function readCloudEvent(
  msg: CeMessage,
  codec: Codec,
): { meta: CloudEventMeta; data: unknown } {
  const { meta, data } = parseCloudEvent(msg);
  return { meta, data: data(codec) };
}

export function resolveCloudEvent(
  init: CloudEventInit,
  defaults: { source?: string; contentType?: string },
  gen: { now?: () => Date; id?: () => string } = {},
): CloudEventMeta {
  const source = init.source ?? defaults.source;
  if (!source) {
    throw new NatsConfigError(
      'CloudEvent source is required: set cloudEvent.source or NatsModule cloudEvents.source',
    );
  }
  if (!init.type) throw new NatsConfigError('CloudEvent type is required');
  for (const name of Object.keys(init.extensions ?? {})) {
    if (!isValidExtensionName(name)) {
      throw new NatsConfigError(
        `Invalid CloudEvent extension name "${name}": must match [a-z0-9]+ and not be a reserved attribute`,
      );
    }
  }
  if (init.datacontenttype === '') {
    throw new NatsConfigError('CloudEvent datacontenttype must not be empty');
  }
  const id = init.id ?? (gen.id ?? randomUUID)();
  if (!id) throw new NatsConfigError('CloudEvent id must not be empty');
  const time = init.time ?? (gen.now ?? (() => new Date()))();
  if (time instanceof Date && Number.isNaN(time.getTime())) {
    throw new NatsConfigError('CloudEvent time is an invalid Date');
  }
  const meta: CloudEventMeta = {
    id,
    source,
    type: init.type,
    specversion: '1.0',
    time: time instanceof Date ? time.toISOString() : time,
    extensions: { ...init.extensions },
  };
  const datacontenttype = init.datacontenttype ?? defaults.contentType;
  if (datacontenttype !== undefined) meta.datacontenttype = datacontenttype;
  if (init.subject !== undefined) meta.subject = init.subject;
  if (init.dataschema !== undefined) meta.dataschema = init.dataschema;
  return meta;
}

export function writeCloudEvent(
  meta: CloudEventMeta,
  data: unknown,
  codec: Codec,
  mode: 'binary' | 'structured',
  headers: MsgHdrs,
): Uint8Array {
  const extensions = Object.entries(meta.extensions).filter(([name]) => isValidExtensionName(name));
  if (mode === 'binary') {
    const body = encodePayload(codec, data);
    for (const [name, value] of extensions) {
      headers.set(`${CE_PREFIX}${name}`, value);
    }
    headers.set('ce-specversion', meta.specversion);
    headers.set('ce-id', meta.id);
    headers.set('ce-source', meta.source);
    headers.set('ce-type', meta.type);
    if (meta.time !== undefined) headers.set('ce-time', meta.time);
    if (meta.subject !== undefined) headers.set('ce-subject', meta.subject);
    if (meta.dataschema !== undefined) headers.set('ce-dataschema', meta.dataschema);
    if (meta.datacontenttype !== undefined) headers.set('Content-Type', meta.datacontenttype);
    return body;
  }
  const env: Record<string, unknown> = Object.fromEntries(extensions);
  env.specversion = meta.specversion;
  env.id = meta.id;
  env.source = meta.source;
  env.type = meta.type;
  if (meta.time !== undefined) env.time = meta.time;
  if (meta.subject !== undefined) env.subject = meta.subject;
  if (meta.dataschema !== undefined) env.dataschema = meta.dataschema;
  if (meta.datacontenttype !== undefined) env.datacontenttype = meta.datacontenttype;
  if (data !== undefined) {
    if (codec.contentType === 'application/json' && !(data instanceof Uint8Array)) {
      env.data = data;
    } else {
      const body = encodePayload(codec, data);
      if (body.length > 0) env.data_base64 = Buffer.from(body).toString('base64');
    }
  }
  headers.set('Content-Type', CE_STRUCTURED_CONTENT_TYPE);
  return textEncoder.encode(JSON.stringify(env));
}

export function cloudEventKey(msg: CeMessage): string | undefined {
  const meta = readCloudEventMeta(msg);
  return meta ? `${meta.source}|${meta.id}` : undefined;
}
