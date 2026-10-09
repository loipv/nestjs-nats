import type { ParamData } from '@nestjs/common';
import type { ParamsFactory } from '@nestjs/core';
import { NatsBatchContext } from '../context/nats-batch-context.js';
import type { NatsContext } from '../context/nats-context.js';
import { NatsParamType } from '../decorators/param.decorators.js';

type Item = Record<string, unknown> | null | undefined;

/**
 * Resolves `@Payload/@Headers/@Subject/@Msg/@Ctx` values from the `[payload, ctx]`
 * handler args. For a batch every value but the context is an array, index-aligned with
 * the payloads: `@Payload('id')` maps the property over the items, `@Headers()` gives
 * each message's headers, `@Subject()` / `@Msg()` each subject / raw message.
 */
export class NatsParamsFactory implements ParamsFactory {
  exchangeKeyForValue(type: number, data: ParamData, args: unknown[]): unknown {
    const [payload, ctx] = args as [unknown, NatsContext | NatsBatchContext];
    const key = typeof data === 'string' ? data : undefined;
    if (ctx instanceof NatsBatchContext) return batchValue(type, key, payload, ctx);
    switch (type) {
      case NatsParamType.PAYLOAD:
        return key ? (payload as Item)?.[key] : payload;
      case NatsParamType.HEADERS:
        return key ? ctx.headers[key] : ctx.headers;
      case NatsParamType.SUBJECT:
        return ctx.subject;
      case NatsParamType.MSG:
        return ctx.raw;
      case NatsParamType.CONTEXT:
        return ctx;
      case NatsParamType.CLOUD_EVENT:
        return ctx.cloudEvent;
      default:
        return undefined;
    }
  }
}

function batchValue(
  type: number,
  key: string | undefined,
  payload: unknown,
  ctx: NatsBatchContext,
): unknown {
  switch (type) {
    case NatsParamType.PAYLOAD:
      return key && Array.isArray(payload) ? payload.map((item: Item) => item?.[key]) : payload;
    case NatsParamType.HEADERS:
      return ctx.messages.map((m) => (key ? m.headers[key] : m.headers));
    case NatsParamType.SUBJECT:
      return ctx.messages.map((m) => m.subject);
    case NatsParamType.MSG:
      return ctx.messages.map((m) => m.raw);
    case NatsParamType.CONTEXT:
      return ctx;
    case NatsParamType.CLOUD_EVENT:
      return ctx.messages.map((m) => m.cloudEvent);
    default:
      return undefined;
  }
}
