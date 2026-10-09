import type { ParamData } from '@nestjs/common';
import type { ParamsFactory } from '@nestjs/core';
import type { NatsContext } from '../context/nats-context.js';
import { NatsParamType } from '../decorators/param.decorators.js';

/** Resolves `@Payload/@Headers/@Subject/@Msg/@Ctx` values from the `[payload, ctx]` handler args. */
export class NatsParamsFactory implements ParamsFactory {
  exchangeKeyForValue(type: number, data: ParamData, args: unknown[]): unknown {
    const [payload, ctx] = args as [unknown, NatsContext];
    const key = typeof data === 'string' ? data : undefined;
    switch (type) {
      case NatsParamType.PAYLOAD:
        return key ? (payload as Record<string, unknown> | null | undefined)?.[key] : payload;
      case NatsParamType.HEADERS:
        return key ? ctx.headers[key] : ctx.headers;
      case NatsParamType.SUBJECT:
        return ctx.subject;
      case NatsParamType.MSG:
        return ctx.raw;
      case NatsParamType.CONTEXT:
        return ctx;
      default:
        return undefined;
    }
  }
}
