import type { PipeTransform, Type } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';

/**
 * Param types stored in `ROUTE_ARGS_METADATA` keys. Nest maps the numeric type to the
 * `ArgumentMetadata.type` seen by pipes: 3 → `'body'`, 4 → `'query'`, 5 → `'param'`,
 * anything else → `'custom'`. `PAYLOAD` is therefore 3 (like Nest's `RpcParamtype.PAYLOAD`)
 * so `ValidationPipe` validates payloads, and the other types avoid 4/5 so pipes never try
 * to validate headers, the raw message or the context.
 */
export enum NatsParamType {
  PAYLOAD = 3,
  HEADERS = 6,
  SUBJECT = 7,
  MSG = 8,
  CONTEXT = 9,
  CLOUD_EVENT = 10,
}

type Pipe = Type<PipeTransform> | PipeTransform;

function createNatsParamDecorator(
  type: NatsParamType,
  data?: string,
  pipes: Pipe[] = [],
): ParameterDecorator {
  return (target, key, index) => {
    if (key === undefined) return;
    const args = Reflect.getMetadata(ROUTE_ARGS_METADATA, target.constructor, key) ?? {};
    Reflect.defineMetadata(
      ROUTE_ARGS_METADATA,
      { ...args, [`${type}:${index}`]: { index, data, pipes } },
      target.constructor,
      key,
    );
  };
}

export function Payload(): ParameterDecorator;
export function Payload(property: string, ...pipes: Pipe[]): ParameterDecorator;
export function Payload(...pipes: Pipe[]): ParameterDecorator;
export function Payload(...args: (string | Pipe)[]): ParameterDecorator {
  const [first, ...rest] = args;
  if (typeof first === 'string') {
    return createNatsParamDecorator(NatsParamType.PAYLOAD, first, rest as Pipe[]);
  }
  return createNatsParamDecorator(NatsParamType.PAYLOAD, undefined, args as Pipe[]);
}

export function Headers(name?: string): ParameterDecorator {
  return createNatsParamDecorator(NatsParamType.HEADERS, name);
}

export function Subject(): ParameterDecorator {
  return createNatsParamDecorator(NatsParamType.SUBJECT);
}

export function Msg(): ParameterDecorator {
  return createNatsParamDecorator(NatsParamType.MSG);
}

export function Ctx(): ParameterDecorator {
  return createNatsParamDecorator(NatsParamType.CONTEXT);
}
