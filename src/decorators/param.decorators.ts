import type { PipeTransform, Type } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';

/**
 * Param types stored in `ROUTE_ARGS_METADATA` keys. Nest maps the numeric type to the
 * `ArgumentMetadata.type` seen by pipes: 3 → `'body'`, 4 → `'query'`, 5 → `'param'`,
 * anything else → `'custom'`. `PAYLOAD` is therefore 3 so `ValidationPipe` validates
 * payloads, and the other types avoid 4/5 so pipes never try to validate headers, the
 * raw message or the context. `PAYLOAD` and `CONTEXT` match `RpcParamtype` of
 * `@nestjs/microservices` (3 and 6), so its `@Payload()`/`@Ctx()` resolve to the same
 * values, and 9 (its `GRPC_CALL`) is left unused.
 */
export enum NatsParamType {
  PAYLOAD = 3,
  CONTEXT = 6,
  SUBJECT = 7,
  MSG = 8,
  HEADERS = 10,
  CLOUD_EVENT = 11,
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

/**
 * The CloudEvent attributes of the message (`CloudEventMeta`, `undefined` if it is not a
 * CloudEvent); in a batch handler, an array aligned with the payloads.
 */
export function CloudEvent(): ParameterDecorator {
  return createNatsParamDecorator(NatsParamType.CLOUD_EVENT);
}
