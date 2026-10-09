import type { PipeTransform, Type } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';

export enum NatsParamType {
  PAYLOAD = 0,
  HEADERS = 1,
  SUBJECT = 2,
  MSG = 3,
  CONTEXT = 4,
  CLOUD_EVENT = 5,
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
