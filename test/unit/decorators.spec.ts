import 'reflect-metadata';
import { createParamDecorator, ParseIntPipe, type PipeTransform } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';
import { describe, expect, it } from 'vitest';
import {
  NATS_CONSUME_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from '../../src/decorators/constants.js';
import {
  Consume,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../src/decorators/handler.decorators.js';
import {
  Ctx,
  Headers,
  Msg,
  NatsParamType,
  Payload,
  Subject,
} from '../../src/decorators/param.decorators.js';

const Custom = createParamDecorator((_d: unknown, _c: unknown) => 1);
const pipeInstance: PipeTransform = { transform: (v) => v };

@NatsService({ name: 'svc', version: '1.0.0' })
class H {
  @Subscribe('a.*', { queue: 'q' })
  onA(
    @Payload() _p: unknown,
    @Headers('x') _x: string,
    @Subject() _s: string,
    @Ctx() _c: unknown,
    @Msg() _m: unknown,
  ) {}

  @Consume({ stream: 'S', durable: 'd' })
  onC(@Payload('id', ParseIntPipe) _id: number) {}

  @Subscribe('b')
  onB(
    @Payload(ParseIntPipe, pipeInstance) _p: number,
    @Headers() _h: unknown,
    @Custom() _c: unknown,
  ) {}

  @ServiceEndpoint('echo', { subject: 'svc.echo' })
  echo() {}
}

describe('handler decorators', () => {
  it('stores subscribe metadata', () =>
    expect(Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, H.prototype.onA)).toEqual({
      subject: 'a.*',
      options: { queue: 'q' },
    }));

  it('stores consume metadata', () =>
    expect(Reflect.getMetadata(NATS_CONSUME_METADATA, H.prototype.onC)).toEqual({
      options: { stream: 'S', durable: 'd' },
    }));

  it('stores endpoint metadata', () =>
    expect(Reflect.getMetadata(NATS_ENDPOINT_METADATA, H.prototype.echo)).toEqual({
      name: 'echo',
      options: { subject: 'svc.echo' },
    }));

  it('stores service metadata on the class', () =>
    expect(Reflect.getMetadata(NATS_SERVICE_METADATA, H)).toEqual({
      name: 'svc',
      version: '1.0.0',
    }));

  it('uses the documented metadata keys', () => {
    expect(NATS_SUBSCRIBE_METADATA).toBe('nats:subscribe');
    expect(NATS_CONSUME_METADATA).toBe('nats:consume');
    expect(NATS_SERVICE_METADATA).toBe('nats:service');
    expect(NATS_ENDPOINT_METADATA).toBe('nats:endpoint');
  });
});

describe('param decorators', () => {
  it('stores params under ROUTE_ARGS_METADATA', () => {
    const meta = Reflect.getMetadata(ROUTE_ARGS_METADATA, H, 'onA');
    expect(meta[`${NatsParamType.PAYLOAD}:0`]).toEqual({ index: 0, data: undefined, pipes: [] });
    expect(meta[`${NatsParamType.HEADERS}:1`].data).toBe('x');
    expect(meta[`${NatsParamType.SUBJECT}:2`].index).toBe(2);
    expect(meta[`${NatsParamType.CONTEXT}:3`].index).toBe(3);
    expect(meta[`${NatsParamType.MSG}:4`].index).toBe(4);
  });

  it('supports property + pipes on Payload', () =>
    expect(
      Reflect.getMetadata(ROUTE_ARGS_METADATA, H, 'onC')[`${NatsParamType.PAYLOAD}:0`],
    ).toEqual({
      index: 0,
      data: 'id',
      pipes: [ParseIntPipe],
    }));

  it('treats a non-string first argument of Payload as a pipe', () => {
    const meta = Reflect.getMetadata(ROUTE_ARGS_METADATA, H, 'onB');
    expect(meta[`${NatsParamType.PAYLOAD}:0`]).toEqual({
      index: 0,
      data: undefined,
      pipes: [ParseIntPipe, pipeInstance],
    });
  });

  it('coexists with Nest custom param decorators', () => {
    const meta = Reflect.getMetadata(ROUTE_ARGS_METADATA, H, 'onB');
    expect(Object.keys(meta)).toHaveLength(3);
    expect(meta[`${NatsParamType.HEADERS}:1`]).toEqual({ index: 1, data: undefined, pipes: [] });
  });

  it('defines the param type enum values', () => {
    expect(NatsParamType).toMatchObject({
      PAYLOAD: 3,
      CONTEXT: 6,
      SUBJECT: 7,
      MSG: 8,
      HEADERS: 10,
      CLOUD_EVENT: 11,
    });
  });

  it('lines up with @nestjs/microservices param types', () => {
    // RpcParamtype of @nestjs/microservices: PAYLOAD = 3, CONTEXT = 6, GRPC_CALL = 9.
    // Its @Payload()/@Ctx() must resolve to our payload/context, and nothing may map to
    // GRPC_CALL or to Nest's 'query'/'param' pipe types (4/5).
    expect(NatsParamType.PAYLOAD).toBe(3);
    expect(NatsParamType.CONTEXT).toBe(6);
    const values = Object.values(NatsParamType).filter((v) => typeof v === 'number');
    expect(values).not.toContain(4);
    expect(values).not.toContain(5);
    expect(values).not.toContain(9);
  });
});
