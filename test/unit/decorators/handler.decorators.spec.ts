import 'reflect-metadata';
import { createParamDecorator, ParseIntPipe, type PipeTransform } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import {
  NATS_CONSUME_METADATA,
  NATS_CONSUME_ORDERED_METADATA,
  NATS_CONSUME_PUSH_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from '../../../src/decorators/constants.js';
import {
  Consume,
  ConsumeOrdered,
  ConsumePush,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../../src/decorators/handler.decorators.js';
import { Ctx, Headers, Msg, Payload, Subject } from '../../../src/decorators/param.decorators.js';

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

  @ConsumePush({ stream: 'S', durable: 'D', deliverGroup: 'g' })
  onPush() {}

  @ConsumeOrdered({ stream: 'S', filterSubjects: 'orders.>' })
  onOrdered() {}

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

  it('stores push consume metadata', () =>
    expect(Reflect.getMetadata(NATS_CONSUME_PUSH_METADATA, H.prototype.onPush)).toEqual({
      options: { stream: 'S', durable: 'D', deliverGroup: 'g' },
    }));

  it('stores ordered consume metadata', () =>
    expect(Reflect.getMetadata(NATS_CONSUME_ORDERED_METADATA, H.prototype.onOrdered)).toEqual({
      options: { stream: 'S', filterSubjects: 'orders.>' },
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
    expect(NATS_CONSUME_PUSH_METADATA).toBe('nats:consume-push');
    expect(NATS_CONSUME_ORDERED_METADATA).toBe('nats:consume-ordered');
    expect(NATS_SERVICE_METADATA).toBe('nats:service');
    expect(NATS_ENDPOINT_METADATA).toBe('nats:endpoint');
  });
});
