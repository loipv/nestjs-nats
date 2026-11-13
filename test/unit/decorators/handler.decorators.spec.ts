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
import { NatsConfigError } from '../../../src/errors/errors.js';

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

describe('handler decorators given startup functions', () => {
  const subject = () => 'orders.created';
  const subscribeOptions = () => ({ queue: 'q' });
  const consumeOptions = () => ({ stream: 'S', durable: 'd' });
  const name = () => 'get';
  const endpointOptions = () => ({ subject: 'svc.get' });

  class Fns {
    @Subscribe(subject, subscribeOptions)
    onSub() {}

    @Consume(consumeOptions)
    onC() {}

    @ConsumePush(consumeOptions)
    onPush() {}

    @ConsumeOrdered(() => ({ stream: 'S' }))
    onOrdered() {}

    @ServiceEndpoint(name, endpointOptions)
    get() {}
  }

  it('stores the functions themselves', () => {
    const sub = Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, Fns.prototype.onSub);
    expect(sub.subject).toBe(subject);
    expect(sub.options).toBe(subscribeOptions);
    expect(Reflect.getMetadata(NATS_CONSUME_METADATA, Fns.prototype.onC).options).toBe(
      consumeOptions,
    );
    expect(Reflect.getMetadata(NATS_CONSUME_PUSH_METADATA, Fns.prototype.onPush).options).toBe(
      consumeOptions,
    );
    expect(
      typeof Reflect.getMetadata(NATS_CONSUME_ORDERED_METADATA, Fns.prototype.onOrdered).options,
    ).toBe('function');
    const endpoint = Reflect.getMetadata(NATS_ENDPOINT_METADATA, Fns.prototype.get);
    expect(endpoint.name).toBe(name);
    expect(endpoint.options).toBe(endpointOptions);
  });

  it('rejects @Subscribe given a function twice on one method', () => {
    expect(() => {
      class Orders {
        @Subscribe(subject)
        @Subscribe(subject)
        on() {}
      }
      return Orders;
    }).toThrow(
      '@Subscribe is applied more than once to Orders.on; each handler decorator declares one handler per method',
    );
  });
});

describe('handler decorators applied more than once', () => {
  const TAIL = 'each handler decorator declares one handler per method';

  const methodDecorators: Array<[string, () => MethodDecorator]> = [
    ['@Subscribe', () => Subscribe('orders.created')],
    ['@Consume', () => Consume({ stream: 'ORDERS', durable: 'billing' })],
    ['@ConsumePush', () => ConsumePush({ stream: 'ORDERS', durable: 'billing' })],
    ['@ConsumeOrdered', () => ConsumeOrdered({ stream: 'ORDERS' })],
    ['@ServiceEndpoint', () => ServiceEndpoint('get')],
  ];

  /** Applies `decorators` to `Orders.prototype.on`, the last one first (as TypeScript does). */
  function decorateOrdersOn(decorators: MethodDecorator[]): void {
    class Orders {
      on() {}
    }
    const descriptor = Object.getOwnPropertyDescriptor(Orders.prototype, 'on');
    for (const decorate of [...decorators].reverse()) {
      decorate(Orders.prototype, 'on', descriptor as PropertyDescriptor);
    }
  }

  it.each(methodDecorators)('rejects %s twice on one method', (name, decorator) => {
    const apply = () => decorateOrdersOn([decorator(), decorator()]);
    expect(apply).toThrow(NatsConfigError);
    expect(apply).toThrow(`${name} is applied more than once to Orders.on; ${TAIL}`);
  });

  it('rejects two @Subscribe written on one method, even with different subjects', () => {
    expect(() => {
      class Orders {
        @Subscribe('orders.created')
        @Subscribe('orders.updated')
        on() {}
      }
      return Orders;
    }).toThrow(`@Subscribe is applied more than once to Orders.on; ${TAIL}`);
  });

  it('names the class of a static method', () => {
    expect(() => {
      // biome-ignore lint/complexity/noStaticOnlyClass: the decorated method must be static
      class Jobs {
        @Subscribe('a')
        @Subscribe('b')
        static run() {}
      }
      return Jobs;
    }).toThrow(`@Subscribe is applied more than once to Jobs.run; ${TAIL}`);
  });

  it('accepts handler decorators of different kinds on one method', () => {
    expect(() =>
      decorateOrdersOn(methodDecorators.map(([, decorator]) => decorator())),
    ).not.toThrow();
  });

  it('accepts a decorated override of a decorated method', () => {
    class Base {
      @Subscribe('a')
      on() {}
    }
    class Child extends Base {
      @Subscribe('b')
      override on() {}
    }
    expect(Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, Base.prototype.on)).toMatchObject({
      subject: 'a',
    });
    expect(Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, Child.prototype.on)).toMatchObject({
      subject: 'b',
    });
  });

  it('rejects @NatsService twice on one class', () => {
    const apply = () => {
      @NatsService({ name: 'a', version: '1.0.0' })
      @NatsService({ name: 'b', version: '1.0.0' })
      class Billing {}
      return Billing;
    };
    expect(apply).toThrow(NatsConfigError);
    expect(apply).toThrow(
      '@NatsService is applied more than once to Billing; each class declares one service',
    );
  });

  it('accepts @NatsService on a subclass of a service', () => {
    @NatsService({ name: 'base', version: '1.0.0' })
    class Base {}
    @NatsService({ name: 'child', version: '2.0.0' })
    class Child extends Base {}
    expect(Reflect.getMetadata(NATS_SERVICE_METADATA, Base)).toMatchObject({ name: 'base' });
    expect(Reflect.getMetadata(NATS_SERVICE_METADATA, Child)).toMatchObject({ name: 'child' });
  });
});
