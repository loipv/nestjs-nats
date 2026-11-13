import 'reflect-metadata';
import { describe, expect, it } from 'vitest';
import { CloudEventRouter, OnCloudEvent } from '../../../src/decorators/cloudevent.decorators.js';
import {
  NATS_CLOUD_EVENT_ROUTER_METADATA,
  NATS_ON_CLOUD_EVENT_METADATA,
} from '../../../src/decorators/constants.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

describe('@CloudEventRouter', () => {
  it('stores its options on the class', () => {
    @CloudEventRouter({ subscribe: 'events', unmatched: 'term' })
    class Events {}
    expect(Reflect.getMetadata(NATS_CLOUD_EVENT_ROUTER_METADATA, Events)).toEqual({
      subscribe: 'events',
      unmatched: 'term',
    });
  });

  it('rejects being applied twice to one class', () => {
    const apply = () => {
      @CloudEventRouter({ subscribe: 'a' })
      @CloudEventRouter({ subscribe: 'b' })
      class Events {}
      return Events;
    };
    expect(apply).toThrow(NatsConfigError);
    expect(apply).toThrow(
      '@CloudEventRouter is applied more than once to Events; each class declares one router',
    );
  });

  it('stores a startup function itself', () => {
    const options = () => ({ subscribe: 'events' });
    @CloudEventRouter(options)
    class Events {}
    expect(Reflect.getMetadata(NATS_CLOUD_EVENT_ROUTER_METADATA, Events)).toBe(options);
  });

  it('rejects a startup function applied twice to one class', () => {
    const options = () => ({ subscribe: 'events' });
    expect(() => {
      @CloudEventRouter(options)
      @CloudEventRouter(options)
      class Events {}
      return Events;
    }).toThrow(
      '@CloudEventRouter is applied more than once to Events; each class declares one router',
    );
  });

  it('accepts a router on a subclass of a router', () => {
    @CloudEventRouter({ subscribe: 'a' })
    class Base {}
    @CloudEventRouter({ subscribe: 'b' })
    class Child extends Base {}
    expect(Reflect.getMetadata(NATS_CLOUD_EVENT_ROUTER_METADATA, Base)).toEqual({
      subscribe: 'a',
    });
    expect(Reflect.getMetadata(NATS_CLOUD_EVENT_ROUTER_METADATA, Child)).toEqual({
      subscribe: 'b',
    });
  });
});

describe('@OnCloudEvent', () => {
  it('collects every pattern applied to one method (decorators apply bottom-up)', () => {
    class Events {
      @OnCloudEvent('order.created')
      @OnCloudEvent('order.*')
      on() {}
    }
    expect(Reflect.getMetadata(NATS_ON_CLOUD_EVENT_METADATA, Events.prototype.on)).toEqual([
      'order.*',
      'order.created',
    ]);
  });
});
