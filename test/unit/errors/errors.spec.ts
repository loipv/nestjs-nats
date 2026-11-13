import { describe, expect, it } from 'vitest';
import {
  NatsAtomicPublishError,
  NatsConfigError,
  NatsConnectionError,
  NatsDecodeError,
  NatsHandlerTimeoutError,
  NatsModuleError,
  NatsRequestError,
  NonRetryableError,
  StreamConfigConflictError,
} from '../../../src/errors/errors.js';

describe('errors', () => {
  it('sets name and keeps cause', () => {
    const e = new NatsDecodeError('x', { cause: 'c' });
    expect(e).toBeInstanceOf(NatsModuleError);
    expect(e).toBeInstanceOf(Error);
    expect(e.name).toBe('NatsDecodeError');
    expect(e.message).toBe('x');
    expect(e.cause).toBe('c');
  });

  it('describes a handler that outlived its timeout', () => {
    const e = new NatsHandlerTimeoutError('Orders.on', 5000);
    expect(e).toBeInstanceOf(NatsModuleError);
    expect(e.name).toBe('NatsHandlerTimeoutError');
    expect(e.message).toBe('Handler Orders.on did not finish within 5000 ms');
    expect(e.handler).toBe('Orders.on');
    expect(e.timeout).toBe(5000);
  });

  it('uses the class name for simple subclasses', () => {
    expect(new NatsModuleError('m').name).toBe('NatsModuleError');
    expect(new NatsConfigError('m').name).toBe('NatsConfigError');
    expect(new NonRetryableError('m').name).toBe('NonRetryableError');
  });

  it('carries connection details', () => {
    const e = new NatsConnectionError('down', {
      connection: 'default',
      servers: ['nats://h:4222'],
    });
    expect(e).toBeInstanceOf(NatsModuleError);
    expect(e.name).toBe('NatsConnectionError');
    expect(e.connection).toBe('default');
    expect(e.servers).toEqual(['nats://h:4222']);
  });

  it('carries atomic publish details', () => {
    const e = new NatsAtomicPublishError('failed', { subject: 'o.a', count: 3, cause: 'c' });
    expect(e).toBeInstanceOf(NatsModuleError);
    expect(e.name).toBe('NatsAtomicPublishError');
    expect(e.subject).toBe('o.a');
    expect(e.count).toBe(3);
    expect(e.cause).toBe('c');
  });

  it('carries stream config conflict details', () => {
    const e = new StreamConfigConflictError('conflict', {
      stream: 'ORDERS',
      consumer: 'c1',
      field: 'max_age',
      current: 1,
      declared: 2,
    });
    expect(e.name).toBe('StreamConfigConflictError');
    expect(e.stream).toBe('ORDERS');
    expect(e.consumer).toBe('c1');
    expect(e.field).toBe('max_age');
    expect(e.current).toBe(1);
    expect(e.declared).toBe(2);
  });

  it('carries request error details', () => {
    const e = new NatsRequestError('failed', {
      code: 'service_error',
      subject: 'svc.do',
      serviceCode: 500,
      description: 'boom',
      cause: 'c',
    });
    expect(e.name).toBe('NatsRequestError');
    expect(e.code).toBe('service_error');
    expect(e.subject).toBe('svc.do');
    expect(e.serviceCode).toBe(500);
    expect(e.description).toBe('boom');
    expect(e.cause).toBe('c');
  });
});
