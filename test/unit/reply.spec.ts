import { InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { NatsContext } from '../../src/context/nats-context.js';
import { NatsDecodeError } from '../../src/errors/errors.js';
import type { Delivery } from '../../src/pipeline/delivery.js';
import { isReplyDelivery, toServiceError } from '../../src/pipeline/reply.js';

describe('toServiceError', () => {
  it('maps HttpException', () =>
    expect(toServiceError(new NotFoundException('gone'))).toEqual({
      code: 404,
      description: 'gone',
    }));

  it('maps 5xx HttpException with its own message', () =>
    expect(toServiceError(new InternalServerErrorException('boom'))).toEqual({
      code: 500,
      description: 'boom',
    }));

  it('maps decode errors to 400', () =>
    expect(toServiceError(new NatsDecodeError('x'))).toEqual({
      code: 400,
      description: 'Bad Request',
    }));

  it('hides internal errors', () =>
    expect(toServiceError(new Error('db password=…'))).toEqual({
      code: 500,
      description: 'Internal Server Error',
    }));

  it('hides non-Error throwables', () =>
    expect(toServiceError('nope')).toEqual({ code: 500, description: 'Internal Server Error' }));
});

describe('isReplyDelivery', () => {
  const delivery = (kind: string, reply: string) =>
    ({
      handler: { kind },
      context: new NatsContext({
        kind: 'core',
        connection: 'default',
        raw: { subject: 's', reply } as never,
      }),
    }) as unknown as Delivery;

  it('is true for service endpoints', () =>
    expect(isReplyDelivery(delivery('service-endpoint', ''))).toBe(true));
  it('is true for subscriptions with a reply subject', () =>
    expect(isReplyDelivery(delivery('subscribe', '_INBOX.1'))).toBe(true));
  it('is false for subscriptions without a reply subject', () =>
    expect(isReplyDelivery(delivery('subscribe', ''))).toBe(false));
  it('is false for consumers', () =>
    expect(isReplyDelivery(delivery('consume', '$JS.ACK.x'))).toBe(false));
});
