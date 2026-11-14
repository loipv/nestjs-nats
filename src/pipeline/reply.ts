import { HttpException } from '@nestjs/common';
import { UnmatchedCloudEventError } from '../cloudevents/errors.js';
import { NatsDecodeError, NatsHandlerTimeoutError } from '../errors/errors.js';
import type { Delivery } from './delivery.js';

export interface ServiceError {
  code: number;
  description: string;
}

/** Maps a handler error to a service error without leaking internal error messages. */
export function toServiceError(err: unknown): ServiceError {
  if (err instanceof HttpException) return { code: err.getStatus(), description: err.message };
  if (err instanceof NatsDecodeError) return { code: 400, description: 'Bad Request' };
  if (err instanceof NatsHandlerTimeoutError)
    return { code: 504, description: 'Handler timed out' };
  if (err instanceof UnmatchedCloudEventError) return { code: 404, description: err.message };
  return { code: 500, description: 'Internal Server Error' };
}

/**
 * Whether the delivery expects a reply (service endpoint, or a request on a
 * subscription). A batch never replies.
 */
export function isReplyDelivery(d: Delivery): boolean {
  if (d.context.kind === 'batch') return false;
  return (
    d.handler.kind === 'service-endpoint' ||
    (d.handler.kind === 'subscribe' && d.context.replyTo !== undefined)
  );
}
