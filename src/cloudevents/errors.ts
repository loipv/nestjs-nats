import { NatsModuleError } from '../errors/errors.js';

/**
 * Internal: a `@CloudEventRouter` received a CloudEvent whose type no `@OnCloudEvent`
 * matches, and its `unmatched` policy is not `'ack'`. Settlement applies the policy
 * instead of the retry/DLQ decision; a request gets a 404 error reply.
 */
export class UnmatchedCloudEventError extends NatsModuleError {
  readonly type: string;
  readonly policy: 'term' | 'dlq';

  constructor(type: string, policy: 'term' | 'dlq') {
    super(`No handler for CloudEvent type "${type}"`);
    this.type = type;
    this.policy = policy;
  }
}
