import { NatsModuleError } from '../errors/errors.js';

/**
 * Internal: a `@CloudEventRouter` received a CloudEvent whose type no `@OnCloudEvent`
 * matches, and its `unmatched` policy is not `'ack'` or the message is a request.
 * Settlement applies the policy instead of the retry/DLQ decision; a request gets a 404
 * error reply, whatever the policy.
 */
export class UnmatchedCloudEventError extends NatsModuleError {
  readonly type: string;
  readonly policy: 'ack' | 'term' | 'dlq';

  constructor(type: string, policy: 'ack' | 'term' | 'dlq') {
    super(`No handler for CloudEvent type "${type}"`);
    this.type = type;
    this.policy = policy;
  }
}
