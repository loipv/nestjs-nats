import { NoRespondersError } from '@nats-io/transport-node';

/** The message of an error, or the value as a string when it is not an `Error`. */
export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** The stack of an error (falling back to its message), for logging. */
export function errorTrace(err: unknown): string {
  return (err instanceof Error ? err.stack : undefined) ?? errorMessage(err);
}

/**
 * Whether `err` (or one of its causes) says nobody answered the request. Also matches by
 * name, for errors thrown by another copy of the NATS library.
 */
export function isNoResponders(err: unknown): boolean {
  for (let e = err, depth = 0; e instanceof Error && depth < 5; e = e.cause, depth++) {
    if (e instanceof NoRespondersError || e.name === 'NoRespondersError') return true;
  }
  return false;
}
