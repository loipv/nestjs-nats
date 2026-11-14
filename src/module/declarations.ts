import { NatsConfigError } from '../errors/errors.js';
import type { NatsModuleOptions } from './options.js';

/**
 * Throws a `NatsConfigError` when a connection still sets `jetstream.removedFields` to
 * anything but `'ignore'`: `'reset'` was removed in 0.3.0, and silently ignoring it would
 * change what sync does to the application's streams and consumers.
 */
export function rejectRemovedFieldsReset(connection: string, options: NatsModuleOptions): void {
  const jetstream = options.jetstream as { removedFields?: unknown } | undefined;
  const mode = jetstream?.removedFields;
  if (mode === undefined || mode === 'ignore') return;
  throw new NatsConfigError(
    `NATS connection "${connection}" sets jetstream.removedFields to ${JSON.stringify(mode)}, which was removed in 0.3.0: a field removed from a declaration keeps its value on the server. Remove the option, and change such fields by hand`,
  );
}
