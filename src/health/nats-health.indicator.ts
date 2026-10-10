import { Inject, Injectable } from '@nestjs/common';
import { type HealthIndicatorResult, HealthIndicatorService } from '@nestjs/terminus';
import { ConnectionRegistry } from '../connection/connection-registry.js';
import { redactServers, redactText } from '../connection/redact.js';
import { NatsConfigError } from '../errors/errors.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { errorMessage } from '../util/errors.js';
import { settlesWithin } from '../util/timeout.js';

export interface NatsHealthCheckOptions {
  /** The connection to check (default `'default'`). */
  connection?: string;
  /**
   * Longest wait, in ms, for the round trip (and the JetStream check) (default 1000). A
   * finite number in (0, 2^31 - 1]; anything else rejects with a `NatsConfigError`.
   */
  rttTimeout?: number;
  /** Also require JetStream to answer an account info request (default `false`). */
  jetstream?: boolean;
}

/** The largest delay `setTimeout` honours; above it Node fires after 1 ms. */
const MAX_TIMEOUT_MS = 2 ** 31 - 1;

type Bounded<T> =
  | { kind: 'value'; value: T }
  | { kind: 'error'; error: unknown }
  | { kind: 'timeout' };

/** Runs `fn`, waiting at most `ms`; never rejects and never throws synchronously. */
async function bounded<T>(fn: () => Promise<T>, ms: number): Promise<Bounded<T>> {
  const run = (async (): Promise<Bounded<T>> => {
    try {
      return { kind: 'value', value: await fn() };
    } catch (error) {
      return { kind: 'error', error };
    }
  })();
  return (await settlesWithin(run, ms)) ? run : { kind: 'timeout' };
}

/** A Terminus health indicator for a NATS connection. */
@Injectable()
export class NatsHealthIndicator {
  // Injected as properties, so the internal registry stays out of the constructor's type.
  @Inject(HealthIndicatorService) private readonly health!: HealthIndicatorService;
  @Inject(ConnectionRegistry) private readonly registry!: ConnectionRegistry;

  async isHealthy<K extends string>(
    key: K,
    options: NatsHealthCheckOptions = {},
  ): Promise<HealthIndicatorResult<K>> {
    const timeout = options.rttTimeout ?? 1000;
    // A misconfigured probe, not an unhealthy connection: fail loudly instead of `down`.
    if (!(Number.isFinite(timeout) && timeout > 0 && timeout <= MAX_TIMEOUT_MS)) {
      throw new NatsConfigError(
        `NATS health check rttTimeout must be a finite number of ms in (0, ${MAX_TIMEOUT_MS}], got ${timeout}`,
      );
    }
    const session = this.health.check(key);
    const connection = options.connection ?? DEFAULT_CONNECTION;
    const down = (reason: string): HealthIndicatorResult<K> =>
      session.down({ connection, reason }) as HealthIndicatorResult<K>;
    let secrets: Array<string | undefined> = [];
    try {
      if (!this.registry.has(connection)) return down('unknown connection');
      const status = this.registry.status(connection);
      if (status.state !== 'connected') return down(status.state);

      const entry = this.registry.get(connection);
      const { pass, token } = entry.options.connection;
      secrets = [pass, token];

      const rtt = await bounded(() => entry.nc.rtt(), timeout);
      if (rtt.kind === 'timeout') return down(`rtt timeout after ${timeout}ms`);
      if (rtt.kind === 'error')
        return down(`rtt failed: ${redactText(errorMessage(rtt.error), secrets)}`);

      if (options.jetstream) {
        const js = await bounded(async () => (await entry.jsm()).getAccountInfo(), timeout);
        if (js.kind === 'timeout') return down(`jetstream unavailable: timeout after ${timeout}ms`);
        if (js.kind === 'error') {
          return down(`jetstream unavailable: ${redactText(errorMessage(js.error), secrets)}`);
        }
      }

      return session.up({
        connection,
        ...(status.server !== undefined ? { server: redactServers(status.server)[0] } : {}),
        rtt: Math.round(rtt.value),
      }) as HealthIndicatorResult<K>;
    } catch (err) {
      return down(`check failed: ${redactText(errorMessage(err), secrets)}`);
    }
  }
}
