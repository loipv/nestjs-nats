import { Injectable } from '@nestjs/common';
import { type HealthIndicatorResult, HealthIndicatorService } from '@nestjs/terminus';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import { redactServers, redactText } from '../connection/redact.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { errorMessage } from '../util/errors.js';
import { settlesWithin } from '../util/timeout.js';

export interface NatsHealthCheckOptions {
  /** The connection to check (default `'default'`). */
  connection?: string;
  /** Longest wait, in ms, for the round trip (and the JetStream check) (default 1000). */
  rttTimeout?: number;
  /** Also require JetStream to answer an account info request (default `false`). */
  jetstream?: boolean;
}

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
  constructor(
    private readonly health: HealthIndicatorService,
    private readonly manager: NatsConnectionManager,
  ) {}

  async isHealthy<K extends string>(
    key: K,
    options: NatsHealthCheckOptions = {},
  ): Promise<HealthIndicatorResult<K>> {
    const session = this.health.check(key);
    const connection = options.connection ?? DEFAULT_CONNECTION;
    const down = (reason: string): HealthIndicatorResult<K> =>
      session.down({ connection, reason }) as HealthIndicatorResult<K>;
    let secrets: Array<string | undefined> = [];
    try {
      if (!this.manager.has(connection)) return down('unknown connection');
      const status = this.manager.status(connection);
      if (status.state !== 'connected') return down(status.state);

      const entry = this.manager.get(connection);
      const { pass, token } = entry.options.connection;
      secrets = [pass, token];
      const timeout = options.rttTimeout ?? 1000;

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
