import type { LoggerService } from '@nestjs/common';
import type { RestartPolicy } from '../settlement/restart.js';
import { backoffDelay } from '../settlement/retry.js';
import { errorMessage } from '../util/errors.js';
import type { HandlerState } from './adapter.js';

export interface RestartSupervisorDeps {
  policy: RestartPolicy | false;
  label: string;
  logger: LoggerService;
  isConnectionClosed: () => boolean;
  /** Opens the intake again; rejects when it cannot. */
  reopen: () => Promise<void>;
  onState: (state: HandlerState) => void;
  random?: () => number;
}

/**
 * Restarts a handler's intake with backoff after it ended. The adapter calls
 * `started()` once the intake runs and `ended()` when it stopped on its own.
 */
export class RestartSupervisor {
  private current: HandlerState | undefined;
  private attempts = 0;
  private stopped = false;
  private restartTimer: NodeJS.Timeout | undefined;
  private stableTimer: NodeJS.Timeout | undefined;
  private inFlight: Promise<void> | undefined;
  /** A reopen was called and has not been handled yet. */
  private reopening = false;
  /** The first intake ended before `started()`. */
  private endedBeforeStart = false;
  /** The intake ended while `reopening`. */
  private endedWhileReopening = false;

  constructor(private readonly deps: RestartSupervisorDeps) {}

  /** `undefined` until `started()`. */
  get state(): HandlerState | undefined {
    return this.current;
  }

  /** Whether `ended()` would schedule a restart now. */
  get restartable(): boolean {
    const { policy } = this.deps;
    return (
      !this.stopped &&
      policy !== false &&
      !this.deps.isConnectionClosed() &&
      this.attempts < policy.maxAttempts
    );
  }

  started(): void {
    this.setState('running');
    if (this.endedBeforeStart) {
      this.endedBeforeStart = false;
      this.ended();
    }
  }

  /**
   * The intake stopped on its own. Adapters must call this only for their current intake:
   * an old intake finishing late during a reopen would be charged to the new one.
   */
  ended(_err?: unknown): void {
    if (this.stopped) return;
    if (this.current === undefined) {
      // The first open is still in progress: handled once `started()` is called.
      this.endedBeforeStart = true;
      return;
    }
    if (this.reopening) {
      // The reopened intake died before it was marked running: the reopen then counts as failed.
      this.endedWhileReopening = true;
      return;
    }
    if (this.current !== 'running') return;
    clearTimeout(this.stableTimer);
    if (this.deps.policy === false || this.deps.isConnectionClosed()) {
      // The adapter's own error line already says no more messages will be received.
      this.setState('failed');
      return;
    }
    // Gives up, with a log line, when the attempts are used up.
    this.schedule();
  }

  stop(): void {
    this.stopped = true;
    clearTimeout(this.restartTimer);
    clearTimeout(this.stableTimer);
  }

  /** Resolves when no reopen is in flight; never rejects. */
  async settled(): Promise<void> {
    await this.inFlight;
  }

  private setState(state: HandlerState): void {
    this.current = state;
    this.deps.onState(state);
  }

  private fail(message: string): void {
    this.setState('failed');
    this.deps.logger.error(message);
  }

  private schedule(): void {
    const { policy, label, logger, random } = this.deps;
    if (policy === false) return;
    if (this.attempts >= policy.maxAttempts) {
      this.fail(
        `Gave up restarting ${label} after ${this.attempts} attempts; no more messages will be received`,
      );
      return;
    }
    this.attempts++;
    this.setState('restarting');
    const delay = backoffDelay(policy, this.attempts, random);
    const limit = Number.isFinite(policy.maxAttempts) ? `/${policy.maxAttempts}` : '';
    logger.warn(`Restarting ${label} in ${delay} ms (attempt ${this.attempts}${limit})`);
    this.restartTimer = setTimeout(() => this.run(), delay);
    this.restartTimer.unref();
  }

  private run(): void {
    const { policy, label, logger } = this.deps;
    if (this.stopped || policy === false) return;
    if (this.deps.isConnectionClosed()) {
      this.fail(`Gave up restarting ${label}: the connection is closed`);
      return;
    }
    this.reopening = true;
    this.endedWhileReopening = false;
    this.inFlight = Promise.resolve()
      .then(() => this.deps.reopen())
      .then(
        () => {
          this.reopening = false;
          if (this.stopped) return;
          if (this.endedWhileReopening) {
            logger.warn(
              `Restart of ${label} failed (attempt ${this.attempts}): the intake ended while restarting`,
            );
            this.schedule();
            return;
          }
          this.setState('running');
          logger.log(`Restarted ${label} (attempt ${this.attempts})`);
          this.stableTimer = setTimeout(() => {
            this.attempts = 0;
          }, policy.maxDelay);
          this.stableTimer.unref();
        },
        (err: unknown) => {
          this.reopening = false;
          if (this.stopped) return;
          logger.warn(
            `Restart of ${label} failed (attempt ${this.attempts}): ${errorMessage(err)}`,
          );
          this.schedule();
        },
      );
  }
}
