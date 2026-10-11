import type { RestartPolicy } from '../settlement/restart.js';

/** Runtime binding between a discovered handler and its NATS message source. */
export interface HandlerAdapter {
  /** Starts receiving messages; resolves once the subscription is registered with the server. */
  start(): Promise<void>;
  /** Stops receiving new messages. Messages already received are still processed. */
  stop(): Promise<void>;
  /** Resolves when no message is being processed. */
  idle(): Promise<void>;
  /**
   * Gives up on work still pending at the end of shutdown: naks JetStream messages that
   * are being processed but not yet settled and cancels pending in-process retries.
   */
  forceStop(): ForceStopResult;
}

export interface ForceStopResult {
  /** Messages nak'd because they were still being processed. */
  nakked: number;
  /** Pending in-process retries that were cancelled. */
  cancelledRetries: number;
}

export type HandlerState = 'running' | 'restarting' | 'failed';

/** How an adapter restarts its intake; given by the lifecycle. */
export interface RestartBinding {
  policy: RestartPolicy | false;
  /** Registers the adapter's handler under `label`; returns its state setter. */
  track(label: string): (state: HandlerState) => void;
}

export const NO_RESTART: RestartBinding = { policy: false, track: () => () => {} };
