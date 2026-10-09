/** Runtime binding between a discovered handler and its NATS message source. */
export interface HandlerAdapter {
  /** Starts receiving messages; resolves once the subscription is registered with the server. */
  start(): Promise<void>;
  /** Stops receiving new messages. Messages already received are still processed. */
  stop(): Promise<void>;
  /** Resolves when no message is being processed. */
  idle(): Promise<void>;
}
