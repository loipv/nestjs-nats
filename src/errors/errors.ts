export class NatsModuleError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

export class NatsConfigError extends NatsModuleError {}

export class NatsConnectionError extends NatsModuleError {
  readonly connection: string;
  readonly servers: string[];

  constructor(
    message: string,
    details: { connection: string; servers: string[]; cause?: unknown },
  ) {
    super(message, { cause: details.cause });
    this.connection = details.connection;
    this.servers = details.servers;
  }
}

export class StreamConfigConflictError extends NatsModuleError {
  readonly stream: string;
  readonly consumer?: string;
  readonly field: string;
  readonly current: unknown;
  readonly declared: unknown;

  constructor(
    message: string,
    details: {
      stream: string;
      consumer?: string;
      field: string;
      current: unknown;
      declared: unknown;
      cause?: unknown;
    },
  ) {
    super(message, { cause: details.cause });
    this.stream = details.stream;
    this.consumer = details.consumer;
    this.field = details.field;
    this.current = details.current;
    this.declared = details.declared;
  }
}

export class NatsDecodeError extends NatsModuleError {}

export type NatsRequestErrorCode = 'timeout' | 'no_responders' | 'service_error';

export class NatsRequestError extends NatsModuleError {
  readonly code: NatsRequestErrorCode;
  readonly subject: string;
  /** The `Nats-Service-Error-Code` header; unset when it is not an integer. */
  readonly serviceCode?: number;
  readonly description?: string;

  constructor(
    message: string,
    details: {
      code: NatsRequestErrorCode;
      subject: string;
      serviceCode?: number;
      description?: string;
      cause?: unknown;
    },
  ) {
    super(message, { cause: details.cause });
    this.code = details.code;
    this.subject = details.subject;
    this.serviceCode = details.serviceCode;
    this.description = details.description;
  }
}

export class NonRetryableError extends NatsModuleError {}
