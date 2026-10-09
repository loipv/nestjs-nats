export const DEFAULT_CONNECTION = 'default';

export const NATS_FEATURE_TOKEN_PREFIX = 'NATS_FEATURE:';

export const getConnectionToken = (name: string = DEFAULT_CONNECTION): string =>
  `NATS_CONNECTION:${name}`;

export const getNatsClientToken = (name: string = DEFAULT_CONNECTION): string =>
  `NATS_CLIENT:${name}`;

export const getJetStreamClientToken = (name: string = DEFAULT_CONNECTION): string =>
  `NATS_JETSTREAM:${name}`;

export const getStreamManagerToken = (name: string = DEFAULT_CONNECTION): string =>
  `NATS_STREAM_MANAGER:${name}`;

export const getModuleOptionsToken = (name: string = DEFAULT_CONNECTION): string =>
  `NATS_OPTIONS:${name}`;
