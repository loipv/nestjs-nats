import { Logger, type LoggerService } from '@nestjs/common';

const noopLogger: LoggerService = {
  log() {},
  error() {},
  warn() {},
  debug() {},
  verbose() {},
};

export function createNatsLogger(
  option: false | LoggerService | undefined,
  context = 'NatsModule',
): LoggerService {
  if (option === false) return noopLogger;
  if (option === undefined) return new Logger(context);
  return option;
}
