import { Logger } from '@nestjs/common';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createNatsLogger } from '../../../src/module/logger.js';
import { mockLogger } from '../../support/logger.js';

const contextOf = (logger: unknown) => (logger as { context?: string }).context;

describe('createNatsLogger', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns a logger that writes nothing for `false`', () => {
    const stdout = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    const stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    const logger = createNatsLogger(false);
    expect(logger).not.toBeInstanceOf(Logger);
    for (const level of ['log', 'error', 'warn', 'debug', 'verbose'] as const) {
      logger[level]?.('message', 'context');
    }
    expect(stdout).not.toHaveBeenCalled();
    expect(stderr).not.toHaveBeenCalled();
  });

  it('returns a Nest Logger with the given context when none is given', () => {
    const logger = createNatsLogger(undefined, 'NatsConnection:orders');
    expect(logger).toBeInstanceOf(Logger);
    expect(contextOf(logger)).toBe('NatsConnection:orders');
  });

  it("names the default logger's context NatsModule", () => {
    expect(contextOf(createNatsLogger(undefined))).toBe('NatsModule');
  });

  it('returns a given logger as it is', () => {
    const mine = mockLogger();
    expect(createNatsLogger(mine, 'ignored')).toBe(mine);
  });
});
