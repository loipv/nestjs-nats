import { type Mock, vi } from 'vitest';

/** A `LoggerService` whose methods are `vi.fn()` mocks. */
export type MockLogger = Record<'log' | 'error' | 'warn' | 'debug' | 'verbose', Mock>;

export function mockLogger(): MockLogger {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}
