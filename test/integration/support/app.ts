import type { LoggerService, ModuleMetadata } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

/** A Nest logger that prints nothing. */
const silentLogger: LoggerService = { log() {}, error() {}, warn() {} };

/**
 * Compiles and initialises a module; closes it when init fails, so no connection leaks.
 * `silent` silences Nest's logger, for apps whose handlers fail on purpose (the library logs
 * their errors through it). Nest's logger is global: it stays silent, for every app,
 * until the next app is compiled, so use it only where every logged error is expected.
 */
export async function createTestApp(
  metadata: ModuleMetadata,
  opts: { silent?: boolean } = {},
): Promise<TestingModule> {
  const builder = Test.createTestingModule(metadata);
  if (opts.silent) builder.setLogger(silentLogger);
  const moduleRef = await builder.compile();
  try {
    await moduleRef.init();
  } catch (err) {
    await moduleRef.close().catch(() => {});
    throw err;
  }
  return moduleRef;
}
