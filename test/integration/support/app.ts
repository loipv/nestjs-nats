import type { ModuleMetadata } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';

/** Compiles and initialises a module; closes it when init fails, so no connection leaks. */
export async function createTestApp(metadata: ModuleMetadata): Promise<TestingModule> {
  const moduleRef = await Test.createTestingModule(metadata).compile();
  try {
    await moduleRef.init();
  } catch (err) {
    await moduleRef.close().catch(() => {});
    throw err;
  }
  return moduleRef;
}
