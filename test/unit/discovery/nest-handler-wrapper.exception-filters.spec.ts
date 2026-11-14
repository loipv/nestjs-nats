import { headers as natsHeaders } from '@nats-io/transport-node';
import {
  type ArgumentsHost,
  Catch,
  type ExceptionFilter,
  Injectable,
  Logger,
  UseFilters,
} from '@nestjs/common';
import { APP_FILTER, ExternalContextCreator } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsContext } from '../../../src/context/nats-context.js';
import type { Invoke } from '../../../src/discovery/handler-definition.js';
import { wrapHandler } from '../../../src/discovery/nest-handler-wrapper.js';

class MineError extends Error {}
class GlobalError extends Error {}

let caughtHost: ArgumentsHost | undefined;

@Catch(MineError)
class MineFilter implements ExceptionFilter {
  catch(_: MineError, host: ArgumentsHost): unknown {
    caughtHost = host;
    return 'mine';
  }
}

@Catch(MineError)
class ClassFilter implements ExceptionFilter {
  catch(): unknown {
    return 'class';
  }
}

@Catch()
class SilentFilter implements ExceptionFilter {
  catch(): void {}
}

@Catch(GlobalError)
class GlobalFilter implements ExceptionFilter {
  catch(): unknown {
    return 'global';
  }
}

@Injectable()
class Handlers {
  plain(): never {
    throw new Error('boom');
  }

  @UseFilters(MineFilter)
  mine(): never {
    throw new MineError('mine');
  }

  @UseFilters(MineFilter)
  other(): never {
    throw new Error('other');
  }

  @UseFilters(SilentFilter)
  silent(): never {
    throw new Error('silent');
  }

  global(): never {
    throw new GlobalError('global');
  }
}

@UseFilters(ClassFilter)
@Injectable()
class ClassFiltered {
  fail(): never {
    throw new MineError('class');
  }
}

function natsCtx(): NatsContext {
  return new NatsContext({
    kind: 'core',
    connection: 'default',
    raw: { subject: 's', reply: '', headers: natsHeaders() } as never,
  });
}

describe('wrapHandler exception filters', () => {
  let moduleRef: TestingModule;
  const errorLog = vi.spyOn(Logger.prototype, 'error');
  /** `Handlers.<method>`, or `ClassFiltered.fail` for `'fail'`, wrapped. */
  const find = (method: string): { invoke: Invoke } => {
    const instance = method === 'fail' ? moduleRef.get(ClassFiltered) : moduleRef.get(Handlers);
    return { invoke: wrapHandler(moduleRef.get(ExternalContextCreator), instance, method) };
  };

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      providers: [Handlers, ClassFiltered, { provide: APP_FILTER, useClass: GlobalFilter }],
    }).compile();
  });

  afterEach(() => {
    errorLog.mockClear();
    caughtHost = undefined;
  });

  afterAll(async () => {
    errorLog.mockRestore();
    await moduleRef.close();
  });

  it("rethrows an unhandled error without Nest's ExceptionsHandler logging it", async () => {
    const err = await find('plain')
      .invoke(undefined, natsCtx())
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toBe('boom');
    expect(errorLog).not.toHaveBeenCalled();
  });

  it('runs a method filter and returns what it returns, with a nats host', async () => {
    const ctx = natsCtx();
    await expect(find('mine').invoke({ p: 1 }, ctx)).resolves.toBe('mine');
    expect(caughtHost?.getType()).toBe('nats');
    expect(caughtHost?.getArgs()).toEqual([{ p: 1 }, ctx]);
    expect(caughtHost?.getArgByIndex(1)).toBe(ctx);
    expect(caughtHost?.switchToRpc().getData()).toEqual({ p: 1 });
    expect(caughtHost?.switchToRpc().getContext()).toBe(ctx);
  });

  it('rethrows without logging an error the filters do not catch', async () => {
    await expect(find('other').invoke(undefined, natsCtx())).rejects.toThrow('other');
    expect(caughtHost).toBeUndefined();
    expect(errorLog).not.toHaveBeenCalled();
  });

  it('treats a filter returning nothing as not handling the error, like Nest', async () => {
    await expect(find('silent').invoke(undefined, natsCtx())).rejects.toThrow('silent');
    expect(errorLog).not.toHaveBeenCalled();
  });

  it('runs class filters', async () => {
    await expect(find('fail').invoke(undefined, natsCtx())).resolves.toBe('class');
  });

  it('runs global filters', async () => {
    await expect(find('global').invoke(undefined, natsCtx())).resolves.toBe('global');
  });
});

describe('wrapHandler without access to Nest exception filters', () => {
  it("falls back to Nest's own exception handling", async () => {
    const target = () => Promise.resolve('done');
    const ecc = { create: vi.fn(() => target) };
    const invoke = wrapHandler(ecc as never, new Handlers(), 'plain');
    expect(invoke).toBe(target);
    expect(ecc.create).toHaveBeenCalledOnce();
    expect((ecc.create.mock.calls[0] as unknown[])[7]).toEqual({
      guards: true,
      interceptors: true,
      filters: true,
    });
  });
});
