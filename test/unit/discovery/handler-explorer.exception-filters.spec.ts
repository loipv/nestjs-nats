import { headers as natsHeaders } from '@nats-io/transport-node';
import {
  type ArgumentsHost,
  Catch,
  type ExceptionFilter,
  Injectable,
  Logger,
  UseFilters,
} from '@nestjs/common';
import { APP_FILTER, DiscoveryModule, DiscoveryService, MetadataScanner } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsContext } from '../../../src/context/nats-context.js';
import { Subscribe } from '../../../src/decorators/handler.decorators.js';
import {
  type HandlerDefinition,
  HandlerExplorer,
} from '../../../src/discovery/handler-explorer.js';

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
  @Subscribe('plain')
  plain(): never {
    throw new Error('boom');
  }

  @UseFilters(MineFilter)
  @Subscribe('mine')
  mine(): never {
    throw new MineError('mine');
  }

  @UseFilters(MineFilter)
  @Subscribe('other')
  other(): never {
    throw new Error('other');
  }

  @UseFilters(SilentFilter)
  @Subscribe('silent')
  silent(): never {
    throw new Error('silent');
  }

  @Subscribe('global')
  global(): never {
    throw new GlobalError('global');
  }
}

@UseFilters(ClassFilter)
@Injectable()
class ClassFiltered {
  @Subscribe('class')
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

describe('HandlerExplorer exception filters', () => {
  let moduleRef: TestingModule;
  let defs: HandlerDefinition[];
  const errorLog = vi.spyOn(Logger.prototype, 'error');
  const find = (method: string) => {
    const d = defs.find((x) => x.methodName === method);
    if (!d) throw new Error(`no handler ${method}`);
    return d;
  };

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [
        HandlerExplorer,
        Handlers,
        ClassFiltered,
        { provide: APP_FILTER, useClass: GlobalFilter },
      ],
    }).compile();
    defs = moduleRef.get(HandlerExplorer).explore();
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

describe('HandlerExplorer without access to Nest exception filters', () => {
  it("falls back to Nest's own exception handling", async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [Handlers],
    }).compile();
    const ecc = { create: vi.fn(() => () => Promise.resolve()) };
    new HandlerExplorer(
      moduleRef.get(DiscoveryService),
      moduleRef.get(MetadataScanner),
      ecc as never,
    ).explore();
    expect(ecc.create).toHaveBeenCalled();
    for (const call of ecc.create.mock.calls as unknown[][]) {
      expect(call[7]).toEqual({ guards: true, interceptors: true, filters: true });
    }
    await moduleRef.close();
  });
});
