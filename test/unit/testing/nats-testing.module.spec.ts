import { type INestApplicationContext, Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import { NatsClient } from '../../../src/client/nats-client.js';
import { Subscribe } from '../../../src/decorators/handler.decorators.js';
import { Payload } from '../../../src/decorators/param.decorators.js';
import { getNatsClientToken } from '../../../src/module/tokens.js';
import { NatsTestHarness, NatsTestingModule } from '../../../src/testing/index.js';

describe('NatsTestingModule', () => {
  it('fails dispatch on misconfigured handlers, like bootstrap', async () => {
    @Injectable()
    class Bad {
      @Subscribe('a', { concurrency: 0 })
      on() {}
    }
    const moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot()],
      providers: [Bad],
    }).compile();
    const h = NatsTestHarness.create(moduleRef);
    await expect(h.dispatch({ handler: 'Bad.on' }, 1)).rejects.toThrow(/invalid concurrency/);
    await moduleRef.close();
  });

  it('rejects a @Subscribe handler with batch, as bootstrap does', async () => {
    @Injectable()
    class Bad {
      @Subscribe('a', { batch: { maxSize: 2 } } as never)
      on() {}
    }
    const moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot()],
      providers: [Bad],
    }).compile();
    const h = NatsTestHarness.create(moduleRef);
    await expect(h.dispatch({ handler: 'Bad.on' }, [1, 2], { request: true })).rejects.toThrow(
      /sets batch on @Subscribe, which was removed in 0\.3\.0/,
    );
    await moduleRef.close();
  });

  it('does not dispatch to a disabled handler, as bootstrap would not start it', async () => {
    @Injectable()
    class Toggled {
      @Subscribe('off', { disabled: () => true })
      off() {}

      @Subscribe('on', { disabled: false })
      on(@Payload() p: number) {
        return p + 1;
      }
    }
    const moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot({ logger: false })],
      providers: [Toggled],
    }).compile();
    const h = NatsTestHarness.create(moduleRef);
    await expect(h.dispatch({ handler: 'Toggled.off' }, 1)).rejects.toThrow(
      'NATS handler Toggled.off is disabled',
    );
    await expect(h.dispatch({ subject: 'off' }, 1)).rejects.toThrow(/No NATS handler matches/);
    const r = await h.dispatch({ handler: 'Toggled.on' }, 1, { request: true });
    expect(r.reply).toEqual({ data: 2 });
    await moduleRef.close();
  });

  it('registers named connections without class aliases', async () => {
    @Injectable()
    class Other {
      @Subscribe('o', { connection: 'other' })
      on(@Payload() p: number) {
        return p + 1;
      }
    }
    const moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot(), NatsTestingModule.forRoot({ name: 'other' })],
      providers: [Other],
    }).compile();
    const app: INestApplicationContext = moduleRef;
    expect(app.get(getNatsClientToken('other'))).toBeInstanceOf(NatsClient);
    expect(app.get(getNatsClientToken('other'))).not.toBe(app.get(NatsClient));
    const h = NatsTestHarness.create(app);
    const r = await h.dispatch({ handler: 'Other.on' }, 1, { request: true });
    expect(r.reply).toEqual({ data: 2 });
    await moduleRef.close();
  });
});
