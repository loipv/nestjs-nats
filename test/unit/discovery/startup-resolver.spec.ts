import 'reflect-metadata';
import { Injectable, Module, Scope } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { StartupContext } from '../../../src/decorators/startup-value.js';
import { StartupResolver } from '../../../src/discovery/startup-resolver.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

const SUBJECT = 'NATS handler H.on';

let moduleRef: TestingModule | undefined;

/** A resolver over an application with `providers` (and `imports`). */
async function resolverOf(
  providers: Parameters<typeof Test.createTestingModule>[0]['providers'] = [],
  imports: Parameters<typeof Test.createTestingModule>[0]['imports'] = [],
): Promise<StartupResolver> {
  moduleRef = await Test.createTestingModule({ imports, providers }).compile();
  return new StartupResolver(moduleRef.get(ModuleRef));
}

/** A resolver whose functions get no provider. */
const bare = () => new StartupResolver({ get: vi.fn() } as unknown as ModuleRef);

function errorOf(run: () => unknown): NatsConfigError {
  try {
    run();
  } catch (err) {
    expect(err).toBeInstanceOf(NatsConfigError);
    return err as NatsConfigError;
  }
  throw new Error('expected a NatsConfigError');
}

afterEach(async () => {
  await moduleRef?.close();
  moduleRef = undefined;
});

describe('StartupResolver.resolve', () => {
  it('returns a value as it is, and what a function returns', () => {
    const r = bare();
    expect(r.resolve('a.b', SUBJECT, '@Subscribe subject')).toBe('a.b');
    expect(r.resolve(() => 'c.d', SUBJECT, '@Subscribe subject')).toBe('c.d');
  });

  it('calls a function once and gives every caller its value, whatever names it', () => {
    const r = bare();
    const fn = vi.fn(() => ({ stream: 'S' }));
    const first = r.resolve(fn, SUBJECT, '@Consume options');
    expect(r.resolve(fn, 'NATS handler H.other', '@Consume options')).toBe(first);
    expect(fn).toHaveBeenCalledOnce();
  });

  it('runs a function again in another resolver (another application)', () => {
    const fn = vi.fn(() => 'x');
    bare().resolve(fn, SUBJECT, 'w');
    bare().resolve(fn, SUBJECT, 'w');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('throws the same error each time, without calling a failed function again', () => {
    const r = bare();
    const failing = vi.fn((): string => {
      throw new Error('no config');
    });
    const message = 'NATS handler H.on failed to resolve its @Subscribe subject: no config';
    expect(() => r.resolve(failing, SUBJECT, '@Subscribe subject')).toThrow(message);
    expect(() => r.resolve(failing, SUBJECT, '@Subscribe subject')).toThrow(message);
    expect(failing).toHaveBeenCalledOnce();
  });
});

describe('StartupResolver providers', () => {
  it('gets a provider declared in another module', async () => {
    @Injectable()
    class Settings {
      readonly subject = 'from.module';
    }
    @Module({ providers: [Settings], exports: [] })
    class SettingsModule {}
    const r = await resolverOf([], [SettingsModule]);
    expect(r.resolve(({ get }: StartupContext) => get(Settings).subject, SUBJECT, 'w')).toBe(
      'from.module',
    );
  });

  it('gets a provider by an abstract class token', async () => {
    abstract class Settings {
      abstract readonly subject: string;
    }
    const r = await resolverOf([{ provide: Settings, useValue: { subject: 'abstract.subject' } }]);
    expect(r.resolve(({ get }: StartupContext) => get(Settings).subject, SUBJECT, 'w')).toBe(
      'abstract.subject',
    );
  });

  it.each([
    ['a request-scoped provider', 'SCOPED'],
    ['a missing provider', 'MISSING'],
  ])('fails, naming the handler, when a function gets %s', async (_, token) => {
    @Injectable({ scope: Scope.REQUEST })
    class Scoped {}
    const r = await resolverOf([{ provide: 'SCOPED', useClass: Scoped }]);
    const err = errorOf(() =>
      r.resolveOptions(
        ({ get }: StartupContext) => ({ stream: get<string>(token) }),
        SUBJECT,
        '@Consume options',
      ),
    );
    expect(err.message).toMatch(/^NATS handler H\.on failed to resolve its @Consume options: /);
    expect(err.cause).toBeDefined();
  });
});

describe('StartupResolver.resolveString', () => {
  it.each([
    [42, '42'],
    ['', '""'],
    [undefined, 'undefined'],
    [null, 'null'],
  ])('rejects a function returning %j', (value, shown) => {
    const err = errorOf(() =>
      bare().resolveString(() => value as unknown as string, SUBJECT, '@Subscribe subject'),
    );
    expect(err.message).toBe(
      `NATS handler H.on: @Subscribe subject resolved to ${shown}; expected a non-empty string`,
    );
  });

  it('takes a written string as it is', () => {
    expect(bare().resolveString('', SUBJECT, '@Subscribe subject')).toBe('');
  });
});

describe('StartupResolver.resolveOptions', () => {
  it.each([
    [null, 'null'],
    [undefined, 'undefined'],
    ['S', '"S"'],
    [['S'], 'an array'],
  ])('rejects a function returning %j', (value, shown) => {
    const err = errorOf(() =>
      bare().resolveOptions(() => value as never, SUBJECT, '@Consume options'),
    );
    expect(err.message).toBe(
      `NATS handler H.on: @Consume options resolved to ${shown}; expected an object`,
    );
  });

  it('accepts undefined from a function only when the options are optional', () => {
    const r = bare();
    expect(r.resolveOptions(() => undefined, SUBJECT, '@Subscribe options', true)).toBeUndefined();
    expect(r.resolveOptions(undefined, SUBJECT, '@Subscribe options', true)).toBeUndefined();
    expect(
      errorOf(() => r.resolveOptions(() => null as never, SUBJECT, '@Subscribe options', true))
        .message,
    ).toBe('NATS handler H.on: @Subscribe options resolved to null; expected an object');
  });

  it.each([
    ['a whole object, without its contents', { prefix: 'p', natsPassword: 'hunter2' }, 'an object'],
    [
      'a circular object',
      (() => {
        const o: Record<string, unknown> = {};
        o.self = o;
        return o;
      })(),
      'an object',
    ],
    ['a BigInt', 10n, '10n'],
    ['a function', () => 'x', 'a function'],
  ])('describes %s in its error', (_, value, shown) => {
    const err = errorOf(() =>
      bare().resolveString(() => value as unknown as string, SUBJECT, '@Subscribe subject'),
    );
    expect(err.message).toBe(
      `NATS handler H.on: @Subscribe subject resolved to ${shown}; expected a non-empty string`,
    );
    expect(err.message).not.toContain('hunter2');
  });
});

describe('StartupResolver.classOptions', () => {
  const KEY = 'test:options';

  it('is undefined for a class without the decorator', () => {
    class Plain {}
    expect(bare().classOptions(KEY, Plain, '@NatsService')).toBeUndefined();
  });

  it('resolves the options stored on the class', () => {
    class Billing {}
    Reflect.defineMetadata(KEY, () => ({ name: 'svc' }), Billing);
    expect(bare().classOptions(KEY, Billing, '@NatsService')).toEqual({ name: 'svc' });
  });

  it('rejects options resolved to no object, naming the class', () => {
    class Billing {}
    Reflect.defineMetadata(KEY, () => null, Billing);
    expect(errorOf(() => bare().classOptions(KEY, Billing, '@NatsService')).message).toBe(
      '@NatsService Billing: @NatsService options resolved to null; expected an object',
    );
  });

  it('names the class when its options function throws', () => {
    class Billing {}
    Reflect.defineMetadata(
      KEY,
      () => {
        throw new Error('no config');
      },
      Billing,
    );
    expect(errorOf(() => bare().classOptions(KEY, Billing, '@NatsService')).message).toBe(
      '@NatsService Billing failed to resolve its @NatsService options: no config',
    );
  });
});

describe('StartupResolver.isDisabled', () => {
  it.each([
    [undefined, false],
    [false, false],
    [true, true],
  ])('reads %s as %s', (value, expected) => {
    expect(bare().isDisabled(value, SUBJECT)).toBe(expected);
  });

  it('calls a function once, with the providers', async () => {
    const r = await resolverOf([{ provide: 'FLAGS', useValue: { off: true } }]);
    const off = vi.fn(({ get }: StartupContext) => get<{ off: boolean }>('FLAGS').off);
    expect(r.isDisabled(off, SUBJECT)).toBe(true);
    expect(r.isDisabled(off, SUBJECT)).toBe(true);
    expect(off).toHaveBeenCalledOnce();
  });

  it('wraps what a function throws, naming the handler', () => {
    const cause = new Error('no config');
    const err = errorOf(() =>
      bare().isDisabled(() => {
        throw cause;
      }, SUBJECT),
    );
    expect(err.message).toBe('NATS handler H.on failed to resolve disabled: no config');
    expect(err.cause).toBe(cause);
  });

  it.each([
    ['a string', 'true', '"true"'],
    ['a function returning a string', () => 'yes', '"yes"'],
    ['null', null, 'null'],
  ])('rejects %s', (_, value, shown) => {
    expect(errorOf(() => bare().isDisabled(value as never, SUBJECT)).message).toBe(
      `NATS handler H.on has invalid disabled ${shown}; expected a boolean or a function returning one`,
    );
  });
});
