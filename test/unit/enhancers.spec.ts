import { headers as natsHeaders } from '@nats-io/transport-node';
import {
  BadRequestException,
  type CallHandler,
  type CanActivate,
  Catch,
  createParamDecorator,
  type ExceptionFilter,
  type ExecutionContext,
  ForbiddenException,
  Injectable,
  type NestInterceptor,
  Scope,
  UseFilters,
  UseGuards,
  UseInterceptors,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { IsString } from 'class-validator';
import { map, type Observable } from 'rxjs';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { NatsContext } from '../../src/context/nats-context.js';
import {
  Consume,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../src/decorators/handler.decorators.js';
import {
  Ctx,
  Headers,
  Msg,
  NatsParamType,
  Payload,
  Subject,
} from '../../src/decorators/param.decorators.js';
import { type HandlerDefinition, HandlerExplorer } from '../../src/discovery/handler-explorer.js';
import { NatsParamsFactory } from '../../src/discovery/nats-params-factory.js';

class Dto {
  @IsString()
  name!: string;
}

const Tenant = createParamDecorator(
  (_: unknown, ctx: ExecutionContext) =>
    ctx.switchToRpc().getContext<NatsContext>().headers['x-tenant'],
);

const seen: { guardType?: string; interceptorCalled: boolean; deny: boolean } = {
  interceptorCalled: false,
  deny: false,
};

@Injectable()
class G implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    seen.guardType = context.getType();
    return !seen.deny;
  }
}

@Injectable()
class I implements NestInterceptor {
  intercept(_: ExecutionContext, next: CallHandler): Observable<unknown> {
    seen.interceptorCalled = true;
    return next.handle().pipe(map((v) => v));
  }
}

@Catch()
class F implements ExceptionFilter {
  catch(): unknown {
    return 'handled';
  }
}

@UseGuards(G)
@UseInterceptors(I)
@Injectable()
class H {
  @Subscribe('x')
  on(@Payload(new ValidationPipe()) dto: Dto, @Tenant() t: string) {
    return { dto, t };
  }
}

@Injectable()
class Filtered {
  @UseFilters(F)
  @Subscribe('y', { connection: 'other', queue: 'q' })
  fail(): never {
    throw new Error('boom');
  }

  @Consume({ stream: 'S', durable: 'd', connection: 'js' })
  consume(@Payload('id') id: number, @Subject() s: string, @Headers('h') h: string) {
    return { id, s, h };
  }
}

@NatsService({ name: 'svc', version: '1.0.0', connection: 'svcconn' })
@Injectable()
class Svc {
  @ServiceEndpoint('get', { subject: 'svc.get' })
  get(@Ctx() ctx: NatsContext, @Msg() m: unknown) {
    return { kind: ctx.kind, same: m === ctx.raw };
  }

  @Subscribe('not-endpoint')
  plain() {
    return 'plain';
  }
}

// Method-level ValidationPipe (as a global one would) must validate only the payload.
@Injectable()
class Piped {
  @UsePipes(new ValidationPipe())
  @Subscribe('piped')
  piped(@Payload() dto: Dto, @Ctx() ctx: NatsContext, @Headers() h: Record<string, string>) {
    return { dto, ctx, h };
  }
}

// @ServiceEndpoint outside a @NatsService class is ignored.
@Injectable()
class Orphan {
  @ServiceEndpoint('orphan')
  orphan() {}
}

@Injectable({ scope: Scope.REQUEST })
class Scoped {
  @Subscribe('scoped')
  on() {}
}

function natsCtx(h: Record<string, string> = {}, subject = 'x') {
  const hdrs = natsHeaders();
  for (const [k, v] of Object.entries(h)) hdrs.set(k, v);
  return new NatsContext({
    kind: 'core',
    connection: 'default',
    raw: { subject, reply: '', headers: hdrs } as never,
  });
}

describe('HandlerExplorer', () => {
  let moduleRef: TestingModule;
  let defs: HandlerDefinition[];
  const find = (method: string) => {
    const d = defs.find((x) => x.methodName === method);
    if (!d) throw new Error(`no handler ${method}`);
    return d;
  };

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [HandlerExplorer, H, G, I, Filtered, Svc, Orphan, Scoped, Piped],
    }).compile();
    defs = moduleRef.get(HandlerExplorer).explore();
  });

  afterAll(() => moduleRef.close());

  beforeEach(() => {
    seen.guardType = undefined;
    seen.interceptorCalled = false;
    seen.deny = false;
  });

  it('discovers subscribe, consume and service-endpoint handlers', () => {
    expect(
      defs.map((d) => `${d.kind}:${d.className}.${d.methodName}@${d.connection}`).sort(),
    ).toEqual(
      [
        'subscribe:H.on@default',
        'subscribe:Filtered.fail@other',
        'consume:Filtered.consume@js',
        'service-endpoint:Svc.get@svcconn',
        'subscribe:Svc.plain@svcconn',
        'subscribe:Scoped.on@default',
        'subscribe:Piped.piped@default',
      ].sort(),
    );
  });

  it('exposes handler metadata', () => {
    const on = find('fail');
    expect(on).toMatchObject({ kind: 'subscribe', subject: 'y', options: { queue: 'q' } });
    const ep = find('get');
    expect(ep).toMatchObject({
      kind: 'service-endpoint',
      endpoint: 'get',
      options: { subject: 'svc.get' },
      service: { name: 'svc', version: '1.0.0' },
    });
    expect(ep.instance).toBeInstanceOf(Svc);
    expect(find('consume')).toMatchObject({
      kind: 'consume',
      options: { stream: 'S', durable: 'd' },
    });
    expect(find('plain')).toMatchObject({
      kind: 'subscribe',
      subject: 'not-endpoint',
      options: {},
    });
  });

  it('returns request-scoped handlers instead of skipping them', () => {
    const scoped = defs.find((d) => d.className === 'Scoped');
    expect(scoped).toBeDefined();
    expect(scoped?.wrapper.isDependencyTreeStatic()).toBe(false);
  });

  it('runs pipes, guards, interceptors and custom param decorators', async () => {
    const out = await find('on').invoke({ name: 'a' }, natsCtx({ 'x-tenant': 't1' }));
    expect(out).toEqual({ dto: expect.objectContaining({ name: 'a' }), t: 't1' });
    expect(seen.guardType).toBe('nats');
    expect(seen.interceptorCalled).toBe(true);
  });

  it('rejects invalid payload via ValidationPipe', async () => {
    await expect(find('on').invoke({}, natsCtx())).rejects.toBeInstanceOf(BadRequestException);
  });

  it('applies method-level ValidationPipe to the payload only', async () => {
    const ctx = natsCtx({ k: 'v' });
    const out = (await find('piped').invoke({ name: 'a' }, ctx)) as Record<string, unknown>;
    expect(out.ctx).toBe(ctx);
    expect(out.h).toEqual({ k: 'v' });
    expect(out.dto).toEqual({ name: 'a' });
    await expect(find('piped').invoke({ name: 1 }, natsCtx())).rejects.toBeInstanceOf(
      BadRequestException,
    );
  });

  it('rejects with ForbiddenException when a guard denies', async () => {
    seen.deny = true;
    await expect(find('on').invoke({ name: 'a' }, natsCtx())).rejects.toBeInstanceOf(
      ForbiddenException,
    );
    expect(seen.interceptorCalled).toBe(false);
  });

  it('lets exception filters swallow errors', async () => {
    await expect(find('fail').invoke(undefined, natsCtx())).resolves.toBe('handled');
  });

  it('resolves built-in param decorators', async () => {
    await expect(
      find('consume').invoke({ id: 7 }, natsCtx({ h: 'hv' }, 'orders.1')),
    ).resolves.toEqual({ id: 7, s: 'orders.1', h: 'hv' });
    const ctx = natsCtx();
    await expect(find('get').invoke(undefined, ctx)).resolves.toEqual({ kind: 'core', same: true });
  });
});

describe('NatsParamsFactory', () => {
  const f = new NatsParamsFactory();
  const ctx = natsCtx({ a: '1' }, 'sub.j');
  const args = [{ p: 2 }, ctx];

  it.each([
    [NatsParamType.PAYLOAD, undefined, { p: 2 }],
    [NatsParamType.PAYLOAD, 'p', 2],
    [NatsParamType.HEADERS, undefined, { a: '1' }],
    [NatsParamType.HEADERS, 'a', '1'],
    [NatsParamType.SUBJECT, undefined, 'sub.j'],
    [NatsParamType.MSG, undefined, ctx.raw],
    [NatsParamType.CONTEXT, undefined, ctx],
    [NatsParamType.CLOUD_EVENT, undefined, undefined],
    [99, undefined, undefined],
  ])('type %s data %s', (type, data, expected) =>
    expect(f.exchangeKeyForValue(type, data as never, args)).toEqual(expected),
  );

  it('tolerates a missing payload when a property is requested', () =>
    expect(f.exchangeKeyForValue(NatsParamType.PAYLOAD, 'p', [undefined, ctx])).toBeUndefined());
});
