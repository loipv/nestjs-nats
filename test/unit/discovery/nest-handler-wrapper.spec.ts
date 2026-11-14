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
  type Type,
  UseFilters,
  UseGuards,
  UseInterceptors,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { ExternalContextCreator } from '@nestjs/core';
import { Test, type TestingModule } from '@nestjs/testing';
import { IsString } from 'class-validator';
import { map, type Observable } from 'rxjs';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { CloudEventMeta } from '../../../src/cloudevents/types.js';
import { NatsContext } from '../../../src/context/nats-context.js';
import {
  CloudEvent,
  Ctx,
  Headers,
  Msg,
  Payload,
  Subject,
} from '../../../src/decorators/param.decorators.js';
import type { Invoke } from '../../../src/discovery/handler-definition.js';
import { wrapHandler } from '../../../src/discovery/nest-handler-wrapper.js';

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
  on(@Payload(new ValidationPipe()) dto: Dto, @Tenant() t: string) {
    return { dto, t };
  }
}

@Injectable()
class Filtered {
  @UseFilters(F)
  fail(): never {
    throw new Error('boom');
  }

  consume(@Payload('id') id: number, @Subject() s: string, @Headers('h') h: string) {
    return { id, s, h };
  }
}

@Injectable()
class Svc {
  get(@Ctx() ctx: NatsContext, @Msg() m: unknown) {
    return { kind: ctx.kind, same: m === ctx.raw };
  }
}

// Method-level ValidationPipe (as a global one would) must validate only the payload.
@Injectable()
class Piped {
  @UsePipes(new ValidationPipe())
  piped(@Payload() dto: Dto, @Ctx() ctx: NatsContext, @Headers() h: Record<string, string>) {
    return { dto, ctx, h };
  }
}

@Injectable()
class CeHandler {
  on(@CloudEvent() ce: CloudEventMeta | undefined, @Payload() p: unknown) {
    return { ce, p };
  }
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

describe('wrapHandler', () => {
  let moduleRef: TestingModule;
  /** `method` of the `type` provider, wrapped with Nest's enhancers. */
  const wrap = (type: Type, method: string): Invoke =>
    wrapHandler(moduleRef.get(ExternalContextCreator), moduleRef.get(type), method);

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      providers: [G, I, H, Filtered, Svc, Piped, CeHandler],
    }).compile();
  });

  afterAll(() => moduleRef.close());

  beforeEach(() => {
    seen.guardType = undefined;
    seen.interceptorCalled = false;
    seen.deny = false;
  });

  it('runs pipes, guards, interceptors and custom param decorators', async () => {
    const out = await wrap(H, 'on')({ name: 'a' }, natsCtx({ 'x-tenant': 't1' }));
    expect(out).toEqual({ dto: expect.objectContaining({ name: 'a' }), t: 't1' });
    expect(seen.guardType).toBe('nats');
    expect(seen.interceptorCalled).toBe(true);
  });

  it('rejects invalid payload via ValidationPipe', async () => {
    await expect(wrap(H, 'on')({}, natsCtx())).rejects.toBeInstanceOf(BadRequestException);
  });

  it('applies method-level ValidationPipe to the payload only', async () => {
    const ctx = natsCtx({ k: 'v' });
    const out = (await wrap(Piped, 'piped')({ name: 'a' }, ctx)) as Record<string, unknown>;
    expect(out.ctx).toBe(ctx);
    expect(out.h).toEqual({ k: 'v' });
    expect(out.dto).toEqual({ name: 'a' });
    await expect(wrap(Piped, 'piped')({ name: 1 }, natsCtx())).rejects.toBeInstanceOf(
      BadRequestException,
    );
  });

  it('rejects with ForbiddenException when a guard denies', async () => {
    seen.deny = true;
    await expect(wrap(H, 'on')({ name: 'a' }, natsCtx())).rejects.toBeInstanceOf(
      ForbiddenException,
    );
    expect(seen.interceptorCalled).toBe(false);
  });

  it('lets exception filters swallow errors', async () => {
    await expect(wrap(Filtered, 'fail')(undefined, natsCtx())).resolves.toBe('handled');
  });

  it('injects @CloudEvent() meta', async () => {
    const ce = { 'ce-specversion': '1.0', 'ce-id': '1', 'ce-source': 's', 'ce-type': 't' };
    const out = (await wrap(CeHandler, 'on')('body', natsCtx(ce))) as {
      ce: CloudEventMeta | undefined;
    };
    expect(out.ce).toMatchObject({ type: 't', source: 's', id: '1', specversion: '1.0' });
    const none = (await wrap(CeHandler, 'on')('body', natsCtx())) as { ce: unknown };
    expect(none.ce).toBeUndefined();
  });

  it('resolves built-in param decorators', async () => {
    await expect(
      wrap(Filtered, 'consume')({ id: 7 }, natsCtx({ h: 'hv' }, 'orders.1')),
    ).resolves.toEqual({ id: 7, s: 'orders.1', h: 'hv' });
    const ctx = natsCtx();
    await expect(wrap(Svc, 'get')(undefined, ctx)).resolves.toEqual({ kind: 'core', same: true });
  });
});
