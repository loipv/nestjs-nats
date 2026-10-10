import {
  ConflictException,
  type INestApplicationContext,
  Inject,
  Injectable,
  ValidationPipe,
} from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { IsInt } from 'class-validator';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsClient } from '../../src/client/nats-client.js';
import type { CloudEventMeta } from '../../src/cloudevents/types.js';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { ConnectionRegistry } from '../../src/connection/connection-registry.js';
import type { NatsBatchContext } from '../../src/context/nats-batch-context.js';
import type { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { CloudEventRouter, OnCloudEvent } from '../../src/decorators/cloudevent.decorators.js';
import {
  Consume,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../src/decorators/handler.decorators.js';
import { CloudEvent, Ctx, Payload } from '../../src/decorators/param.decorators.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import {
  getConnectionToken,
  getJetStreamClientToken,
  getNatsClientToken,
} from '../../src/module/tokens.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import {
  type MockJetStreamClient,
  type MockNatsClient,
  NatsTestHarness,
  NatsTestingModule,
} from '../../src/testing/index.js';
import { subjectMatches } from '../../src/testing/subject.js';

class OrderDto {
  @IsInt()
  id!: number;
}

const calls: string[] = [];

@Injectable()
class Handlers {
  constructor(@Inject(NatsClient) private readonly nats: NatsClient) {}

  @Consume({ stream: 'ORDERS', durable: 'billing', config: { filter_subject: 'billing.*' } })
  bill(@Payload() p: unknown, @Ctx() ctx: NatsJetStreamContext) {
    this.nats.publish('billed', p);
    return { billed: p, seq: ctx.seq, deliveryCount: ctx.deliveryCount, subject: ctx.subject };
  }

  @Consume({ stream: 'ORDERS', durable: 'flaky' })
  flaky() {
    throw new Error('boom');
  }

  @Consume({ stream: 'ORDERS', durable: 'dead', dlq: true })
  dead() {
    throw new Error('dead');
  }

  @Consume({ stream: 'ORDERS', durable: 'valid' })
  validated(@Payload(new ValidationPipe()) dto: OrderDto) {
    return dto;
  }

  @Consume({ stream: 'ORDERS', durable: 'manual', ack: 'manual' })
  manual(@Ctx() ctx: NatsJetStreamContext) {
    ctx.nak(1234);
  }

  @Consume({ stream: 'ORDERS', durable: 'idem', idempotency: true })
  idem() {
    calls.push('idem');
  }

  @Consume({ stream: 'ORDERS', durable: 'batch', batch: { maxSize: 10 } })
  batch(@Payload() items: number[], @Ctx() ctx: NatsBatchContext) {
    const i = items.indexOf(2);
    if (i >= 0) ctx.fail(i, new Error('two'));
  }

  @Subscribe('orders.*')
  onOrder(@Payload() p: unknown) {
    return { got: p };
  }

  @Subscribe('fail.core')
  failCore() {
    calls.push('failCore');
    throw new Error('nope');
  }

  @Subscribe('shared.*')
  sharedA() {}

  @Subscribe('shared.x')
  sharedB() {}
}

@NatsService({ name: 'orders', version: '1.0.0' })
@Injectable()
class Service {
  @ServiceEndpoint('create')
  create() {
    throw new ConflictException('Order exists');
  }

  @ServiceEndpoint('get', { subject: 'orders-svc.get' })
  get(@Payload() id: number) {
    return { id };
  }
}

@CloudEventRouter({ subscribe: 'events' })
@Injectable()
class Events {
  @OnCloudEvent('o.created')
  created(@Payload() p: unknown, @CloudEvent() meta: CloudEventMeta) {
    return { type: meta.type, source: meta.source, p };
  }
}

describe('NatsTestHarness', () => {
  let moduleRef: TestingModule;
  let h: NatsTestHarness;

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot({ retry: { initialDelay: 10, jitter: 0 } })],
      providers: [Handlers, Service, Events],
    }).compile();
    h = NatsTestHarness.create(moduleRef);
  });

  afterAll(async () => {
    await moduleRef.close();
  });

  it('is the injected harness', () => {
    expect(h).toBeInstanceOf(NatsTestHarness);
    expect(moduleRef.get(NatsTestHarness)).toBe(h);
  });

  it('acks a successful @Consume handler', async () =>
    expect((await h.dispatch({ handler: 'Handlers.bill' }, { id: 1 })).settlement).toBe('ack'));

  it('hands the handler a JetStream context and returns its result', async () => {
    const r = await h.dispatch({ handler: 'Handlers.bill' }, { id: 2 }, { deliveryCount: 3 });
    expect(r).toMatchObject({
      settlement: 'ack',
      outcomes: [{ action: 'ack' }],
      dlq: [],
      result: { billed: { id: 2 }, deliveryCount: 3, subject: 'billing.*' },
    });
    expect((r.result as { seq: number }).seq).toBeGreaterThan(0);
  });

  it('naks a failing @Consume handler with the backoff delay', async () =>
    expect(await h.dispatch({ handler: 'Handlers.flaky' }, {})).toMatchObject({
      settlement: 'nak',
      nakDelay: 10,
    }));

  it('dead-letters an exhausted message', async () => {
    const r = await h.dispatch({ handler: 'Handlers.dead' }, { id: 9 }, { deliveryCount: 6 });
    expect(r.settlement).toBe('dlq');
    expect(r.dlq).toHaveLength(1);
    expect(r.dlq[0]).toMatchObject({ subject: 'dlq.ORDERS.dead', data: { id: 9 } });
    expect(r.dlq[0]?.headers['Nats-DLQ-Deliveries']).toBe('6');
    expect(r.dlq[0]?.headers['Nats-DLQ-Error']).toBe('dead');
  });

  it('runs pipes and guards', async () => {
    const r = await h.dispatch({ handler: 'Handlers.validated' }, { id: 'x' });
    expect(r.settlement).toBe('term');
    expect(r.outcomes[0]).toMatchObject({ action: 'term' });
    expect((r.outcomes[0] as { error: unknown }).error).toMatchObject({ status: 400 });
    expect((await h.dispatch({ handler: 'Handlers.validated' }, { id: 1 })).settlement).toBe('ack');
  });

  it('reports a manual settlement', async () =>
    expect(await h.dispatch({ handler: 'Handlers.manual' }, {})).toMatchObject({
      settlement: 'nak',
      nakDelay: 1234,
    }));

  it('replies to a request', async () => {
    const r = await h.dispatch({ handler: 'Handlers.onOrder' }, { n: 1 }, { request: true });
    expect(r).toMatchObject({ settlement: 'replied', reply: { data: { got: { n: 1 } } } });
    const noRequest = await h.dispatch({ handler: 'Handlers.onOrder' }, { n: 1 });
    expect(noRequest.settlement).toBe('none');
    expect(noRequest.reply).toBeUndefined();
  });

  it('replies with an error to a failing request', async () => {
    const r = await h.dispatch({ handler: 'Handlers.failCore' }, {}, { request: true });
    expect(r).toMatchObject({
      settlement: 'replied',
      reply: { error: { code: 500, description: 'Internal Server Error' } },
    });
  });

  it('retries a failing core handler without running it again', async () => {
    calls.length = 0;
    const r = await h.dispatch({ handler: 'Handlers.failCore' }, {});
    expect(r.settlement).toBe('retry');
    expect(r.outcomes[0]).toMatchObject({ action: 'retry', delayMs: 10 });
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(calls).toEqual(['failCore']);
    // Past the last attempt it is dropped.
    expect(
      (await h.dispatch({ handler: 'Handlers.failCore' }, {}, { attempt: 6 })).settlement,
    ).toBe('drop');
  });

  it('replies with service errors from endpoints', async () => {
    const r = await h.dispatch({ handler: 'Service.create' }, {});
    expect(r).toMatchObject({
      settlement: 'replied',
      reply: { error: { code: 409, description: 'Order exists' } },
    });
    expect(await h.dispatch({ subject: 'orders-svc.get' }, 7).then((x) => x.reply)).toEqual({
      data: { id: 7 },
    });
  });

  it('routes CloudEvents', async () => {
    const r = await h.dispatch(
      { handler: 'Events.@CloudEventRouter' },
      { id: 1 },
      { cloudEvent: { type: 'o.created', source: 's' } },
    );
    expect(r).toMatchObject({
      settlement: 'none',
      result: { type: 'o.created', source: 's', p: { id: 1 } },
    });
    expect((await h.dispatch({ subject: 'events' }, 1)).outcomes[0]).toMatchObject({
      action: 'drop',
    });
  });

  it('hands non-JSON CloudEvent data to the route as bytes', async () => {
    const bytes = new TextEncoder().encode('123');
    const r = await h.dispatch({ handler: 'Events.@CloudEventRouter' }, bytes, {
      cloudEvent: { type: 'o.created', source: 's', datacontenttype: 'text/plain' },
    });
    expect(r.result).toMatchObject({ type: 'o.created', p: bytes });
  });

  it('skips idempotent duplicates across dispatches', async () => {
    calls.length = 0;
    const headers = { 'Nats-Msg-Id': 'm1' };
    expect((await h.dispatch({ handler: 'Handlers.idem' }, {}, { headers })).settlement).toBe(
      'ack',
    );
    expect((await h.dispatch({ handler: 'Handlers.idem' }, {}, { headers })).settlement).toBe(
      'skipped',
    );
    expect(calls).toEqual(['idem']);
  });

  it('dispatches a batch', async () => {
    const r = await h.dispatch({ handler: 'Handlers.batch' }, [1, 2, 3]);
    expect(r.outcomes).toHaveLength(3);
    expect(r.outcomes.map((o) => o.action)).toEqual(['ack', 'retry', 'ack']);
    expect(r.settlement).toBe('ack');
    await expect(h.dispatch({ handler: 'Handlers.batch' }, 1)).rejects.toThrow(NatsConfigError);
  });

  it('resolves handlers by subject', async () => {
    const r = await h.dispatch({ subject: 'orders.created' }, { n: 2 }, { request: true });
    expect(r.reply).toEqual({ data: { got: { n: 2 } } });
    // A consumer is matched through its filter subject; the message gets the target subject.
    const bill = await h.dispatch({ subject: 'billing.eu' }, 1);
    expect(bill.result).toMatchObject({ billed: 1, subject: 'billing.eu' });
  });

  it('uses the actual subject given in the options', async () => {
    const r = await h.dispatch({ handler: 'Handlers.bill' }, 1, { subject: 'billing.us' });
    expect(r.result).toMatchObject({ subject: 'billing.us' });
  });

  it.each([[{ subject: 'nothing.here' }], [{ subject: 'shared.x' }], [{ handler: 'Nope.x' }]])(
    'rejects unknown or ambiguous target %j',
    async (target) => await expect(h.dispatch(target, {})).rejects.toThrow(NatsConfigError),
  );

  it('lists the candidates of an ambiguous target', async () => {
    const err = await h.dispatch({ subject: 'shared.x' }, {}).catch((e: unknown) => e);
    expect((err as Error).message).toContain('Handlers.sharedA');
    expect((err as Error).message).toContain('Handlers.sharedB');
  });

  it('provides mock clients for the connection', async () => {
    const client = moduleRef.get<MockNatsClient>(NatsClient);
    expect(moduleRef.get(getNatsClientToken())).toBe(client);
    const before = client.published.length;
    await h.dispatch({ handler: 'Handlers.bill' }, { id: 5 });
    expect(client.published.slice(before)).toEqual([
      expect.objectContaining({ subject: 'billed', data: { id: 5 } }),
    ]);
    const js = moduleRef.get<MockJetStreamClient>(JetStreamClient);
    expect(moduleRef.get(getJetStreamClientToken())).toBe(js);
    await js.publish('x', 1);
    expect(js.published.at(-1)).toMatchObject({ subject: 'x', data: 1 });
    expect(moduleRef.get(getConnectionToken())).toBeDefined();
  });

  it('has no JetStream management', async () => {
    const manager = moduleRef.get(ConnectionRegistry);
    await expect(manager.get().jsm()).rejects.toThrow(
      new NatsConfigError('JetStream management is not available in NatsTestingModule'),
    );
    await expect(moduleRef.get(NatsStreamManager).info('ORDERS')).rejects.toThrow(NatsConfigError);
    expect(manager.status()).toMatchObject({ name: 'default', state: 'connected' });
    // The public facade answers from the same (fake) connections.
    const facade = moduleRef.get(NatsConnectionManager);
    expect(facade.names()).toEqual(['default']);
    expect(facade.has('default')).toBe(true);
    expect(facade.status()).toEqual(manager.status());
  });
});

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

describe('subjectMatches', () => {
  it.each([
    ['a.b', 'a.b', true],
    ['a.*', 'a.b', true],
    ['a.*', 'a.b.c', false],
    ['a.*', 'a', false],
    ['*.b', 'a.b', true],
    ['a.>', 'a.b', true],
    ['a.>', 'a.b.c', true],
    ['a.>', 'a', false],
    ['>', 'a.b', true],
    ['a.b', 'a.c', false],
    ['a.*.c', 'a.x.c', true],
    ['a.*', 'a.*', true],
  ])('%s ~ %s is %s', (pattern, subject, expected) =>
    expect(subjectMatches(pattern, subject)).toBe(expected),
  );
});

describe('NatsTestHarness with plugins', () => {
  it('runs inbound plugins and surfaces their failures', async () => {
    const seen: string[] = [];
    let fail = false;
    const plugin: NatsPlugin = {
      name: 'p',
      inbound: {
        stage: 'outer',
        async handle(d, next) {
          seen.push(d.handler.methodName);
          if (fail) throw new Error('plugin failed');
          await next();
        },
      },
    };
    @Injectable()
    class H {
      @Consume({ stream: 'S', durable: 'd' })
      on() {}
    }
    const moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot({ plugins: [plugin], logger: false })],
      providers: [H],
    }).compile();
    const h = NatsTestHarness.create(moduleRef);
    expect((await h.dispatch({ handler: 'H.on' }, 1)).settlement).toBe('ack');
    fail = true;
    await expect(h.dispatch({ handler: 'H.on' }, 1)).rejects.toThrow('plugin failed');
    expect(seen).toEqual(['on', 'on']);
    await moduleRef.close();
  });
});

describe('NatsTestHarness fake messages', () => {
  it('builds JetStream and core messages as the adapters receive them', async () => {
    const seen: { js?: NatsJetStreamContext; core?: NatsContext } = {};
    @Injectable()
    class H {
      @Consume({ stream: 'S', durable: 'd' })
      js(@Ctx() ctx: NatsJetStreamContext) {
        ctx.working();
        seen.js = ctx;
      }

      @Subscribe('c.>')
      core(@Ctx() ctx: NatsContext) {
        seen.core = ctx;
      }
    }
    const moduleRef = await Test.createTestingModule({
      imports: [NatsTestingModule.forRoot({ logger: false })],
      providers: [H],
    }).compile();
    const h = NatsTestHarness.create(moduleRef);
    await h.dispatch({ handler: 'H.js' }, 1, { deliveryCount: 2, headers: { a: '1' } });
    const js = seen.js as NatsJetStreamContext;
    expect(js.raw.redelivered).toBe(true);
    expect(js.raw.info).toMatchObject({ stream: 'S', consumer: 'd', deliveryCount: 2 });
    expect(js.raw.info.streamSequence).toBe(js.seq);
    expect(js.headers).toEqual({ a: '1' });
    expect((js.raw as unknown as { calls: unknown[] }).calls).toEqual([
      { op: 'working' },
      { op: 'ack' },
    ]);
    await h.dispatch({ subject: 'c.x.y' }, 1, { request: true });
    expect(seen.core?.subject).toBe('c.x.y');
    expect(seen.core?.replyTo).toMatch(/^_INBOX\./);
    await h.dispatch({ subject: 'c.x.y' }, 1);
    expect(seen.core?.replyTo).toBeUndefined();
    await expect(h.dispatch({ handler: 'H.js' }, 1, { deliveryCount: 0 })).rejects.toThrow(
      NatsConfigError,
    );
    await moduleRef.close();
  });
});
