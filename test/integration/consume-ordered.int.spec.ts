import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable, type Type } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsOrderedContext } from '../../src/context/nats-ordered-context.js';
import { ConsumeOrdered } from '../../src/decorators/handler.decorators.js';
import { Ctx, Payload } from '../../src/decorators/param.decorators.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();
const json = (v: unknown) => enc.encode(JSON.stringify(v));

@Injectable()
class RetryingHandlers {
  readonly seen: number[] = [];
  private failures = 0;

  @ConsumeOrdered({ stream: 'OR1', retry: { maxRetries: 3, initialDelay: 50, jitter: 0 } })
  on(@Payload() n: number) {
    this.seen.push(n);
    if (n === 3 && this.failures++ < 2) throw new Error('not yet');
  }
}

@Injectable()
class FanOutHandlers {
  readonly seen: number[] = [];

  @ConsumeOrdered({ stream: 'OR2' })
  on(@Payload() n: number) {
    this.seen.push(n);
  }
}

@Injectable()
class PoisonHandlers {
  readonly seen: number[] = [];

  @ConsumeOrdered({ stream: 'OR3', retry: false, dlq: true })
  on(@Payload() n: number) {
    this.seen.push(n);
    if (n === 2) throw new Error('poison');
  }
}

@Injectable()
class RecoveringHandlers {
  readonly seen: number[] = [];
  last?: NatsOrderedContext;

  @ConsumeOrdered({ stream: 'OR4' })
  on(@Payload() n: number, @Ctx() ctx: NatsOrderedContext) {
    this.seen.push(n);
    this.last = ctx;
  }
}

@Injectable()
class FromSequenceHandlers {
  readonly seen: number[] = [];

  @ConsumeOrdered({ stream: 'OR5', deliverPolicy: { startSeq: 3 } })
  on(@Payload() n: number) {
    this.seen.push(n);
  }
}

@Injectable()
class MissingStreamHandlers {
  @ConsumeOrdered({ stream: 'NOPE' })
  on() {}
}

describe('@ConsumeOrdered', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  const apps: TestingModule[] = [];

  const start = async <T>(provider: Type<T>): Promise<T> => {
    const app = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: srv.url }, logger: false })],
      providers: [provider],
    });
    apps.push(app);
    return app.get(provider);
  };
  const publish = async (subject: string, values: number[]) => {
    for (const n of values) await js.publish(subject, json(n));
  };

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    js = jetstream(rawNc);
    jsm = await jetstreamManager(rawNc);
    for (const n of [1, 2, 3, 4, 5]) {
      await jsm.streams.add({ name: `OR${n}`, subjects: [`or${n}.>`] });
    }
    // Dead letters of both replicas carry the same Nats-Msg-Id: deduplicated within 2 min.
    await jsm.streams.add({ name: 'ODLQ', subjects: ['dlq.>'], duplicate_window: 120e9 });
  });

  afterAll(async () => {
    for (const app of apps) await app.close();
    await rawNc?.close();
    await srv?.stop();
  });

  it('keeps stream order across retries', async () => {
    const h = await start(RetryingHandlers);
    await publish('or1.x', [1, 2, 3, 4, 5]);
    await eventually(() => expect(h.seen).toEqual([1, 2, 3, 3, 3, 4, 5]));
  });

  it('fans out to every instance', async () => {
    const a = await start(FanOutHandlers);
    const b = await start(FanOutHandlers);
    expect(a).not.toBe(b);
    await publish('or2.x', [1, 2, 3, 4, 5]);
    await eventually(() => {
      expect(a.seen).toEqual([1, 2, 3, 4, 5]);
      expect(b.seen).toEqual([1, 2, 3, 4, 5]);
    });
  });

  it('dead-letters once across replicas', async () => {
    const a = await start(PoisonHandlers);
    const b = await start(PoisonHandlers);
    await publish('or3.x', [1, 2, 3]);
    // Message 3 is processed once message 2 is dead-lettered.
    await eventually(() => {
      expect(a.seen).toEqual([1, 2, 3]);
      expect(b.seen).toEqual([1, 2, 3]);
    });
    expect((await jsm.streams.info('ODLQ')).state.messages).toBe(1);
    const m = await jsm.streams.getMessage('ODLQ', {
      last_by_subj: 'dlq.OR3.ordered.PoisonHandlers.on',
    });
    expect(m?.header.get('Nats-DLQ-Consumer')).toBe('ordered:PoisonHandlers.on');
    expect(m?.header.get('Nats-DLQ-Seq')).toBe('2');
    expect(m?.json()).toBe(2);
  });

  it('recovers after the consumer is deleted', async () => {
    const h = await start(RecoveringHandlers);
    await publish('or4.x', [1]);
    await eventually(() => expect(h.seen).toEqual([1]));
    const consumer = h.last?.raw.info.consumer;
    expect(consumer).toBeDefined();
    await jsm.consumers.delete('OR4', consumer as string);
    await publish('or4.x', [2, 3]);
    // With nats.js's default pull (30 s expiry, 15 s idle heartbeat) the consumer is
    // recreated once heartbeats are missed (about 45 s), and its first pull asks for one
    // message only (stale pending count): message 3 follows the next missed heartbeats
    // (about 75 s in all).
    await eventually(() => expect(h.seen).toEqual([1, 2, 3]), { timeout: 110_000, interval: 200 });
  }, 120_000);

  it('starts from a stream sequence', async () => {
    await publish('or5.x', [1, 2, 3, 4, 5]);
    const h = await start(FromSequenceHandlers);
    await eventually(() => expect(h.seen).toEqual([3, 4, 5]));
  });

  it('fails bootstrap when the stream is missing', async () => {
    await expect(start(MissingStreamHandlers)).rejects.toThrow(
      new NatsConfigError('Stream "NOPE" not found for @ConsumeOrdered MissingStreamHandlers.on'),
    );
  });
});
