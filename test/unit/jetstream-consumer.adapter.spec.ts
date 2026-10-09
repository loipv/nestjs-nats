import type { ConsumerMessages, JsMsg } from '@nats-io/jetstream';
import { describe, expect, it, vi } from 'vitest';
import { JetStreamConsumerAdapter } from '../../src/adapters/jetstream-consumer.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-manager.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import type { NatsStreamManager } from '../../src/jetstream/stream-manager.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Push-driven async iterable whose `close()` ends iteration after buffered items (like nats.js). */
function fakeMessages() {
  const queue: JsMsg[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  const messages = {
    async *[Symbol.asyncIterator]() {
      for (;;) {
        const next = queue.shift();
        if (next) yield next;
        else if (closed) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
    close: () => {
      closed = true;
      wake?.();
      return Promise.resolve();
    },
  };
  return {
    messages: messages as unknown as ConsumerMessages,
    push(msg: JsMsg) {
      queue.push(msg);
      wake?.();
    },
  };
}

function fakeMsg(id: number) {
  const msg = {
    subject: 'ord.x',
    data: new TextEncoder().encode(JSON.stringify(id)),
    headers: undefined,
    info: { stream: 'ORD', streamSequence: id, deliveryCount: 1 },
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
  };
  return msg;
}

function setup(invoke: (payload: unknown) => Promise<unknown>) {
  const fake = fakeMessages();
  const consume = vi.fn(() => Promise.resolve(fake.messages));
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const entry = {
    name: 'default',
    options: {},
    logger,
    js: () => ({ consumers: { get: () => Promise.resolve({ consume }) } }),
  } as unknown as ConnectionEntry;
  const def = {
    kind: 'consume',
    connection: 'default',
    className: 'C',
    methodName: 'm',
    options: { stream: 'ORD', durable: 'd' },
    invoke,
  } as unknown as HandlerDefinition & { kind: 'consume' };
  const adapter = new JetStreamConsumerAdapter(def, entry, {} as NatsStreamManager);
  return { adapter, fake, consume, logger };
}

describe('JetStreamConsumerAdapter', () => {
  it('requests max(2 * concurrency, 2) messages', async () => {
    const { adapter, consume } = setup(() => Promise.resolve());
    await adapter.start();
    expect(consume).toHaveBeenCalledWith({ max_messages: 2 });
    await adapter.stop();
    await adapter.idle();
  });

  it('naks received-but-unstarted messages on stop and lets in-flight ones finish', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const handled: unknown[] = [];
    const { adapter, fake } = setup(async (payload) => {
      handled.push(payload);
      await gate;
    });
    await adapter.start();
    const [a, b, c] = [fakeMsg(1), fakeMsg(2), fakeMsg(3)];
    for (const m of [a, b, c]) fake.push(m as unknown as JsMsg);
    await tick();
    expect(handled).toEqual([1]);

    await adapter.stop();
    release();
    await adapter.idle();

    expect(handled).toEqual([1]);
    expect(a.ack).toHaveBeenCalledTimes(1);
    expect(b.nak).toHaveBeenCalledTimes(1);
    expect(c.nak).toHaveBeenCalledTimes(1);
    expect(b.ack).not.toHaveBeenCalled();
  });

  it('does not wait for in-flight handlers in stop()', async () => {
    const { adapter, fake } = setup(() => new Promise<void>(() => {}));
    await adapter.start();
    fake.push(fakeMsg(1) as unknown as JsMsg);
    await tick();
    await expect(adapter.stop()).resolves.toBeUndefined();
  });
});
