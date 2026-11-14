import type { NatsConnection } from '@nats-io/transport-node';
import { NoRespondersError, headers as natsHeaders } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../../src/client/nats-client.js';
import { jsonCodec } from '../../../src/codec/codec.js';
import {
  type ConnectionEntry,
  ConnectionRegistry,
} from '../../../src/connection/connection-registry.js';
import { NatsConfigError, NatsDecodeError, NatsRequestError } from '../../../src/errors/errors.js';
import type { NatsPlugin } from '../../../src/pipeline/types.js';
import { FakeNatsConnection } from '../../../src/testing/fake-connection.js';

class FakeConnectionManager extends ConnectionRegistry {
  constructor(private readonly fake: unknown) {
    super();
  }

  protected override openConnection(): Promise<NatsConnection> {
    return Promise.resolve(this.fake as NatsConnection);
  }
}

describe('NatsClient at close', () => {
  it('sends a publish still in its outbound plugins before the connection drains', async () => {
    const fake = new FakeNatsConnection(jsonCodec());
    const events: string[] = [];
    fake.onPublish((m) => events.push(`publish ${m.subject}`));
    const drain = fake.drain.bind(fake);
    vi.spyOn(fake, 'drain').mockImplementation(() => {
      events.push('drain');
      return drain();
    });
    const slow: NatsPlugin = {
      name: 'slow',
      outbound: {
        async handle(_m, next) {
          await new Promise((resolve) => setTimeout(resolve, 50));
          await next();
        },
      },
    };
    const mgr = new FakeConnectionManager(fake);
    const entry = await mgr.connect('default', { connection: {}, logger: false, plugins: [slow] });
    const client = new NatsClient(entry);
    client.publish('o', 1);
    await mgr.closeAll();
    expect(events).toEqual(['publish o', 'drain']);
  });
});

describe('NatsClient.publish without outbound plugins', () => {
  function setup(publish: () => void) {
    const error = vi.fn();
    const entry = {
      name: 'default',
      options: { connection: {} },
      nc: { publish: vi.fn(publish), flush: vi.fn(async () => {}) },
      logger: { log: vi.fn(), warn: vi.fn(), error },
      onClose: () => () => {},
    } as unknown as ConnectionEntry;
    return { client: new NatsClient(entry), nc: entry.nc, error };
  }

  it('publishes at once', () => {
    const { client, nc } = setup(() => {});
    client.publish('o', { n: 1 }, { headers: { a: '1' } });
    expect(nc.publish).toHaveBeenCalledOnce();
    const [subject, payload, opts] = vi.mocked(nc.publish).mock.calls[0] as unknown as [
      string,
      Uint8Array,
      { headers: { get(k: string): string } },
    ];
    expect(subject).toBe('o');
    expect(new TextDecoder().decode(payload)).toBe('{"n":1}');
    expect(opts.headers.get('a')).toBe('1');
  });

  it('logs a failure at once, with credentials masked, and never throws', () => {
    const { client, error } = setup(() => {
      throw new Error('closed nats://u:secret@h');
    });
    expect(() => client.publish('o', 1)).not.toThrow();
    expect(error).toHaveBeenCalledExactlyOnceWith('Failed to publish to "o": closed nats://***@h');
  });

  it('still throws an invalid cloudEvent to the caller', () => {
    const { client, nc } = setup(() => {});
    expect(() => client.publish('o', 1, { cloudEvent: { type: 't' } })).toThrow(/source/);
    expect(nc.publish).not.toHaveBeenCalled();
  });
});

describe('NatsClient.request no responders', () => {
  function clientRejecting(err: unknown) {
    const entry = {
      name: 'default',
      options: { connection: {} },
      nc: { request: vi.fn(() => Promise.reject(err)) },
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
      onClose: () => () => {},
    } as unknown as ConnectionEntry;
    return new NatsClient(entry);
  }

  it('recognises a NoRespondersError deeper in the cause chain', async () => {
    const err = new Error('request failed', {
      cause: new Error('503', { cause: new NoRespondersError('svc') }),
    });
    await expect(clientRejecting(err).request('svc')).rejects.toMatchObject({
      code: 'no_responders',
      subject: 'svc',
    });
  });

  it('recognises one from another copy of the NATS library by its name', async () => {
    const foreign = Object.assign(new Error('no responders'), { name: 'NoRespondersError' });
    await expect(
      clientRejecting(new Error('request failed', { cause: foreign })).request('svc'),
    ).rejects.toMatchObject({ code: 'no_responders' });
  });
});

describe('NatsClient.requestMany', () => {
  const enc = new TextEncoder();

  function reply(data: Uint8Array, h: Record<string, string> = {}) {
    const hdrs = Object.keys(h).length > 0 ? natsHeaders() : undefined;
    for (const [k, v] of Object.entries(h)) hdrs?.set(k, v);
    return { subject: '_INBOX.x', data, headers: hdrs };
  }

  /** A client whose `nc.requestMany` yields `replies`, then ends with `end` if given. */
  function setup(replies: unknown[], end?: Error, plugins: NatsPlugin[] = []) {
    const requestMany = vi.fn(async () => ({
      async *[Symbol.asyncIterator]() {
        yield* replies;
        if (end) throw end;
      },
    }));
    const entry = {
      name: 'default',
      options: { connection: {}, plugins },
      nc: { requestMany },
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
      onClose: () => () => {},
    } as unknown as ConnectionEntry;
    return { client: new NatsClient(entry), requestMany };
  }

  it.each([
    [{}, { strategy: 'timer', maxWait: 1000 }],
    [{ maxMessages: 2 }, { strategy: 'count', maxWait: 1000, maxMessages: 2 }],
    [
      { stall: 50, maxMessages: 2 },
      { strategy: 'stall', maxWait: 1000, stall: 50, maxMessages: 2 },
    ],
  ])('sends %j as %j', async (opts, expected) => {
    const { client, requestMany } = setup([]);
    await client.requestMany('svc', { q: 1 }, opts);
    const [subject, payload, sent] = requestMany.mock.calls[0] as unknown as [
      string,
      Uint8Array,
      Record<string, unknown>,
    ];
    expect(subject).toBe('svc');
    expect(new TextDecoder().decode(payload)).toBe('{"q":1}');
    expect(sent).toEqual(expected);
  });

  it('gathers every reply in order, a failing one as its error', async () => {
    const { client } = setup([
      reply(enc.encode('{"ok":1}')),
      reply(new Uint8Array(0), { 'Nats-Service-Error-Code': '503', 'Nats-Service-Error': 'busy' }),
      reply(enc.encode('not json{')),
    ]);
    const replies = await client.requestMany('svc');
    expect(replies).toHaveLength(3);
    expect(replies[0]).toEqual({ data: { ok: 1 }, subject: '_INBOX.x', headers: {} });
    const [, failed, undecodable] = replies as Array<{ error: Error }>;
    expect(failed?.error).toBeInstanceOf(NatsRequestError);
    expect(failed?.error).toMatchObject({
      code: 'service_error',
      serviceCode: 503,
      message: 'Service error 503 from "svc": busy',
    });
    expect(undecodable?.error).toBeInstanceOf(NatsDecodeError);
  });

  /** Replies that keep coming every `every` ms until stopped, as nats.js's iterator does. */
  function endless(every: number) {
    let stopped = false;
    const stop = vi.fn(() => {
      stopped = true;
    });
    const iterable = {
      stop,
      async *[Symbol.asyncIterator]() {
        let n = 0;
        while (!stopped) {
          await new Promise((r) => setTimeout(r, every));
          if (!stopped) yield reply(enc.encode(String(n++)));
        }
      },
    };
    return { iterable, stop };
  }

  function clientOver(iterable: unknown) {
    const entry = {
      name: 'default',
      options: { connection: {} },
      nc: { requestMany: vi.fn(async () => iterable) },
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
      onClose: () => () => {},
    } as unknown as ConnectionEntry;
    return new NatsClient(entry);
  }

  it('stops at maxMessages whatever the strategy, and ends the replies', async () => {
    const { iterable, stop } = endless(1);
    const replies = await clientOver(iterable).requestMany('svc', undefined, {
      stall: 100,
      maxMessages: 2,
    });
    expect(replies.map((r) => ('data' in r ? r.data : r))).toEqual([0, 1]);
    expect(stop).toHaveBeenCalled();
  });

  it('never waits longer than maxWait, even while replies keep coming', async () => {
    const { iterable, stop } = endless(10);
    const start = Date.now();
    const replies = await clientOver(iterable).requestMany('svc', undefined, {
      maxWait: 100,
      stall: 50,
    });
    expect(Date.now() - start).toBeLessThan(300);
    expect(replies.length).toBeGreaterThan(0);
    expect(stop).toHaveBeenCalled();
  });

  it('resolves [] when nobody answers', async () => {
    const { client } = setup([], new NoRespondersError('svc'));
    await expect(client.requestMany('svc')).resolves.toEqual([]);
  });

  it('rejects with any other failure', async () => {
    const closed = new Error('closed');
    const { client } = setup([], closed);
    await expect(client.requestMany('svc')).rejects.toBe(closed);
  });

  it.each([
    ['maxWait', { maxWait: 0 }, 'a number of ms in (0, 2147483647]'],
    ['maxWait', { maxWait: 2 ** 31 }, 'a number of ms in (0, 2147483647]'],
    ['maxMessages', { maxMessages: 0 }, 'an integer >= 1'],
    ['maxMessages', { maxMessages: 1.5 }, 'an integer >= 1'],
    ['stall', { stall: 0 }, 'a number of ms in (0, maxWait]'],
    ['stall', { stall: 2000, maxWait: 1000 }, 'a number of ms in (0, maxWait]'],
  ])('rejects an invalid %s (%j)', async (field, opts, expected) => {
    const { client, requestMany } = setup([]);
    const value = (opts as Record<string, number>)[field];
    await expect(client.requestMany('svc', undefined, opts)).rejects.toThrow(
      new NatsConfigError(
        `requestMany to "svc" has invalid ${field} ${value}; expected ${expected}`,
      ),
    );
    expect(requestMany).not.toHaveBeenCalled();
  });

  it('goes through the outbound plugins as a request', async () => {
    const kinds: string[] = [];
    const plugin: NatsPlugin = {
      name: 'tag',
      outbound: {
        async handle(m, next) {
          kinds.push(m.kind);
          m.headers.set('x-tag', 'yes');
          await next();
        },
      },
    };
    const { client, requestMany } = setup([], undefined, [plugin]);
    await client.requestMany('svc');
    expect(kinds).toEqual(['request']);
    const sent = (
      requestMany.mock.calls[0] as unknown as [
        string,
        unknown,
        { headers: { get(k: string): string } },
      ]
    )[2];
    expect(sent.headers.get('x-tag')).toBe('yes');
  });

  it('rejects when a plugin drops the request', async () => {
    const dropping: NatsPlugin = { name: 'drop', outbound: { handle: async () => {} } };
    const { client, requestMany } = setup([], undefined, [dropping]);
    await expect(client.requestMany('svc')).rejects.toThrow('Request to "svc" produced no reply');
    expect(requestMany).not.toHaveBeenCalled();
  });
});

describe('NatsClient.publish error hook', () => {
  function setup(onPublishError?: unknown) {
    const error = vi.fn();
    const entry = {
      name: 'default',
      options: { connection: {}, onPublishError },
      nc: {
        publish: vi.fn(() => {
          throw new Error('closed');
        }),
        flush: vi.fn(async () => {}),
      },
      logger: { log: vi.fn(), warn: vi.fn(), error },
      onClose: () => () => {},
    } as unknown as ConnectionEntry;
    return { client: new NatsClient(entry), nc: entry.nc, error };
  }

  it("hands a failure to the connection's hook before publish() returns, without logging it", () => {
    const hook = vi.fn();
    const { client, error } = setup(hook);
    client.publish('o', 1);
    expect(hook).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ message: 'closed' }), {
      subject: 'o',
      connection: 'default',
    });
    expect(error).not.toHaveBeenCalled();
  });

  it('prefers the onError of the call', () => {
    const hook = vi.fn();
    const onError = vi.fn();
    const { client } = setup(hook);
    client.publish('o', 1, { onError });
    expect(onError).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ message: 'closed' }));
    expect(hook).not.toHaveBeenCalled();
  });

  it('logs both errors when the hook throws, and never throws itself', () => {
    const broken = new Error('hook broke');
    const { client, error } = setup(() => {
      throw broken;
    });
    expect(() => client.publish('o', 1)).not.toThrow();
    expect(error.mock.calls).toEqual([
      ['Publish to "o": the publish error handler failed', broken.stack],
      ['Failed to publish to "o": closed'],
    ]);
  });

  it('logs both errors when an async hook rejects, and flush() waits for it', async () => {
    const broken = new Error('outbox down');
    const { client, error } = setup(async () => {
      await Promise.resolve();
      throw broken;
    });
    client.publish('o', 1);
    await client.flush();
    expect(error.mock.calls).toEqual([
      ['Publish to "o": the publish error handler failed', broken.stack],
      ['Failed to publish to "o": closed'],
    ]);
  });

  it('rejects an onError that is not a function before sending anything', () => {
    const { client, nc } = setup();
    expect(() => client.publish('o', 1, { onError: 'x' as never })).toThrow(NatsConfigError);
    expect(nc.publish).not.toHaveBeenCalled();
  });
});
