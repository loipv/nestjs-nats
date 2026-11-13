import type { ConsumerMessages, JsMsg } from '@nats-io/jetstream';
import { describe, expect, it, vi } from 'vitest';
import { discardMessages } from '../../../src/adapters/discard-messages.js';
import { tick } from '../../support/async.js';

/** Consumer messages that yield `buffered`, then end (or throw `fail`). */
function fakeMessages(
  buffered: string[],
  opts: { close?: () => Promise<unknown>; fail?: Error } = {},
) {
  const close = vi.fn(opts.close ?? (() => Promise.resolve()));
  const messages = {
    close,
    async *[Symbol.asyncIterator]() {
      for (const m of buffered) yield m as unknown as JsMsg;
      if (opts.fail) throw opts.fail;
    },
  } as unknown as ConsumerMessages;
  return { messages, close };
}

describe('discardMessages', () => {
  it('closes the messages and hands each buffered one to discard, in order', async () => {
    const { messages, close } = fakeMessages(['a', 'b', 'c']);
    const discarded: unknown[] = [];
    const onError = vi.fn();
    await discardMessages(messages, (m) => discarded.push(m), onError);
    expect(close).toHaveBeenCalledOnce();
    expect(discarded).toEqual(['a', 'b', 'c']);
    expect(onError).not.toHaveBeenCalled();
  });

  it('resolves only once the close has settled', async () => {
    let finishClose!: () => void;
    const { messages } = fakeMessages([], {
      close: () =>
        new Promise<void>((resolve) => {
          finishClose = resolve;
        }),
    });
    let done = false;
    const discarding = discardMessages(messages, () => {}, vi.fn()).then(() => {
      done = true;
    });
    await tick();
    expect(done).toBe(false);
    finishClose();
    await discarding;
    expect(done).toBe(true);
  });

  it('reports a close that rejects, still reads the messages, and never rejects', async () => {
    const err = new Error('close failed');
    const { messages } = fakeMessages(['a'], { close: () => Promise.reject(err) });
    const discard = vi.fn();
    const onError = vi.fn();
    await expect(discardMessages(messages, discard, onError)).resolves.toBeUndefined();
    expect(onError).toHaveBeenCalledWith(err);
    expect(discard).toHaveBeenCalledWith('a');
  });

  it('reports a close that throws synchronously', async () => {
    const err = new Error('closed already');
    const { messages } = fakeMessages(['a'], {
      close: () => {
        throw err;
      },
    });
    const discard = vi.fn();
    const onError = vi.fn();
    await expect(discardMessages(messages, discard, onError)).resolves.toBeUndefined();
    expect(onError).toHaveBeenCalledWith(err);
    expect(discard).toHaveBeenCalledWith('a');
  });

  it('reports an iterator that fails, after discarding what it yielded', async () => {
    const err = new Error('iterator failed');
    const { messages } = fakeMessages(['a', 'b'], { fail: err });
    const discarded: unknown[] = [];
    const onError = vi.fn();
    await expect(
      discardMessages(messages, (m) => discarded.push(m), onError),
    ).resolves.toBeUndefined();
    expect(discarded).toEqual(['a', 'b']);
    expect(onError).toHaveBeenCalledExactlyOnceWith(err);
  });
});
