import { inspect } from 'node:util';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  redactError,
  redactServers,
  redactText,
  serverSecrets,
} from '../../../src/connection/redact.js';

const chars = (alphabet: string) => fc.constantFrom(...alphabet.split('')).map((c) => c as string);

/** A secret: never contains `*`, so the `***` mask cannot spell one. */
const secret = fc.string({
  unit: chars('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!$%&-_.~'),
  minLength: 1,
  maxLength: 16,
});

/** A password of a server URL: anything but the characters that end the userinfo. */
const password = fc.string({
  unit: chars('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!$%&-_.~@:+='),
  minLength: 1,
  maxLength: 16,
});

const word = fc.string({ unit: chars('abcdefghijklmnopqrstuvwxyz'), minLength: 1, maxLength: 8 });

/** Free text: no `@` or `:`, so it never looks like a URL with userinfo. */
const filler = fc.string({ unit: chars('abcdefghijklmnopqrstuvwxyz0123456789 ,.'), maxLength: 20 });

/**
 * A realistic secret, at least 8 characters: a shorter one can match the text `inspect` adds
 * around an error itself (`Error`, `[cause]`), which the library does not produce.
 */
const longSecret = fc.string({
  unit: chars('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!$%&-_.~'),
  minLength: 8,
  maxLength: 24,
});

describe('redact properties', () => {
  it('masks every known secret wherever it appears in text', () => {
    fc.assert(
      fc.property(
        fc.array(secret, { minLength: 1, maxLength: 3 }),
        fc.array(filler),
        (secrets, parts) => {
          // Every secret, at least once, between arbitrary text.
          const text =
            parts.reduce((acc, part, i) => acc + part + secrets[i % secrets.length], '') +
            secrets.join(' ');
          const out = redactText(text, secrets);
          for (const s of secrets) expect(out).not.toContain(s);
        },
      ),
    );
  });

  it('masks a secret whole when a shorter secret is part of it', () => {
    fc.assert(
      fc.property(secret, secret, (short, tail) => {
        const long = `${short}${tail}`;
        // The shorter one first: masking it inside the longer one would leave `tail` visible.
        // `[` and `]` are not in the secrets' alphabet, so they frame the one occurrence.
        expect(redactText(`[${long}]`, [short, long])).toBe('[***]');
      }),
    );
  });

  it('masks secrets in a thrown value that is not an Error, and in such a cause', () => {
    fc.assert(
      fc.property(longSecret, (s) => {
        expect(redactError(`plain ${s}`, [s]).message).not.toContain(s);
        const safe = redactError(new Error('outer', { cause: `inner ${s}` }), [s]);
        expect(inspect(safe, { depth: 10 })).not.toContain(s);
      }),
    );
  });

  it('leaves text without secrets or credentials as it is', () => {
    fc.assert(
      fc.property(filler, fc.array(secret, { maxLength: 3 }), (text, secrets) => {
        fc.pre(secrets.every((s) => !text.includes(s)));
        expect(redactText(text, secrets)).toBe(text);
      }),
    );
  });

  it('masks the userinfo of a server URL, and finds its password as a secret', () => {
    fc.assert(
      fc.property(
        word,
        password,
        word,
        fc.integer({ min: 1, max: 65535 }),
        (user, pass, host, port) => {
          const url = `nats://${user}:${pass}@${host}:${port}`;
          expect(redactServers(url)).toEqual([`nats://***@${host}:${port}`]);
          expect(serverSecrets(url)).toEqual([pass]);
          const out = redactText(`failed to connect to ${url} (${pass})`, serverSecrets(url));
          expect(out).not.toContain(pass);
        },
      ),
    );
  });

  it('masks secrets in the message, stack and causes of an error', () => {
    fc.assert(
      fc.property(longSecret, fc.integer({ min: 0, max: 4 }), (s, depth) => {
        let err: Error = new Error(`innermost ${s}`);
        for (let i = 0; i < depth; i++) err = new Error(`level ${i} ${s}`, { cause: err });
        const safe = redactError(err, [s]);
        expect(inspect(safe, { depth: 10 })).not.toContain(s);
        for (let e: unknown = safe; e instanceof Error; e = e.cause) {
          expect(e.message).not.toContain(s);
          expect(e.stack ?? '').not.toContain(s);
        }
      }),
    );
  });
});
