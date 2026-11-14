import { describe, expect, it } from 'vitest';
import {
  redactError,
  redactServers,
  redactText,
  serverSecrets,
} from '../../../src/connection/redact.js';

describe('redactServers', () => {
  it('redacts userinfo', () =>
    expect(redactServers('nats://u:secret@h:4222')).toEqual(['nats://***@h:4222']));
  it('keeps plain urls', () => expect(redactServers(['nats://h:1'])).toEqual(['nats://h:1']));
  it('defaults when undefined', () =>
    expect(redactServers(undefined)).toEqual(['nats://127.0.0.1:4222']));
  it('redacts token-only userinfo and schemeless urls', () => {
    expect(redactServers(['nats://tok@h:1', 'u:p@h:2'])).toEqual(['nats://***@h:1', '***@h:2']);
  });
  it('handles passwords containing @', () =>
    expect(redactServers('nats://u:p@ss@h:4222')).toEqual(['nats://***@h:4222']));
});

describe('redactText', () => {
  it('masks url userinfo and known secrets', () => {
    expect(redactText('failed nats://u:p@h:1 with pw hunter2', ['hunter2', undefined])).toBe(
      'failed nats://***@h:1 with pw ***',
    );
  });

  it('masks a secret whole when a shorter secret is part of it', () => {
    expect(redactText('pw is pw-tok-s3cret', ['tok-s3cret', 'pw-tok-s3cret'])).toBe('pw is ***');
  });
});

describe('serverSecrets', () => {
  it('returns passwords and bare tokens of server URLs', () => {
    expect(serverSecrets(['nats://u:p@ss@h:1', 'tok@h:2', 'nats://h:3', 'nats://u:@h:4'])).toEqual([
      'p@ss',
      'tok',
    ]);
    expect(serverSecrets(undefined)).toEqual([]);
  });
});

describe('redactError', () => {
  it('keeps name, code, message and stack masked and drops other own properties', () => {
    const raw = Object.assign(new TypeError('bad nats://u:pw1@h:1 hunter2'), {
      code: 'ERR_X',
      input: 'nats://u:pw1@h:1',
    });
    const safe = redactError(raw, ['hunter2']);
    expect(safe).not.toBe(raw);
    expect(safe.name).toBe('TypeError');
    expect(safe.message).toBe('bad nats://***@h:1 ***');
    expect((safe as Error & { code?: string }).code).toBe('ERR_X');
    expect(Object.hasOwn(safe, 'input')).toBe(false);
    expect(safe.stack).not.toMatch(/pw1|hunter2/);
  });

  it('sanitizes the cause chain and non-Error values', () => {
    const inner = Object.assign(new Error('inner hunter2'), { input: 'hunter2' });
    const safe = redactError(new Error('outer', { cause: inner }), ['hunter2']);
    expect(safe.cause).toBeInstanceOf(Error);
    expect((safe.cause as Error).message).toBe('inner ***');
    expect(Object.hasOwn(safe.cause as Error, 'input')).toBe(false);
    expect(redactError('plain hunter2', ['hunter2']).message).toBe('plain ***');
  });
});
