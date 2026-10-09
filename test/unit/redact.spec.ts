import { describe, expect, it } from 'vitest';
import { redactServers, redactText } from '../../src/connection/redact.js';

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
});
