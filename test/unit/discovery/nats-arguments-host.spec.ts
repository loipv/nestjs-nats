import { describe, expect, it } from 'vitest';
import { NatsArgumentsHost } from '../../../src/discovery/nats-arguments-host.js';

describe('NatsArgumentsHost', () => {
  const payload = { id: 1 };
  const ctx = { subject: 'orders.created' };
  const host = new NatsArgumentsHost([payload, ctx]);

  it("is of type 'nats'", () => {
    expect(host.getType()).toBe('nats');
  });

  it('exposes the [payload, context] arguments', () => {
    expect(host.getArgs()).toEqual([payload, ctx]);
    expect(host.getArgByIndex(0)).toBe(payload);
    expect(host.getArgByIndex(1)).toBe(ctx);
    expect(host.getArgByIndex(2)).toBeUndefined();
  });

  it('switches to RPC as the payload and the context', () => {
    const rpc = host.switchToRpc();
    expect(rpc.getData()).toBe(payload);
    expect(rpc.getContext()).toBe(ctx);
  });

  it('switches to HTTP and WebSockets by argument position, as Nest hosts do', () => {
    const http = host.switchToHttp();
    expect(http.getRequest()).toBe(payload);
    expect(http.getResponse()).toBe(ctx);
    expect(http.getNext()).toBeUndefined();
    const ws = host.switchToWs();
    expect(ws.getClient()).toBe(payload);
    expect(ws.getData()).toBe(ctx);
    expect(ws.getPattern()).toBe(ctx);
  });
});
