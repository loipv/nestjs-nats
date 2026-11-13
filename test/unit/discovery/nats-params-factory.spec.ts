import { headers as natsHeaders } from '@nats-io/transport-node';
import { describe, expect, it } from 'vitest';
import { NatsBatchContext } from '../../../src/context/nats-batch-context.js';
import { NatsContext } from '../../../src/context/nats-context.js';
import { NatsParamType } from '../../../src/decorators/param.decorators.js';
import { NatsParamsFactory } from '../../../src/discovery/nats-params-factory.js';

function natsCtx(h: Record<string, string> = {}, subject = 'x') {
  const hdrs = natsHeaders();
  for (const [k, v] of Object.entries(h)) hdrs.set(k, v);
  return new NatsContext({
    kind: 'core',
    connection: 'default',
    raw: { subject, reply: '', headers: hdrs } as never,
  });
}

describe('NatsParamsFactory', () => {
  const f = new NatsParamsFactory();
  const ctx = natsCtx({ a: '1' }, 'sub.j');
  const args = [{ p: 2 }, ctx];

  it.each([
    [NatsParamType.PAYLOAD, undefined, { p: 2 }],
    [NatsParamType.PAYLOAD, 'p', 2],
    [NatsParamType.HEADERS, undefined, { a: '1' }],
    [NatsParamType.HEADERS, 'a', '1'],
    [NatsParamType.SUBJECT, undefined, 'sub.j'],
    [NatsParamType.MSG, undefined, ctx.raw],
    [NatsParamType.CONTEXT, undefined, ctx],
    [NatsParamType.CLOUD_EVENT, undefined, undefined],
    [99, undefined, undefined],
  ])('type %s data %s', (type, data, expected) =>
    expect(f.exchangeKeyForValue(type, data as never, args)).toEqual(expected),
  );

  it('resolves @CloudEvent() per message of a batch', () => {
    const ce = { 'ce-specversion': '1.0', 'ce-id': '1', 'ce-source': 's', 'ce-type': 't' };
    const batch = new NatsBatchContext({
      connection: 'default',
      messages: [natsCtx(ce), natsCtx()],
      onFail: () => {},
    });
    const out = f.exchangeKeyForValue(NatsParamType.CLOUD_EVENT, undefined as never, [
      [1, 2],
      batch,
    ]);
    expect(out).toEqual([expect.objectContaining({ type: 't', id: '1' }), undefined]);
  });

  it('resolves @nestjs/microservices @Ctx() (RpcParamtype.CONTEXT = 6) to the context', () =>
    expect(f.exchangeKeyForValue(6, undefined as never, args)).toBe(ctx));

  it('tolerates a missing payload when a property is requested', () =>
    expect(f.exchangeKeyForValue(NatsParamType.PAYLOAD, 'p', [undefined, ctx])).toBeUndefined());
});
