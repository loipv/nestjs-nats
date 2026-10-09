import { describe, expect, it } from 'vitest';
import type { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import { validateHandlers } from '../../src/discovery/handler-validator.js';
import { NatsConfigError } from '../../src/errors/errors.js';

const mgr = { has: (name: string) => name === 'default' } as unknown as NatsConnectionManager;

interface Opts {
  connection?: string;
  className?: string;
  methodName?: string;
  requestScoped?: boolean;
  transient?: boolean;
  options?: Record<string, unknown>;
}

function wrapper(o: Opts) {
  return { isDependencyTreeStatic: () => !o.requestScoped, isTransient: !!o.transient };
}

function base(o: Opts) {
  return {
    connection: o.connection ?? 'default',
    className: o.className ?? 'H',
    methodName: o.methodName ?? 'on',
    instance: {},
    wrapper: wrapper(o),
    invoke: async () => undefined,
  };
}

function def(o: Opts = {}): HandlerDefinition {
  return { ...base(o), kind: 'subscribe', subject: 'a', options: o.options ?? {} } as never;
}

function consumeDef(stream: string, durable: string, o: Opts = {}): HandlerDefinition {
  return {
    ...base(o),
    kind: 'consume',
    options: { stream, durable, ...o.options },
  } as never;
}

function endpointDef(service: unknown, o: Opts = {}): HandlerDefinition {
  return {
    ...base(o),
    kind: 'service-endpoint',
    endpoint: 'ep',
    options: {},
    service,
  } as never;
}

const toArray = (d: HandlerDefinition | HandlerDefinition[]) => (Array.isArray(d) ? d : [d]);

describe('validateHandlers', () => {
  it.each([
    ['unknown connection', def({ connection: 'x' })],
    ['duplicate durable', [consumeDef('S', 'd'), consumeDef('S', 'd')]],
    ['request scoped provider', def({ requestScoped: true })],
    ['transient provider', def({ transient: true })],
    ['endpoint without @NatsService', endpointDef(undefined)],
    ['concurrency 0', def({ options: { concurrency: 0 } })],
    ['consume concurrency 0', consumeDef('S', 'd', { options: { concurrency: 0 } })],
  ])('rejects %s', (_, defs) =>
    expect(() => validateHandlers(toArray(defs), mgr)).toThrow(NatsConfigError),
  );

  it('accepts valid handlers', () => {
    expect(() =>
      validateHandlers(
        [
          def({ options: { concurrency: 4 } }),
          consumeDef('S', 'd1'),
          consumeDef('S', 'd2'),
          consumeDef('T', 'd1'),
          endpointDef({ name: 'svc', version: '1.0.0' }),
        ],
        mgr,
      ),
    ).not.toThrow();
  });

  it('allows the same stream + durable on different connections', () => {
    const two = { has: () => true } as unknown as NatsConnectionManager;
    expect(() =>
      validateHandlers([consumeDef('S', 'd'), consumeDef('S', 'd', { connection: 'b' })], two),
    ).not.toThrow();
  });

  it.each([
    ['unknown connection', [def({ connection: 'x', className: 'Orders', methodName: 'onX' })]],
    ['request scoped', [def({ requestScoped: true, className: 'Orders', methodName: 'onX' })]],
    ['concurrency', [def({ options: { concurrency: 0 }, className: 'Orders', methodName: 'onX' })]],
    ['orphan endpoint', [endpointDef(undefined, { className: 'Orders', methodName: 'onX' })]],
  ])('names Class.method in the %s error', (_, defs) =>
    expect(() => validateHandlers(defs, mgr)).toThrow(/Orders\.onX/),
  );

  it('names both handlers in the duplicate durable error', () =>
    expect(() =>
      validateHandlers(
        [
          consumeDef('S', 'd', { className: 'A', methodName: 'one' }),
          consumeDef('S', 'd', { className: 'B', methodName: 'two' }),
        ],
        mgr,
      ),
    ).toThrow(/A\.one.*B\.two|B\.two.*A\.one/));
});
