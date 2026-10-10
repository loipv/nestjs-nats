import { describe, expect, it } from 'vitest';
import type { ConnectionRegistry } from '../../src/connection/connection-registry.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import { validateHandlers } from '../../src/discovery/handler-validator.js';
import { NatsConfigError } from '../../src/errors/errors.js';

const mgr = {
  has: (name: string) => name === 'default',
  get: () => ({ options: {} }),
} as unknown as ConnectionRegistry;

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

function pushDef(stream: string, durable: string, o: Opts = {}): HandlerDefinition {
  return {
    ...base(o),
    kind: 'consume',
    delivery: 'push',
    options: { stream, durable, ...o.options },
  } as never;
}

function orderedDef(o: Opts = {}): HandlerDefinition {
  return {
    ...base(o),
    kind: 'ordered',
    options: { stream: 'S', ...o.options },
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

  it.each([
    ['negative maxRetries', { maxRetries: -1 }],
    ['fractional maxRetries', { maxRetries: 1.5 }],
    ['negative initialDelay', { initialDelay: -1 }],
    ['factor < 1', { factor: 0.5 }],
    ['maxDelay < initialDelay', { initialDelay: 10, maxDelay: 5 }],
    ['jitter > 1', { jitter: 2 }],
    ['negative jitter', { jitter: -0.1 }],
    ['infinite initialDelay', { initialDelay: Number.POSITIVE_INFINITY }],
    ['infinite maxDelay', { maxDelay: Number.POSITIVE_INFINITY }],
    ['maxDelay above the timer limit', { maxDelay: 2 ** 31 }],
    ['initialDelay above the timer limit', { initialDelay: 2 ** 31, maxDelay: 2 ** 31 }],
  ])('rejects retry with %s', (_, retry) => {
    expect(() => validateHandlers([def({ options: { retry } })], mgr)).toThrow(NatsConfigError);
    expect(() => validateHandlers([consumeDef('S', 'd', { options: { retry } })], mgr)).toThrow(
      NatsConfigError,
    );
  });

  it('accepts delays at the timer limit', () =>
    expect(() =>
      validateHandlers(
        [def({ options: { retry: { initialDelay: 2 ** 31 - 1, maxDelay: 2 ** 31 - 1 } } })],
        mgr,
      ),
    ).not.toThrow());

  it('names Class.method and the timer limit in the delay error', () =>
    expect(() =>
      validateHandlers(
        [
          def({
            className: 'Orders',
            methodName: 'onX',
            options: { retry: { maxDelay: 2 ** 31 } },
          }),
        ],
        mgr,
      ),
    ).toThrow(/Orders\.onX.*retry\.maxDelay.*2147483647/));

  describe('dlq subject', () => {
    const withModuleDlq = (dlq: unknown) =>
      ({ has: () => true, get: () => ({ options: { dlq } }) }) as unknown as ConnectionRegistry;

    it.each([
      ['an empty handler subject', mgr, { subject: '' }],
      ['a blank handler subject', mgr, { subject: '  ' }],
      ['a non-string, non-function subject', mgr, { subject: 42 }],
      ['an empty module subject', withModuleDlq({ subject: '' }), true],
      ['an empty module subject, handler unset', withModuleDlq({ subject: '' }), undefined],
    ])('rejects %s', (_, m, dlq) => {
      expect(() => validateHandlers([def({ options: { dlq } })], m)).toThrow(NatsConfigError);
      expect(() => validateHandlers([consumeDef('S', 'd', { options: { dlq } })], m)).toThrow(
        NatsConfigError,
      );
    });

    it('names Class.method in the error', () =>
      expect(() =>
        validateHandlers(
          [def({ className: 'Orders', methodName: 'onX', options: { dlq: { subject: '' } } })],
          mgr,
        ),
      ).toThrow(/Orders\.onX.*dlq\.subject/));

    it.each([
      ['a static subject', mgr, { subject: 'dlq.x' }],
      ['a function subject', mgr, { subject: () => 'dlq.x' }],
      ['the default subject', mgr, true],
      [
        'an empty module subject the handler overrides',
        withModuleDlq({ subject: '' }),
        { subject: 'ok' },
      ],
      [
        'an empty module subject on a handler with dlq: false',
        withModuleDlq({ subject: '' }),
        false,
      ],
      ['an empty module subject when DLQ is off', withModuleDlq(false), undefined],
    ])('accepts %s', (_, m, dlq) =>
      expect(() => validateHandlers([def({ options: { dlq } })], m)).not.toThrow(),
    );
  });

  it('names Class.method and the field in the retry error', () =>
    expect(() =>
      validateHandlers(
        [def({ className: 'Orders', methodName: 'onX', options: { retry: { jitter: 2 } } })],
        mgr,
      ),
    ).toThrow(/Orders\.onX.*jitter/));

  it('validates retry against module-level options', () => {
    const bad = {
      has: () => true,
      get: () => ({ options: { retry: { initialDelay: 100_000 } } }),
    } as unknown as ConnectionRegistry;
    // module initialDelay 100000 > default maxDelay 60000
    expect(() => validateHandlers([def()], bad)).toThrow(/maxDelay/);
  });

  it('accepts retry: false and valid retry options', () => {
    expect(() =>
      validateHandlers(
        [
          def({ options: { retry: false } }),
          consumeDef('S', 'd', { options: { retry: { maxRetries: 0, jitter: 1, factor: 1 } } }),
        ],
        mgr,
      ),
    ).not.toThrow();
  });

  it('validates the idempotency ttl a handler resolves to', () => {
    const withModuleTtl = (ttl: number | undefined) =>
      ({
        has: () => true,
        get: () => ({ options: { idempotency: { ttl } } }),
      }) as unknown as ConnectionRegistry;
    for (const ttl of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => validateHandlers([def({ options: { idempotency: { ttl } } })], mgr)).toThrow(
        /H\.on.*idempotency\.ttl/,
      );
      expect(() =>
        validateHandlers([consumeDef('S', 'd', { options: { idempotency: { ttl } } })], mgr),
      ).toThrow(NatsConfigError);
      expect(() =>
        validateHandlers([def({ options: { idempotency: true } })], withModuleTtl(ttl)),
      ).toThrow(/idempotency\.ttl/);
    }
    // Not enabled: an invalid module ttl is irrelevant; valid values pass.
    expect(() => validateHandlers([def()], withModuleTtl(0))).not.toThrow();
    expect(() =>
      validateHandlers([def({ options: { idempotency: { ttl: 1 } } })], withModuleTtl(0)),
    ).not.toThrow();
    expect(() =>
      validateHandlers([def({ options: { idempotency: true } })], withModuleTtl(undefined)),
    ).not.toThrow();
  });

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
    const two = {
      has: () => true,
      get: () => ({ options: {} }),
    } as unknown as ConnectionRegistry;
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

  describe('push consumers', () => {
    it('rejects a pull and a push consumer of the same durable', () =>
      expect(() => validateHandlers([consumeDef('S', 'D'), pushDef('S', 'D')], mgr)).toThrow(
        /both consume durable "D"/,
      ));

    it('rejects a deliverGroup conflicting with config.deliver_group', () => {
      const run = () =>
        validateHandlers(
          [
            pushDef('S', 'D', {
              options: { deliverGroup: 'a', config: { deliver_group: 'b' } },
            }),
          ],
          mgr,
        );
      expect(run).toThrow(NatsConfigError);
      expect(run).toThrow('deliverGroup "a" conflicts with config.deliver_group "b"');
    });

    it('accepts a matching deliverGroup and config.deliver_group', () =>
      expect(() =>
        validateHandlers(
          [pushDef('S', 'D', { options: { deliverGroup: 'a', config: { deliver_group: 'a' } } })],
          mgr,
        ),
      ).not.toThrow());

    it('rejects manual ack with batch', () => {
      const run = () =>
        validateHandlers([pushDef('S', 'D', { options: { ack: 'manual', batch: {} } })], mgr);
      expect(run).toThrow(NatsConfigError);
      expect(run).toThrow("combines batch with ack: 'manual'");
    });

    it('validates retry and concurrency like a pull consumer', () => {
      expect(() =>
        validateHandlers([pushDef('S', 'D', { options: { retry: { maxRetries: -1 } } })], mgr),
      ).toThrow(NatsConfigError);
      expect(() =>
        validateHandlers([pushDef('S', 'D', { options: { concurrency: 0 } })], mgr),
      ).toThrow(NatsConfigError);
    });
  });

  describe('ordered consumers', () => {
    it('accepts a minimal ordered consumer', () =>
      expect(() => validateHandlers([orderedDef()], mgr)).not.toThrow());

    it('does not treat two ordered consumers of a stream as a duplicate durable', () =>
      expect(() =>
        validateHandlers([orderedDef(), orderedDef({ methodName: 'other' })], mgr),
      ).not.toThrow());

    it.each([
      ['all', 'all'],
      ['last', 'last'],
      ['new', 'new'],
      ['last_per_subject', 'last_per_subject'],
      ['startSeq', { startSeq: 5 }],
      ['startTime string', { startTime: '2026-01-01T00:00:00Z' }],
      ['startTime Date', { startTime: new Date() }],
    ])('accepts deliverPolicy %s', (_, deliverPolicy) =>
      expect(() =>
        validateHandlers([orderedDef({ options: { deliverPolicy } })], mgr),
      ).not.toThrow(),
    );

    it.each([
      ['startSeq 0', { startSeq: 0 }, 'invalid deliverPolicy.startSeq 0; expected an integer >= 1'],
      ['fractional startSeq', { startSeq: 1.5 }, 'invalid deliverPolicy.startSeq 1.5'],
      ['bad startTime', { startTime: 'nope' }, 'invalid deliverPolicy.startTime'],
      ['invalid Date', { startTime: new Date('nope') }, 'invalid deliverPolicy.startTime'],
      ['unknown policy', 'sometimes', 'invalid deliverPolicy "sometimes"'],
      [
        'both startSeq and startTime',
        { startSeq: 5, startTime: '2026-01-01T00:00:00Z' },
        'invalid deliverPolicy; expected only one of startSeq or startTime',
      ],
    ])('rejects deliverPolicy with %s', (_, deliverPolicy, message) => {
      const run = () => validateHandlers([orderedDef({ options: { deliverPolicy } })], mgr);
      expect(run).toThrow(NatsConfigError);
      expect(run).toThrow(message);
    });

    it.each([
      ['an empty stream', { stream: '' }, 'invalid stream ""; expected a non-empty string'],
      ['a missing stream', { stream: undefined }, 'invalid stream undefined'],
      [
        'an empty filter subject',
        { filterSubjects: '' },
        'invalid filterSubjects ""; expected a non-empty string or an array of non-empty strings',
      ],
      [
        'an empty filter subject in an array',
        { filterSubjects: ['a', ''] },
        'invalid filterSubjects ["a",""]',
      ],
      [
        'a filter subject that is not a string',
        { filterSubjects: [1] },
        'invalid filterSubjects [1]',
      ],
    ])('rejects %s', (_, options, message) => {
      const run = () => validateHandlers([orderedDef({ options: options as never })], mgr);
      expect(run).toThrow(NatsConfigError);
      expect(run).toThrow(message);
    });

    it.each([
      ['a filter subject', 'S.a'],
      ['filter subjects', ['S.a', 'S.b']],
      ['no filter subjects', []],
    ])('accepts %s', (_, filterSubjects) =>
      expect(() =>
        validateHandlers([orderedDef({ options: { filterSubjects } })], mgr),
      ).not.toThrow(),
    );

    it.each([
      ['concurrency', 2],
      ['durable', 'D'],
      ['batch', {}],
      ['ack', 'manual'],
      ['config', {}],
    ])('rejects %s', (field, value) => {
      const run = () =>
        validateHandlers([orderedDef({ options: { [field]: value as never } })], mgr);
      expect(run).toThrow(NatsConfigError);
      expect(run).toThrow(`@ConsumeOrdered does not support ${field}`);
    });

    it('validates retry, dlq and idempotency', () => {
      expect(() =>
        validateHandlers([orderedDef({ options: { retry: { maxRetries: -1 } } })], mgr),
      ).toThrow(/invalid retry\.maxRetries/);
      expect(() =>
        validateHandlers([orderedDef({ options: { dlq: { subject: '' } } })], mgr),
      ).toThrow(/dlq\.subject/);
      expect(() =>
        validateHandlers([orderedDef({ options: { idempotency: { ttl: 0 } } })], mgr),
      ).toThrow(/idempotency\.ttl/);
    });

    it('still checks the connection', () =>
      expect(() => validateHandlers([orderedDef({ connection: 'x' })], mgr)).toThrow(
        /unknown NATS connection "x"/,
      ));
  });
});
