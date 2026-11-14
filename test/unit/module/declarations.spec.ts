import { describe, expect, it } from 'vitest';
import { NatsConfigError } from '../../../src/errors/errors.js';
import { rejectRemovedFieldsReset } from '../../../src/module/declarations.js';
import type { NatsModuleOptions } from '../../../src/module/options.js';

const options = (removedFields: unknown) =>
  ({ connection: {}, jetstream: { removedFields } }) as NatsModuleOptions;

describe('rejectRemovedFieldsReset', () => {
  it.each([undefined, 'ignore'])('accepts %s', (mode) => {
    expect(() => rejectRemovedFieldsReset('orders', options(mode))).not.toThrow();
  });

  it('accepts options without jetstream', () => {
    expect(() => rejectRemovedFieldsReset('orders', { connection: {} })).not.toThrow();
  });

  it.each([
    ['reset', '"reset"'],
    ['drop', '"drop"'],
    [true, 'true'],
  ])('rejects %s', (mode, shown) => {
    const check = () => rejectRemovedFieldsReset('orders', options(mode));
    expect(check).toThrow(NatsConfigError);
    expect(check).toThrow(
      `NATS connection "orders" sets jetstream.removedFields to ${shown}, which was removed in 0.3.0: a field removed from a declaration keeps its value on the server. Remove the option, and change such fields by hand`,
    );
  });
});
