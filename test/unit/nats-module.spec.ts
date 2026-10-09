import { describe, expect, it } from 'vitest';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';

describe('NatsModule.forRootAsync', () => {
  it('rejects options without useFactory, useClass or useExisting', () =>
    expect(() => NatsModule.forRootAsync({})).toThrow(NatsConfigError));
});
