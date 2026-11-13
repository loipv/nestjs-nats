import { Test } from '@nestjs/testing';
import { firstValueFrom, Subject } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import {
  NatsConnectionManager,
  type NatsStatusEvent,
} from '../../../src/connection/connection-manager.js';
import { ConnectionRegistry } from '../../../src/connection/connection-registry.js';

describe('NatsConnectionManager', () => {
  it('answers from the injected connection registry', async () => {
    const events = new Subject<NatsStatusEvent>();
    const registry = {
      has: vi.fn((name: string) => name === 'a'),
      names: vi.fn(() => ['a']),
      status: vi.fn((name: string) => ({ name, state: 'connected', server: 'nats://h:4222' })),
      status$: vi.fn(() => events.asObservable()),
    };
    const moduleRef = await Test.createTestingModule({
      providers: [NatsConnectionManager, { provide: ConnectionRegistry, useValue: registry }],
    }).compile();
    const manager = moduleRef.get(NatsConnectionManager);

    expect(manager.has('a')).toBe(true);
    expect(manager.has('b')).toBe(false);
    expect(manager.names()).toEqual(['a']);
    expect(manager.status()).toEqual({
      name: 'default',
      state: 'connected',
      server: 'nats://h:4222',
    });
    expect(registry.status).toHaveBeenCalledWith('default');
    manager.status('a');
    expect(registry.status).toHaveBeenLastCalledWith('a');

    const next = firstValueFrom(manager.status$('a'));
    expect(registry.status$).toHaveBeenCalledWith('a');
    events.next({ connection: 'a', type: 'reconnect' });
    expect(await next).toEqual({ connection: 'a', type: 'reconnect' });
  });
});
