import { GenericContainer, Wait } from 'testcontainers';

export interface NatsTestServer {
  url: string;
  stop(): Promise<void>;
}

export async function startNats(args: string[] = []): Promise<NatsTestServer> {
  const container = await new GenericContainer('nats:2.12-alpine')
    .withCommand(['-js', ...args])
    .withExposedPorts(4222)
    .withWaitStrategy(Wait.forLogMessage(/Server is ready/))
    .start();
  return {
    url: `nats://${container.getHost()}:${container.getMappedPort(4222)}`,
    stop: async () => {
      await container.stop();
    },
  };
}
