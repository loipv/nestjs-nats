import { GenericContainer, Wait } from 'testcontainers';

export interface NatsTestServer {
  url: string;
  stop(): Promise<void>;
}

/** Starts a NATS server with JetStream; `config` is the content of a server config file. */
export async function startNats(
  args: string[] = [],
  { config }: { config?: string } = {},
): Promise<NatsTestServer> {
  const configArgs = config !== undefined ? ['-c', '/etc/nats/test.conf'] : [];
  let image = new GenericContainer('nats:2.12-alpine')
    .withCommand(['-js', ...configArgs, ...args])
    .withExposedPorts(4222)
    .withWaitStrategy(Wait.forLogMessage(/Server is ready/));
  if (config !== undefined) {
    image = image.withCopyContentToContainer([{ content: config, target: '/etc/nats/test.conf' }]);
  }
  const container = await image.start();
  return {
    url: `nats://${container.getHost()}:${container.getMappedPort(4222)}`,
    stop: async () => {
      await container.stop();
    },
  };
}
