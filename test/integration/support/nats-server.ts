import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import { eventually } from './eventually.js';

const CONFIG_PATH = '/etc/nats/test.conf';

export interface NatsTestServer {
  url: string;
  stop(): Promise<void>;
  /**
   * Replaces the config file and reloads it (`SIGHUP`), resolving once the server logged the
   * reload. Only for a server started with `config`.
   */
  reload(config: string): Promise<void>;
}

/** Starts a NATS server with JetStream; `config` is the content of a server config file. */
export async function startNats(
  args: string[] = [],
  { config }: { config?: string } = {},
): Promise<NatsTestServer> {
  const configArgs = config !== undefined ? ['-c', CONFIG_PATH] : [];
  let image = new GenericContainer('nats:2.12-alpine')
    .withCommand(['-js', ...configArgs, ...args])
    .withExposedPorts(4222)
    .withWaitStrategy(Wait.forLogMessage(/Server is ready/));
  if (config !== undefined) {
    image = image.withCopyContentToContainer([{ content: config, target: CONFIG_PATH }]);
  }
  const container = await image.start();
  return {
    url: `nats://${container.getHost()}:${container.getMappedPort(4222)}`,
    stop: async () => {
      await container.stop();
    },
    reload: async (next) => {
      if (config === undefined) throw new Error('reload() needs a server started with a config');
      await reloadConfig(container, next);
    },
  };
}

async function reloadConfig(container: StartedTestContainer, config: string): Promise<void> {
  await container.copyContentToContainer([{ content: config, target: CONFIG_PATH }]);
  // Follow only the lines logged from now on, so an earlier reload does not count.
  const logs = await container.logs({ tail: 0 });
  let text = '';
  logs.on('data', (chunk: Buffer | string) => {
    text += chunk.toString();
  });
  try {
    const { exitCode, output } = await container.exec(['kill', '-HUP', '1']);
    if (exitCode !== 0) throw new Error(`kill -HUP 1 exited with ${exitCode}: ${output}`);
    await eventually(
      () => {
        if (!text.includes('Reloaded server configuration')) {
          throw new Error(`the server did not log a reload; logged:\n${text}`);
        }
      },
      { timeout: 10_000 },
    );
  } finally {
    logs.destroy();
  }
}
