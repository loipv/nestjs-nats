import type { ArgumentsHost, ContextType } from '@nestjs/common';

type RpcArgumentsHost = ReturnType<ArgumentsHost['switchToRpc']>;
type HttpArgumentsHost = ReturnType<ArgumentsHost['switchToHttp']>;
type WsArgumentsHost = ReturnType<ArgumentsHost['switchToWs']>;

/** The `ArgumentsHost` the exception filters of a handler receive: `[payload, context]`, of type `'nats'`. */
export class NatsArgumentsHost implements ArgumentsHost {
  constructor(private readonly args: unknown[]) {}

  getArgs<T extends unknown[] = unknown[]>(): T {
    return this.args as T;
  }

  getArgByIndex<T = unknown>(index: number): T {
    return this.args[index] as T;
  }

  getType<T extends string = ContextType>(): T {
    return 'nats' as T;
  }

  switchToRpc(): RpcArgumentsHost {
    return { getData: () => this.args[0], getContext: () => this.args[1] } as RpcArgumentsHost;
  }

  switchToHttp(): HttpArgumentsHost {
    return {
      getRequest: () => this.args[0],
      getResponse: () => this.args[1],
      getNext: () => this.args[2],
    } as HttpArgumentsHost;
  }

  switchToWs(): WsArgumentsHost {
    return {
      getClient: () => this.args[0],
      getData: () => this.args[1],
      getPattern: () => this.args[this.args.length - 1],
    } as WsArgumentsHost;
  }
}
