import { NatsModuleError } from '../errors/errors.js';
import type { Next } from './types.js';

export type { Next } from './types.js';

export function compose<T>(
  middlewares: ReadonlyArray<(ctx: T, next: Next) => Promise<void>>,
  terminal: (ctx: T) => Promise<void>,
): (ctx: T) => Promise<void> {
  return (ctx) => {
    let last = -1;
    const dispatch = async (i: number): Promise<void> => {
      if (i <= last) throw new NatsModuleError('next() called multiple times');
      last = i;
      const mw = middlewares[i];
      if (mw === undefined) return terminal(ctx);
      return mw(ctx, () => dispatch(i + 1));
    };
    return dispatch(0);
  };
}
