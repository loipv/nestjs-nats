/** Polls `check` until it returns without throwing, or rethrows its last error after `timeout`. */
export async function eventually<T>(
  check: () => T | Promise<T>,
  { timeout = 5000, interval = 20 }: { timeout?: number; interval?: number } = {},
): Promise<T> {
  const deadline = Date.now() + timeout;
  for (;;) {
    try {
      return await check();
    } catch (err) {
      if (Date.now() >= deadline) throw err;
      await new Promise((resolve) => setTimeout(resolve, interval));
    }
  }
}

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
