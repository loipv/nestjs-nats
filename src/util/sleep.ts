/**
 * Waits `ms` milliseconds unless `signal` aborts first. Resolves `true` when the time is
 * up, `false` when aborted (at once when `signal` is already aborted); never rejects. The
 * timer does not keep the process alive.
 */
export function sleep(ms: number, signal?: AbortSignal): Promise<boolean> {
  if (signal?.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(timer);
      resolve(false);
    };
    const timer = setTimeout(
      () => {
        signal?.removeEventListener('abort', onAbort);
        resolve(true);
      },
      Math.max(0, ms),
    );
    timer.unref();
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
