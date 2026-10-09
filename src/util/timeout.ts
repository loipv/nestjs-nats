/**
 * Waits for `promise` to settle for at most `ms` (at least one macrotask, so an already
 * settled promise always counts). Resolves `true` when it settled in time, `false` when the
 * time ran out; never rejects (handle the promise's own rejection where it is created).
 */
export async function settlesWithin(promise: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  const expired = new Promise<false>((resolve) => {
    timer = setTimeout(() => resolve(false), Math.max(0, ms));
  });
  try {
    return await Promise.race([
      promise.then(
        () => true,
        () => true,
      ),
      expired,
    ]);
  } finally {
    clearTimeout(timer);
  }
}
