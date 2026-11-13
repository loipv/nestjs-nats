/** Resolves after the pending I/O callbacks and microtasks have run (one `setImmediate`). */
export const tick = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

/** A promise that never settles. */
export const never = (): Promise<never> => new Promise<never>(() => {});
