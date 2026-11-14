/** `obj` without its keys set to `undefined`: spread over defaults, they keep the default. */
export function withoutUndefined<T extends object>(obj: T): Partial<T> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined)) as Partial<T>;
}
