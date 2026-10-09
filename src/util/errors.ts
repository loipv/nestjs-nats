/** The message of an error, or the value as a string when it is not an `Error`. */
export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** The stack of an error (falling back to its message), for logging. */
export function errorTrace(err: unknown): string {
  return (err instanceof Error ? err.stack : undefined) ?? errorMessage(err);
}
