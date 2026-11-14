/** The longest delay `setTimeout` honours (2^31-1 ms): Node fires a longer one after 1 ms. */
export const MAX_TIMER_DELAY = 2 ** 31 - 1;

/** `ms` as a timer delay: at least 1 ms, at most `MAX_TIMER_DELAY`. */
export function timerDelay(ms: number): number {
  return Math.min(Math.max(ms, 1), MAX_TIMER_DELAY);
}
