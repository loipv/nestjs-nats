/** Whether `n` is 1, 2, 4, 8…: logging on these counts only spaces repeated lines exponentially. */
export const isPowerOfTwo = (n: number): boolean => n >= 1 && (n & (n - 1)) === 0;
