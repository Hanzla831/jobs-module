export interface BackoffOptions {
  baseMs?: number;
  maxMs?: number;
  /** Injectable so retry timing is assertable in tests. */
  random?: () => number;
}

/**
 * Exponential backoff with **full** jitter: the delay is a uniform point in
 * `[0, exponential)` rather than `exponential` plus noise.
 *
 * Without jitter, every job failed by one outage retries at the same instant,
 * and the recovering dependency is hit by exactly the thundering herd that
 * knocked it over. Full jitter spreads them evenly and is the variant AWS
 * measured as best for total completion time.
 */
export function backoffDelay(attempt: number, options: BackoffOptions = {}): number {
  const base = options.baseMs ?? 1_000;
  const max = options.maxMs ?? 5 * 60_000;
  const random = options.random ?? Math.random;

  const exponential = Math.min(max, base * 2 ** Math.max(0, attempt - 1));
  return Math.floor(random() * exponential);
}
