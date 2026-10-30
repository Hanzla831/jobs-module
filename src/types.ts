/**
 * Core domain types for the job queue.
 *
 * The delivery guarantee is **at-least-once**, and that is a deliberate choice
 * rather than a limitation. Exactly-once across a queue and a separate database
 * is not achievable without distributed transactions; pretending otherwise is
 * how systems end up double-charging. So the contract is stated plainly:
 * handlers must be idempotent, and the queue gives them the tools —
 * deduplication keys on the way in, and a stable job id on the way out.
 */

export type JobState =
  | 'waiting'
  | 'active'
  | 'completed'
  | 'failed'
  | 'dead'
  | 'cancelled';

/** Higher runs first. A backlog of reports must not delay a password reset. */
export type JobPriority = 'low' | 'normal' | 'high' | 'critical';

export const PRIORITY_ORDER: Record<JobPriority, number> = {
  critical: 3,
  high: 2,
  normal: 1,
  low: 0,
};

export interface EnqueueOptions {
  priority?: JobPriority;
  /** Earliest run time, as a delay in milliseconds. */
  delayMs?: number;
  /** Absolute run time; takes precedence over `delayMs`. */
  runAt?: Date;
  maxAttempts?: number;
  /**
   * Collapses duplicate work. While a job with this key is waiting or active,
   * another enqueue with the same key is ignored and the existing job id is
   * returned. This is what makes a retried API call safe.
   */
  dedupeKey?: string;
  /** Hard limit on a single execution; a hung handler must not hold its lease. */
  timeoutMs?: number;
  /** Carried through to logs and the admin API. */
  metadata?: Record<string, string>;
}

export interface Job<TPayload = unknown> {
  id: string;
  /** Handler name. A job whose handler is gone must not silently vanish. */
  name: string;
  payload: TPayload;
  state: JobState;
  priority: JobPriority;
  attempts: number;
  maxAttempts: number;
  /** Not claimable before this instant — delays, schedules and backoff. */
  runAt: Date;
  timeoutMs: number;
  dedupeKey: string | null;
  metadata: Record<string, string>;
  /**
   * While active, the instant the lease expires. A worker that dies mid-job
   * stops heartbeating, the lease lapses, and another worker picks the job up
   * rather than it being stranded forever.
   */
  leaseExpiresAt: Date | null;
  lockedBy: string | null;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
  completedAt: Date | null;
}

/** What a handler receives. `attempt` lets a handler behave differently on a retry. */
export interface JobContext<TPayload = unknown> {
  job: Job<TPayload>;
  attempt: number;
  /** Extends the lease; call from a long-running handler. */
  heartbeat: () => Promise<void>;
  /** Flips to true during shutdown so a long handler can stop early. */
  readonly shuttingDown: boolean;
}

export type JobHandler<TPayload = unknown> = (
  payload: TPayload,
  context: JobContext<TPayload>,
) => Promise<void> | void;

export interface QueueStats {
  waiting: number;
  active: number;
  dead: number;
  completed: number;
  /** Oldest waiting job age in ms — the number worth alerting on. */
  oldestWaitingMs: number;
}

export type Clock = () => Date;
export const systemClock: Clock = () => new Date();

export const DEFAULT_MAX_ATTEMPTS = 5;
export const DEFAULT_TIMEOUT_MS = 30_000;
export const DEFAULT_LEASE_MS = 30_000;
