import { randomUUID } from 'node:crypto';
import { InvalidJobError, JobNotFoundError } from './errors.js';
import {
  DEFAULT_LEASE_MS,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_TIMEOUT_MS,
  PRIORITY_ORDER,
  systemClock,
  type Clock,
  type EnqueueOptions,
  type Job,
  type QueueStats,
} from './types.js';

/**
 * Storage boundary for jobs.
 *
 * Everything above this interface is storage-agnostic, so the in-memory
 * implementation below swaps for Postgres (`SELECT … FOR UPDATE SKIP LOCKED`),
 * Redis or SQS without touching the worker or the scheduler. The method set is
 * deliberately shaped like a real broker — leases, heartbeats and a dead-letter
 * store — so that swap stays a one-file change.
 */
export interface JobQueue {
  enqueue<T>(name: string, payload: T, options?: EnqueueOptions): Promise<Job<T>>;
  /** Claims up to `limit` due jobs and leases them to `workerId`. */
  claim(workerId: string, limit: number, leaseMs?: number): Promise<Job[]>;
  /** Extends the lease of an in-flight job. */
  heartbeat(jobId: string, workerId: string, leaseMs?: number): Promise<boolean>;
  complete(jobId: string): Promise<void>;
  /** Schedules another attempt. */
  retry(jobId: string, runAt: Date, error: string): Promise<void>;
  deadLetter(jobId: string, error: string): Promise<void>;
  /** Returns a dead job to the queue, attempts reset. */
  replay(jobId: string): Promise<Job>;
  cancel(jobId: string): Promise<boolean>;
  find(jobId: string): Promise<Job | null>;
  list(state?: Job['state'], limit?: number): Promise<Job[]>;
  stats(): Promise<QueueStats>;
  /** Frees jobs whose worker died without releasing the lease. */
  reclaimExpired(): Promise<number>;
}

export interface InMemoryJobQueueOptions {
  clock?: Clock;
  defaultLeaseMs?: number;
  /** Completed jobs retained for inspection; older ones are trimmed. */
  completedHistory?: number;
}

const clone = <T>(job: Job<T>): Job<T> => ({ ...job, metadata: { ...job.metadata } });

export class InMemoryJobQueue implements JobQueue {
  private readonly jobs = new Map<string, Job>();
  private readonly byDedupeKey = new Map<string, string>();
  private readonly completed: string[] = [];
  private readonly clock: Clock;
  private readonly defaultLeaseMs: number;
  private readonly completedHistory: number;

  constructor(options: InMemoryJobQueueOptions = {}) {
    this.clock = options.clock ?? systemClock;
    this.defaultLeaseMs = options.defaultLeaseMs ?? DEFAULT_LEASE_MS;
    this.completedHistory = options.completedHistory ?? 100;
  }

  async enqueue<T>(name: string, payload: T, options: EnqueueOptions = {}): Promise<Job<T>> {
    if (!name.trim()) throw new InvalidJobError('Job name is required');

    const now = this.clock();

    // Deduplication collapses retried API calls and duplicate webhooks into
    // one unit of work. Only pending jobs count: once a job has finished, the
    // same key legitimately describes new work.
    if (options.dedupeKey) {
      const existingId = this.byDedupeKey.get(options.dedupeKey);
      const existing = existingId ? this.jobs.get(existingId) : undefined;
      if (existing && (existing.state === 'waiting' || existing.state === 'active')) {
        return clone(existing) as Job<T>;
      }
    }

    const runAt =
      options.runAt ?? (options.delayMs ? new Date(now.getTime() + options.delayMs) : now);

    const job: Job<T> = {
      id: randomUUID(),
      name,
      payload,
      state: 'waiting',
      priority: options.priority ?? 'normal',
      attempts: 0,
      maxAttempts: options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
      runAt,
      timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      dedupeKey: options.dedupeKey ?? null,
      metadata: options.metadata ?? {},
      leaseExpiresAt: null,
      lockedBy: null,
      lastError: null,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
    };

    this.jobs.set(job.id, job as Job);
    if (job.dedupeKey) this.byDedupeKey.set(job.dedupeKey, job.id);
    return clone(job);
  }

  /**
   * Claims due work, highest priority first and oldest within a priority.
   *
   * Expired leases are reclaimed first, so a job whose worker was killed
   * mid-execution re-enters the pool instead of being stranded.
   */
  async claim(workerId: string, limit: number, leaseMs?: number): Promise<Job[]> {
    await this.reclaimExpired();

    const now = this.clock();
    const lease = leaseMs ?? this.defaultLeaseMs;

    const due = [...this.jobs.values()]
      .filter((job) => job.state === 'waiting' && job.runAt.getTime() <= now.getTime())
      .sort((a, b) => {
        const byPriority = PRIORITY_ORDER[b.priority] - PRIORITY_ORDER[a.priority];
        return byPriority !== 0 ? byPriority : a.createdAt.getTime() - b.createdAt.getTime();
      })
      .slice(0, Math.max(0, limit));

    for (const job of due) {
      job.state = 'active';
      job.attempts += 1;
      job.lockedBy = workerId;
      job.leaseExpiresAt = new Date(now.getTime() + lease);
      job.updatedAt = now;
    }
    return due.map(clone);
  }

  /** Only the lease holder may extend it, so a stale worker cannot resurrect a job. */
  async heartbeat(jobId: string, workerId: string, leaseMs?: number): Promise<boolean> {
    const job = this.jobs.get(jobId);
    if (!job || job.state !== 'active' || job.lockedBy !== workerId) return false;

    job.leaseExpiresAt = new Date(this.clock().getTime() + (leaseMs ?? this.defaultLeaseMs));
    job.updatedAt = this.clock();
    return true;
  }

  async complete(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.state = 'completed';
    job.leaseExpiresAt = null;
    job.lockedBy = null;
    job.completedAt = this.clock();
    job.updatedAt = job.completedAt;
    // The key is released here, not at enqueue: the same key may describe new
    // work once this one is finished.
    if (job.dedupeKey) this.byDedupeKey.delete(job.dedupeKey);

    this.completed.push(job.id);
    while (this.completed.length > this.completedHistory) {
      const oldest = this.completed.shift();
      if (oldest) this.jobs.delete(oldest);
    }
  }

  async retry(jobId: string, runAt: Date, error: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.state = 'waiting';
    job.runAt = runAt;
    job.lastError = error;
    job.leaseExpiresAt = null;
    job.lockedBy = null;
    job.updatedAt = this.clock();
  }

  async deadLetter(jobId: string, error: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.state = 'dead';
    job.lastError = error;
    job.leaseExpiresAt = null;
    job.lockedBy = null;
    job.updatedAt = this.clock();
    if (job.dedupeKey) this.byDedupeKey.delete(job.dedupeKey);
  }

  /** Operator action after fixing the cause — the point of keeping dead jobs. */
  async replay(jobId: string): Promise<Job> {
    const job = this.jobs.get(jobId);
    if (!job) throw new JobNotFoundError(jobId);
    if (job.state !== 'dead') {
      throw new InvalidJobError(`Only dead jobs can be replayed (state: ${job.state})`);
    }

    job.state = 'waiting';
    job.attempts = 0;
    job.runAt = this.clock();
    job.lastError = null;
    job.updatedAt = this.clock();
    if (job.dedupeKey) this.byDedupeKey.set(job.dedupeKey, job.id);
    return clone(job);
  }

  async cancel(jobId: string): Promise<boolean> {
    const job = this.jobs.get(jobId);
    // An active job cannot be cancelled from here; it holds a lease and is
    // mid-execution. Cancelling it would need cooperative interruption.
    if (!job || job.state !== 'waiting') return false;

    job.state = 'cancelled';
    job.updatedAt = this.clock();
    if (job.dedupeKey) this.byDedupeKey.delete(job.dedupeKey);
    return true;
  }

  async find(jobId: string): Promise<Job | null> {
    const job = this.jobs.get(jobId);
    return job ? clone(job) : null;
  }

  async list(state?: Job['state'], limit = 50): Promise<Job[]> {
    return [...this.jobs.values()]
      .filter((job) => !state || job.state === state)
      .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())
      .slice(0, limit)
      .map(clone);
  }

  async stats(): Promise<QueueStats> {
    const now = this.clock().getTime();
    let waiting = 0;
    let active = 0;
    let dead = 0;
    let completed = 0;
    let oldestWaiting = now;

    for (const job of this.jobs.values()) {
      if (job.state === 'waiting') {
        waiting += 1;
        oldestWaiting = Math.min(oldestWaiting, job.createdAt.getTime());
      } else if (job.state === 'active') active += 1;
      else if (job.state === 'dead') dead += 1;
      else if (job.state === 'completed') completed += 1;
    }

    return {
      waiting,
      active,
      dead,
      completed,
      oldestWaitingMs: waiting > 0 ? now - oldestWaiting : 0,
    };
  }

  async reclaimExpired(): Promise<number> {
    const now = this.clock().getTime();
    let reclaimed = 0;

    for (const job of this.jobs.values()) {
      if (job.state === 'active' && job.leaseExpiresAt && job.leaseExpiresAt.getTime() <= now) {
        // The attempt already counted at claim time, so a worker that crashes
        // repeatedly still exhausts the budget rather than looping forever.
        job.state = 'waiting';
        job.lockedBy = null;
        job.leaseExpiresAt = null;
        job.lastError = 'Lease expired; worker presumed dead';
        job.updatedAt = this.clock();
        reclaimed += 1;
      }
    }
    return reclaimed;
  }
}
