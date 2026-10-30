import { randomUUID } from 'node:crypto';
import { backoffDelay, type BackoffOptions } from './backoff.js';
import {
  JobTimeoutError,
  PermanentJobError,
  RetryableJobError,
  UnknownHandlerError,
} from './errors.js';
import type { JobQueue } from './queue.js';
import {
  systemClock,
  type Clock,
  type Job,
  type JobContext,
  type JobHandler,
} from './types.js';

export interface WorkerOptions {
  /** Jobs executed simultaneously. Bounds database connections and memory. */
  concurrency?: number;
  /** Jobs claimed per poll. Larger batches mean fewer round trips. */
  batchSize?: number;
  leaseMs?: number;
  backoff?: BackoffOptions;
  clock?: Clock;
  workerId?: string;
  onCompleted?: (job: Job) => void;
  onFailed?: (job: Job, error: Error) => void;
  onDead?: (job: Job, error: Error) => void;
}

export interface TickResult {
  claimed: number;
  completed: number;
  retried: number;
  dead: number;
}

/**
 * Executes jobs from the queue.
 *
 * `runOnce` is a single pass rather than an internal loop, so the caller owns
 * the cadence — a timer in production, a direct call in a test. That is what
 * makes the retry, timeout and shutdown behaviour testable without waiting on
 * real time.
 */
export class Worker {
  private readonly handlers = new Map<string, JobHandler<never>>();
  private readonly concurrency: number;
  private readonly batchSize: number;
  private readonly leaseMs: number;
  private readonly backoffOptions: BackoffOptions;
  private readonly clock: Clock;
  readonly id: string;

  private inFlight = 0;
  private stopping = false;

  constructor(
    private readonly queue: JobQueue,
    private readonly options: WorkerOptions = {},
  ) {
    this.concurrency = options.concurrency ?? 5;
    this.batchSize = options.batchSize ?? 10;
    this.leaseMs = options.leaseMs ?? 30_000;
    this.backoffOptions = options.backoff ?? {};
    this.clock = options.clock ?? systemClock;
    this.id = options.workerId ?? `worker-${randomUUID().slice(0, 8)}`;
  }

  /** Registers a handler. Typed per job so payloads stay checked at the edge. */
  register<TPayload>(name: string, handler: JobHandler<TPayload>): this {
    this.handlers.set(name, handler as JobHandler<never>);
    return this;
  }

  get registered(): string[] {
    return [...this.handlers.keys()];
  }

  get isShuttingDown(): boolean {
    return this.stopping;
  }

  /** Claims and runs one batch, respecting the concurrency budget. */
  async runOnce(): Promise<TickResult> {
    const result: TickResult = { claimed: 0, completed: 0, retried: 0, dead: 0 };
    if (this.stopping) return result;

    const capacity = Math.min(this.batchSize, this.concurrency - this.inFlight);
    if (capacity <= 0) return result;

    const jobs = await this.queue.claim(this.id, capacity, this.leaseMs);
    result.claimed = jobs.length;

    await Promise.all(
      jobs.map(async (job) => {
        this.inFlight += 1;
        try {
          const outcome = await this.execute(job);
          result[outcome] += 1;
        } finally {
          this.inFlight -= 1;
        }
      }),
    );

    return result;
  }

  /**
   * Stops claiming and waits for in-flight work to finish.
   *
   * Exiting while jobs are mid-flight would strand them until their leases
   * expire, delaying every retry by the full lease window. Handlers can see
   * `shuttingDown` and bail out early.
   */
  async shutdown(timeoutMs = 30_000): Promise<boolean> {
    this.stopping = true;
    const deadline = Date.now() + timeoutMs;

    while (this.inFlight > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    return this.inFlight === 0;
  }

  private async execute(job: Job): Promise<'completed' | 'retried' | 'dead'> {
    const handler = this.handlers.get(job.name);

    // No handler means a deploy removed it while jobs were still queued.
    // Retrying cannot help, so it goes straight to the dead-letter queue
    // where it survives for replay once the handler is restored.
    if (!handler) {
      const error = new UnknownHandlerError(job.name);
      await this.queue.deadLetter(job.id, error.message);
      this.options.onDead?.(job, error);
      return 'dead';
    }

    const context: JobContext = {
      job,
      attempt: job.attempts,
      heartbeat: async () => {
        await this.queue.heartbeat(job.id, this.id, this.leaseMs);
      },
      get shuttingDown() {
        return false;
      },
    };
    Object.defineProperty(context, 'shuttingDown', { get: () => this.stopping });

    try {
      await this.withTimeout(
        Promise.resolve(handler(job.payload as never, context as never)),
        job.timeoutMs,
      );
      await this.queue.complete(job.id);
      this.options.onCompleted?.(job);
      return 'completed';
    } catch (error) {
      return this.handleFailure(job, error instanceof Error ? error : new Error(String(error)));
    }
  }

  /**
   * A handler that never settles would hold its lease forever (it keeps
   * heartbeating) and silently consume a concurrency slot, so every execution
   * races a timer.
   */
  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new JobTimeoutError(timeoutMs)), timeoutMs);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async handleFailure(job: Job, error: Error): Promise<'retried' | 'dead'> {
    // A permanent failure burns no further attempts: the payload is wrong and
    // will be wrong next time, and retrying delays everything behind it.
    const permanent = error instanceof PermanentJobError;
    const exhausted = job.attempts >= job.maxAttempts;

    if (permanent || exhausted) {
      const reason = permanent ? error.message : `${error.message} (after ${job.attempts} attempts)`;
      await this.queue.deadLetter(job.id, reason);
      this.options.onDead?.(job, error);
      return 'dead';
    }

    // A dependency that tells us when to come back knows better than our curve.
    const suggested = error instanceof RetryableJobError ? error.retryAfterMs : undefined;
    const delay = suggested ?? backoffDelay(job.attempts, this.backoffOptions);

    await this.queue.retry(job.id, new Date(this.clock().getTime() + delay), error.message);
    this.options.onFailed?.(job, error);
    return 'retried';
  }
}
