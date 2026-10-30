import type { JobQueue } from './queue.js';
import { InvalidJobError } from './errors.js';
import { systemClock, type Clock, type EnqueueOptions } from './types.js';

export interface RecurringJob {
  name: string;
  everyMs: number;
  payload: unknown;
  options: EnqueueOptions;
  /** Next time this should be enqueued. */
  nextRunAt: Date;
  lastEnqueuedAt: Date | null;
  /**
   * What to do when the process was down across several intervals.
   *
   * `skip` enqueues one run and jumps to the next future slot — right for a
   * cache refresh, where ten stale runs are pointless. `catchUp` enqueues one
   * job per missed interval — right for billing, where every period matters.
   */
  missedPolicy: 'skip' | 'catchUp';
}

export interface ScheduleOptions extends EnqueueOptions {
  missedPolicy?: 'skip' | 'catchUp';
  /** First run; defaults to one interval from registration. */
  startAt?: Date;
}

/**
 * Recurring work on fixed intervals.
 *
 * Deliberately not a cron parser. Interval scheduling covers the overwhelming
 * majority of background work and has no timezone or DST ambiguity, which is
 * where cron implementations quietly disagree with each other.
 *
 * Every run is enqueued with a deduplication key derived from the slot, so
 * several scheduler instances across several servers cannot produce duplicate
 * work for the same interval — no leader election required.
 */
export class Scheduler {
  private readonly jobs = new Map<string, RecurringJob>();
  private readonly clock: Clock;

  constructor(
    private readonly queue: JobQueue,
    options: { clock?: Clock } = {},
  ) {
    this.clock = options.clock ?? systemClock;
  }

  every(name: string, everyMs: number, payload: unknown = {}, options: ScheduleOptions = {}): this {
    if (everyMs <= 0) throw new InvalidJobError('Interval must be greater than zero');

    const now = this.clock();
    const { missedPolicy = 'skip', startAt, ...enqueueOptions } = options;

    this.jobs.set(name, {
      name,
      everyMs,
      payload,
      options: enqueueOptions,
      nextRunAt: startAt ?? new Date(now.getTime() + everyMs),
      lastEnqueuedAt: null,
      missedPolicy,
    });
    return this;
  }

  remove(name: string): boolean {
    return this.jobs.delete(name);
  }

  get scheduled(): RecurringJob[] {
    return [...this.jobs.values()].map((job) => ({ ...job }));
  }

  /** Enqueues everything now due. Call on a timer. */
  async tick(): Promise<number> {
    const now = this.clock();
    let enqueued = 0;

    for (const job of this.jobs.values()) {
      while (job.nextRunAt.getTime() <= now.getTime()) {
        const slot = job.nextRunAt;

        await this.queue.enqueue(job.name, job.payload, {
          ...job.options,
          // Slot-derived key: two schedulers racing on the same interval
          // produce one job, not two.
          dedupeKey: job.options.dedupeKey ?? `${job.name}@${slot.getTime()}`,
        });
        enqueued += 1;
        job.lastEnqueuedAt = now;
        job.nextRunAt = new Date(slot.getTime() + job.everyMs);

        if (job.missedPolicy === 'skip') {
          // Jump straight to the next future slot rather than replaying every
          // interval missed while the process was down.
          job.nextRunAt = nextFutureSlot(slot, job.everyMs, now);
          break;
        }
      }
    }
    return enqueued;
  }
}

/** First slot strictly after `now`, advancing from `from` by whole intervals. */
export function nextFutureSlot(from: Date, everyMs: number, now: Date): Date {
  const elapsed = now.getTime() - from.getTime();
  if (elapsed < 0) return from;
  const periods = Math.floor(elapsed / everyMs) + 1;
  return new Date(from.getTime() + periods * everyMs);
}
