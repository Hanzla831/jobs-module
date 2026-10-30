import { describe, expect, it } from 'vitest';
import { InMemoryJobQueue } from '../src/queue.js';
import { Scheduler, nextFutureSlot } from '../src/scheduler.js';
import { InvalidJobError } from '../src/errors.js';
import type { Clock } from '../src/types.js';

function testClock(start = new Date(Date.UTC(2026, 0, 15, 12, 0))) {
  let current = start;
  const clock: Clock = () => current;
  return { clock, advance: (ms: number) => void (current = new Date(current.getTime() + ms)), now: () => current };
}

function build() {
  const time = testClock();
  const queue = new InMemoryJobQueue({ clock: time.clock });
  return { time, queue, scheduler: new Scheduler(queue, { clock: time.clock }) };
}

describe('recurring jobs', () => {
  it('enqueues nothing before the first interval', async () => {
    const { scheduler, queue } = build();
    scheduler.every('report', 60_000);
    expect(await scheduler.tick()).toBe(0);
    expect(await queue.list('waiting')).toHaveLength(0);
  });

  it('enqueues once the interval elapses', async () => {
    const { time, scheduler, queue } = build();
    scheduler.every('report', 60_000);

    time.advance(60_000);
    expect(await scheduler.tick()).toBe(1);
    expect((await queue.list('waiting'))[0]!.name).toBe('report');
  });

  it('keeps enqueuing on each interval', async () => {
    const { time, scheduler } = build();
    scheduler.every('report', 60_000);

    for (let i = 0; i < 3; i += 1) {
      time.advance(60_000);
      expect(await scheduler.tick()).toBe(1);
    }
  });

  it('rejects a non-positive interval', () => {
    const { scheduler } = build();
    expect(() => scheduler.every('bad', 0)).toThrow(InvalidJobError);
  });

  it('honours an explicit start time', async () => {
    const { time, scheduler } = build();
    scheduler.every('report', 60_000, {}, { startAt: time.now() });
    // Due immediately rather than one interval out.
    expect(await scheduler.tick()).toBe(1);
  });

  it('passes enqueue options through', async () => {
    const { time, scheduler, queue } = build();
    scheduler.every('report', 1_000, {}, { priority: 'low', maxAttempts: 2 });

    time.advance(1_000);
    await scheduler.tick();
    const job = (await queue.list('waiting'))[0]!;
    expect(job.priority).toBe('low');
    expect(job.maxAttempts).toBe(2);
  });

  it('can be removed', async () => {
    const { time, scheduler } = build();
    scheduler.every('report', 1_000);
    expect(scheduler.remove('report')).toBe(true);

    time.advance(10_000);
    expect(await scheduler.tick()).toBe(0);
  });
});

describe('missed interval policy', () => {
  it('skips missed runs by default', async () => {
    const { time, scheduler } = build();
    scheduler.every('cache.refresh', 60_000);

    // The process was down for an hour. Ten stale cache refreshes are
    // pointless — one, then back on schedule.
    time.advance(60 * 60_000);
    expect(await scheduler.tick()).toBe(1);
    expect(await scheduler.tick()).toBe(0);
  });

  it('catches up every missed run when asked', async () => {
    const { time, scheduler } = build();
    scheduler.every('billing', 60_000, {}, { missedPolicy: 'catchUp' });

    // Billing periods cannot be skipped: each one owes a job.
    time.advance(5 * 60_000);
    expect(await scheduler.tick()).toBe(5);
  });
});

describe('duplicate protection', () => {
  it('derives a dedupe key per slot so two schedulers cannot double-enqueue', async () => {
    const time = testClock();
    const queue = new InMemoryJobQueue({ clock: time.clock });
    const a = new Scheduler(queue, { clock: time.clock });
    const b = new Scheduler(queue, { clock: time.clock });

    a.every('report', 60_000);
    b.every('report', 60_000);

    time.advance(60_000);
    await a.tick();
    await b.tick();

    // Two servers running the same schedule produce one job, not two.
    expect(await queue.list('waiting')).toHaveLength(1);
  });
});

describe('nextFutureSlot', () => {
  it('advances by whole intervals past now', () => {
    const from = new Date(Date.UTC(2026, 0, 15, 12, 0));
    const now = new Date(from.getTime() + 2.5 * 60_000);
    expect(nextFutureSlot(from, 60_000, now).getTime()).toBe(from.getTime() + 3 * 60_000);
  });

  it('returns the slot unchanged when it is still ahead', () => {
    const from = new Date(Date.UTC(2026, 0, 15, 12, 0));
    const now = new Date(from.getTime() - 1_000);
    expect(nextFutureSlot(from, 60_000, now)).toEqual(from);
  });
});
