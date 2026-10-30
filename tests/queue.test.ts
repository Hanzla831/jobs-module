import { beforeEach, describe, expect, it } from 'vitest';
import { InMemoryJobQueue } from '../src/queue.js';
import { InvalidJobError, JobNotFoundError } from '../src/errors.js';
import type { Clock } from '../src/types.js';

function testClock(start = new Date(Date.UTC(2026, 0, 15, 12, 0))) {
  let current = start;
  const clock: Clock = () => current;
  return { clock, advance: (ms: number) => void (current = new Date(current.getTime() + ms)), now: () => current };
}

describe('enqueue', () => {
  let time: ReturnType<typeof testClock>;
  let queue: InMemoryJobQueue;

  beforeEach(() => {
    time = testClock();
    queue = new InMemoryJobQueue({ clock: time.clock });
  });

  it('creates a waiting job with defaults', async () => {
    const job = await queue.enqueue('email.send', { to: 'a@b.co' });
    expect(job.state).toBe('waiting');
    expect(job.attempts).toBe(0);
    expect(job.maxAttempts).toBe(5);
    expect(job.runAt).toEqual(time.now());
  });

  it('rejects an empty job name', async () => {
    await expect(queue.enqueue('  ', {})).rejects.toBeInstanceOf(InvalidJobError);
  });

  it('honours a delay', async () => {
    const job = await queue.enqueue('x', {}, { delayMs: 60_000 });
    expect(job.runAt.getTime()).toBe(time.now().getTime() + 60_000);
    expect(await queue.claim('w1', 10)).toHaveLength(0);
  });

  it('prefers an absolute runAt over a delay', async () => {
    const runAt = new Date(time.now().getTime() + 3600_000);
    const job = await queue.enqueue('x', {}, { delayMs: 1000, runAt });
    expect(job.runAt).toEqual(runAt);
  });
});

describe('deduplication', () => {
  let queue: InMemoryJobQueue;
  beforeEach(() => {
    queue = new InMemoryJobQueue({ clock: testClock().clock });
  });

  it('collapses duplicates while a job is pending', async () => {
    const first = await queue.enqueue('invoice', { id: 7 }, { dedupeKey: 'invoice-7' });
    const second = await queue.enqueue('invoice', { id: 7 }, { dedupeKey: 'invoice-7' });

    // A retried API call must not create a second unit of work.
    expect(second.id).toBe(first.id);
    expect(await queue.list('waiting')).toHaveLength(1);
  });

  it('keeps deduplicating while the job is active', async () => {
    await queue.enqueue('invoice', {}, { dedupeKey: 'k' });
    await queue.claim('w1', 1);
    await queue.enqueue('invoice', {}, { dedupeKey: 'k' });
    expect(await queue.list()).toHaveLength(1);
  });

  it('allows the key again once the job completes', async () => {
    const first = await queue.enqueue('invoice', {}, { dedupeKey: 'k' });
    const [claimed] = await queue.claim('w1', 1);
    await queue.complete(claimed!.id);

    // The same key now describes genuinely new work.
    const second = await queue.enqueue('invoice', {}, { dedupeKey: 'k' });
    expect(second.id).not.toBe(first.id);
  });

  it('does not deduplicate without a key', async () => {
    await queue.enqueue('x', {});
    await queue.enqueue('x', {});
    expect(await queue.list('waiting')).toHaveLength(2);
  });
});

describe('claiming', () => {
  let time: ReturnType<typeof testClock>;
  let queue: InMemoryJobQueue;

  beforeEach(() => {
    time = testClock();
    queue = new InMemoryJobQueue({ clock: time.clock, defaultLeaseMs: 10_000 });
  });

  it('marks claimed jobs active and counts the attempt', async () => {
    await queue.enqueue('x', {});
    const [job] = await queue.claim('w1', 10);

    expect(job!.state).toBe('active');
    expect(job!.attempts).toBe(1);
    expect(job!.lockedBy).toBe('w1');
  });

  it('does not hand the same job to two workers', async () => {
    await queue.enqueue('x', {});
    expect(await queue.claim('w1', 10)).toHaveLength(1);
    expect(await queue.claim('w2', 10)).toHaveLength(0);
  });

  it('orders by priority, then oldest first', async () => {
    await queue.enqueue('low-old', {}, { priority: 'low' });
    time.advance(1000);
    await queue.enqueue('normal', {}, { priority: 'normal' });
    time.advance(1000);
    await queue.enqueue('critical', {}, { priority: 'critical' });

    const claimed = await queue.claim('w1', 10);
    // A backlog of low-priority work must not delay a critical job.
    expect(claimed.map((j) => j.name)).toEqual(['critical', 'normal', 'low-old']);
  });

  it('respects the batch limit', async () => {
    for (let i = 0; i < 5; i += 1) await queue.enqueue('x', {});
    expect(await queue.claim('w1', 2)).toHaveLength(2);
  });

  it('skips jobs that are not due yet', async () => {
    await queue.enqueue('later', {}, { delayMs: 5_000 });
    expect(await queue.claim('w1', 10)).toHaveLength(0);

    time.advance(5_001);
    expect(await queue.claim('w1', 10)).toHaveLength(1);
  });
});

describe('leases', () => {
  let time: ReturnType<typeof testClock>;
  let queue: InMemoryJobQueue;

  beforeEach(() => {
    time = testClock();
    queue = new InMemoryJobQueue({ clock: time.clock, defaultLeaseMs: 10_000 });
  });

  it('reclaims a job whose worker died', async () => {
    await queue.enqueue('x', {});
    await queue.claim('w1', 1);

    time.advance(10_001);
    expect(await queue.reclaimExpired()).toBe(1);

    // Another worker picks it up rather than it being stranded forever.
    const [reclaimed] = await queue.claim('w2', 1);
    expect(reclaimed!.lockedBy).toBe('w2');
    expect(reclaimed!.attempts).toBe(2);
  });

  it('extends a lease on heartbeat', async () => {
    await queue.enqueue('x', {});
    const [job] = await queue.claim('w1', 1);

    time.advance(8_000);
    expect(await queue.heartbeat(job!.id, 'w1')).toBe(true);

    time.advance(5_000); // past the original lease, inside the extended one
    expect(await queue.reclaimExpired()).toBe(0);
  });

  it('ignores a heartbeat from a worker that does not hold the lease', async () => {
    await queue.enqueue('x', {});
    const [job] = await queue.claim('w1', 1);
    // A stale worker must not be able to resurrect a reassigned job.
    expect(await queue.heartbeat(job!.id, 'w2')).toBe(false);
  });
});

describe('completion, failure and replay', () => {
  let time: ReturnType<typeof testClock>;
  let queue: InMemoryJobQueue;

  beforeEach(() => {
    time = testClock();
    queue = new InMemoryJobQueue({ clock: time.clock });
  });

  it('completes a job', async () => {
    await queue.enqueue('x', {});
    const [job] = await queue.claim('w1', 1);
    await queue.complete(job!.id);

    const stored = await queue.find(job!.id);
    expect(stored!.state).toBe('completed');
    expect(stored!.completedAt).not.toBeNull();
  });

  it('reschedules a retry', async () => {
    await queue.enqueue('x', {});
    const [job] = await queue.claim('w1', 1);
    const runAt = new Date(time.now().getTime() + 30_000);
    await queue.retry(job!.id, runAt, 'boom');

    const stored = await queue.find(job!.id);
    expect(stored!.state).toBe('waiting');
    expect(stored!.lastError).toBe('boom');
    expect(await queue.claim('w1', 1)).toHaveLength(0);
  });

  it('dead-letters and replays', async () => {
    await queue.enqueue('x', {});
    const [job] = await queue.claim('w1', 1);
    await queue.deadLetter(job!.id, 'gave up');

    expect((await queue.find(job!.id))!.state).toBe('dead');

    const replayed = await queue.replay(job!.id);
    expect(replayed.state).toBe('waiting');
    expect(replayed.attempts).toBe(0); // fresh budget after the cause is fixed
  });

  it('refuses to replay a job that is not dead', async () => {
    const job = await queue.enqueue('x', {});
    await expect(queue.replay(job.id)).rejects.toBeInstanceOf(InvalidJobError);
  });

  it('reports a missing job on replay', async () => {
    await expect(queue.replay('nope')).rejects.toBeInstanceOf(JobNotFoundError);
  });

  it('cancels a waiting job but not an active one', async () => {
    const waiting = await queue.enqueue('x', {});
    expect(await queue.cancel(waiting.id)).toBe(true);

    await queue.enqueue('y', {});
    const [active] = await queue.claim('w1', 1);
    // An active job holds a lease and is mid-execution.
    expect(await queue.cancel(active!.id)).toBe(false);
  });

  it('trims completed history but keeps live jobs', async () => {
    const small = new InMemoryJobQueue({ clock: time.clock, completedHistory: 2 });
    for (let i = 0; i < 4; i += 1) {
      await small.enqueue('x', { i });
      const [job] = await small.claim('w1', 1);
      await small.complete(job!.id);
    }
    expect(await small.list('completed')).toHaveLength(2);
  });
});

describe('stats', () => {
  it('counts states and the oldest waiting age', async () => {
    const time = testClock();
    const queue = new InMemoryJobQueue({ clock: time.clock });

    await queue.enqueue('old', {});
    time.advance(60_000);
    await queue.enqueue('new', {});
    // Critical so the claim takes this one rather than the oldest, leaving
    // `old` waiting — otherwise the age being measured is the job just claimed.
    await queue.enqueue('active', {}, { priority: 'critical' });
    await queue.claim('w1', 1);

    const stats = await queue.stats();
    expect(stats.active).toBe(1);
    expect(stats.waiting).toBe(2);
    // The number worth alerting on: jobs sitting while nothing consumes them.
    expect(stats.oldestWaitingMs).toBe(60_000);
  });

  it('reports zero age for an empty queue', async () => {
    expect((await new InMemoryJobQueue().stats()).oldestWaitingMs).toBe(0);
  });
});
