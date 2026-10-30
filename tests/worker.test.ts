import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryJobQueue } from '../src/queue.js';
import { Worker } from '../src/worker.js';
import { backoffDelay } from '../src/backoff.js';
import { PermanentJobError, RetryableJobError } from '../src/errors.js';
import type { Clock } from '../src/types.js';

function testClock(start = new Date(Date.UTC(2026, 0, 15, 12, 0))) {
  let current = start;
  const clock: Clock = () => current;
  return { clock, advance: (ms: number) => void (current = new Date(current.getTime() + ms)), now: () => current };
}

function build(options: { maxAttempts?: number; concurrency?: number } = {}) {
  const time = testClock();
  const queue = new InMemoryJobQueue({ clock: time.clock, defaultLeaseMs: 10_000 });
  const worker = new Worker(queue, {
    clock: time.clock,
    workerId: 'w1',
    concurrency: options.concurrency ?? 5,
    backoff: { baseMs: 1_000, random: () => 1 }, // deterministic retry timing
  });
  return { time, queue, worker, maxAttempts: options.maxAttempts ?? 3 };
}

describe('execution', () => {
  it('runs a handler and completes the job', async () => {
    const { queue, worker } = build();
    const handler = vi.fn();
    worker.register('x', handler);
    await queue.enqueue('x', { value: 42 });

    const result = await worker.runOnce();
    expect(result).toMatchObject({ claimed: 1, completed: 1, retried: 0, dead: 0 });
    expect(handler).toHaveBeenCalledWith({ value: 42 }, expect.objectContaining({ attempt: 1 }));
    expect((await queue.stats()).waiting).toBe(0);
  });

  it('dead-letters a job with no registered handler', async () => {
    const { queue, worker } = build();
    await queue.enqueue('ghost', {});

    // A deploy removed the handler while jobs were still queued; no amount of
    // retrying will bring it back.
    const result = await worker.runOnce();
    expect(result.dead).toBe(1);
    expect((await queue.list('dead'))[0]!.lastError).toContain('No handler');
  });

  it('passes a working heartbeat to the handler', async () => {
    const { time, queue, worker } = build();
    worker.register('slow', async (_p, context) => {
      time.advance(8_000);
      await context.heartbeat();
    });
    await queue.enqueue('slow', {});

    await worker.runOnce();
    expect((await queue.stats()).completed).toBe(1);
  });

  it('respects the concurrency budget', async () => {
    const { queue, worker } = build({ concurrency: 2 });
    worker.register('x', async () => {});
    for (let i = 0; i < 5; i += 1) await queue.enqueue('x', {});

    expect((await worker.runOnce()).claimed).toBe(2);
  });
});

describe('failure handling', () => {
  it('retries a transient failure with backoff', async () => {
    const { time, queue, worker } = build();
    let attempts = 0;
    worker.register('flaky', async () => {
      attempts += 1;
      if (attempts === 1) throw new Error('temporary');
    });
    await queue.enqueue('flaky', {});

    expect((await worker.runOnce()).retried).toBe(1);
    // Not retried immediately — the backoff delay has to elapse.
    expect((await worker.runOnce()).claimed).toBe(0);

    time.advance(backoffDelay(1, { baseMs: 1_000, random: () => 1 }) + 1);
    expect((await worker.runOnce()).completed).toBe(1);
  });

  it('dead-letters a permanent failure on the first attempt', async () => {
    const { queue, worker } = build();
    worker.register('bad', async () => {
      throw new PermanentJobError('malformed payload');
    });
    await queue.enqueue('bad', {}, { maxAttempts: 10 });

    // Retrying a malformed payload nine more times only delays the queue.
    const result = await worker.runOnce();
    expect(result).toMatchObject({ retried: 0, dead: 1 });
  });

  it('honours a retry-after hint over its own backoff', async () => {
    const { time, queue, worker } = build();
    worker.register('rate-limited', async () => {
      throw new RetryableJobError('429', 120_000);
    });
    await queue.enqueue('rate-limited', {});

    await worker.runOnce();
    time.advance(60_000);
    expect((await worker.runOnce()).claimed).toBe(0); // provider asked for 2 min

    time.advance(61_000);
    expect((await worker.runOnce()).claimed).toBe(1);
  });

  it('dead-letters once attempts are exhausted', async () => {
    const { time, queue, worker } = build();
    worker.register('always-fails', async () => {
      throw new Error('nope');
    });
    await queue.enqueue('always-fails', {}, { maxAttempts: 2 });

    expect((await worker.runOnce()).retried).toBe(1);
    time.advance(600_000);
    expect((await worker.runOnce()).dead).toBe(1);

    const dead = await queue.list('dead');
    expect(dead[0]!.lastError).toContain('after 2 attempts');
  });

  it('times out a handler that never settles', async () => {
    const { queue, worker } = build();
    // Without the timeout this would hold a concurrency slot forever.
    worker.register('hangs', () => new Promise(() => {}));
    await queue.enqueue('hangs', {}, { timeoutMs: 50, maxAttempts: 1 });

    const result = await worker.runOnce();
    expect(result.dead).toBe(1);
    expect((await queue.list('dead'))[0]!.lastError).toContain('timeout');
  });

  it('treats a thrown non-Error as a failure', async () => {
    const { queue, worker } = build();
    worker.register('odd', async () => {
      throw 'a string';
    });
    await queue.enqueue('odd', {}, { maxAttempts: 1 });
    expect((await worker.runOnce()).dead).toBe(1);
  });
});

describe('callbacks and shutdown', () => {
  it('reports lifecycle events', async () => {
    const time = testClock();
    const queue = new InMemoryJobQueue({ clock: time.clock });
    const onCompleted = vi.fn();
    const onDead = vi.fn();
    const worker = new Worker(queue, { clock: time.clock, onCompleted, onDead });

    worker.register('ok', async () => {});
    worker.register('bad', async () => {
      throw new PermanentJobError('x');
    });
    await queue.enqueue('ok', {});
    await queue.enqueue('bad', {});
    await worker.runOnce();

    expect(onCompleted).toHaveBeenCalledTimes(1);
    expect(onDead).toHaveBeenCalledTimes(1);
  });

  it('stops claiming once shutting down', async () => {
    const { queue, worker } = build();
    worker.register('x', async () => {});
    await queue.enqueue('x', {});

    expect(await worker.shutdown(100)).toBe(true);
    expect(worker.isShuttingDown).toBe(true);
    // The job stays queued for another worker rather than being lost.
    expect((await worker.runOnce()).claimed).toBe(0);
    expect((await queue.stats()).waiting).toBe(1);
  });

  it('exposes shuttingDown to a running handler', async () => {
    const { queue, worker } = build();
    let observed: boolean | null = null;
    worker.register('x', async (_p, context) => {
      observed = context.shuttingDown;
    });
    await queue.enqueue('x', {});

    await worker.runOnce();
    expect(observed).toBe(false);
  });

  it('lists registered handlers', () => {
    const { worker } = build();
    worker.register('a', async () => {}).register('b', async () => {});
    expect(worker.registered.sort()).toEqual(['a', 'b']);
  });
});

describe('backoff', () => {
  it('grows exponentially and caps', () => {
    const options = { baseMs: 1_000, maxMs: 10_000, random: () => 1 };
    expect(backoffDelay(1, options)).toBe(1_000);
    expect(backoffDelay(2, options)).toBe(2_000);
    expect(backoffDelay(3, options)).toBe(4_000);
    expect(backoffDelay(10, options)).toBe(10_000);
  });

  it('applies full jitter', () => {
    // A point in [0, exponential) — this is what spreads a recovering
    // dependency's load instead of retrying everything at the same instant.
    expect(backoffDelay(4, { baseMs: 1_000, random: () => 0 })).toBe(0);
    expect(backoffDelay(4, { baseMs: 1_000, random: () => 0.5 })).toBe(4_000);
  });
});
