import { createApp } from './app.js';
import { PermanentJobError, RetryableJobError } from './errors.js';

/**
 * Node.js entry point: HTTP server, worker poll loop and scheduler tick, with
 * a shutdown sequence that drains rather than drops.
 */
const PORT = Number(process.env.PORT ?? 3000);
const NODE_ENV = process.env.NODE_ENV ?? 'development';
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 500);
const SCHEDULER_INTERVAL_MS = Number(process.env.SCHEDULER_INTERVAL_MS ?? 1_000);

async function main(): Promise<void> {
  const { app, worker, scheduler, queue } = createApp({
    worker: { concurrency: Number(process.env.CONCURRENCY ?? 5) },
    isAuthorized: (req) =>
      !process.env.JOBS_API_TOKEN || req.header('x-jobs-token') === process.env.JOBS_API_TOKEN,
  });

  // Example handlers. Each demonstrates one failure mode the worker treats
  // differently — which is the part that matters when writing real ones.
  worker.register<{ to: string }>('email.send', async (payload, context) => {
    if (!payload?.to?.includes('@')) {
      // Malformed input will be malformed on every retry.
      throw new PermanentJobError(`Invalid recipient: ${payload?.to}`);
    }
    await context.heartbeat();
    console.log(`[jobs] sent email to ${payload.to} (attempt ${context.attempt})`);
  });

  worker.register<{ url: string }>('webhook.deliver', async (payload) => {
    const response = await fetch(payload.url, { method: 'POST' });
    if (response.status === 429 || response.status >= 500) {
      const retryAfter = Number(response.headers.get('retry-after'));
      // The receiver knows better than our backoff curve when to come back.
      throw new RetryableJobError(
        `Receiver responded ${response.status}`,
        Number.isFinite(retryAfter) ? retryAfter * 1000 : undefined,
      );
    }
    if (!response.ok) throw new PermanentJobError(`Receiver rejected: ${response.status}`);
  });

  scheduler.every('reports.daily', 24 * 3600_000, {}, { priority: 'low' });
  scheduler.every('cache.refresh', 15 * 60_000, {}, { missedPolicy: 'skip' });

  const server = app.listen(PORT, () => {
    console.log(`[jobs] listening on http://localhost:${PORT} (${NODE_ENV})`);
    console.log(`[jobs] worker ${worker.id} handling: ${worker.registered.join(', ')}`);
  });

  // `draining` prevents overlapping ticks: a slow batch must not stack up
  // behind another poll and blow past the concurrency budget.
  let draining = false;
  const poll = setInterval(() => {
    if (draining) return;
    draining = true;
    void worker
      .runOnce()
      .then((result) => {
        if (result.claimed > 0) {
          console.log(
            `[jobs] completed=${result.completed} retried=${result.retried} dead=${result.dead}`,
          );
        }
      })
      .catch((error) => console.error('[jobs] worker tick failed', error))
      .finally(() => {
        draining = false;
      });
  }, POLL_INTERVAL_MS);
  poll.unref();

  const tick = setInterval(() => {
    void scheduler.tick().catch((error) => console.error('[jobs] scheduler tick failed', error));
  }, SCHEDULER_INTERVAL_MS);
  tick.unref();

  /**
   * Shutdown order matters: stop accepting HTTP, stop claiming new jobs, then
   * wait for in-flight work. Exiting early would strand active jobs until
   * their leases expire, delaying every retry by the full lease window.
   */
  const shutdown = (signal: string): void => {
    console.log(`[jobs] ${signal} received, draining`);
    clearInterval(poll);
    clearInterval(tick);

    server.close(async () => {
      const drained = await worker.shutdown(30_000);
      const stats = await queue.stats();
      console.log(
        `[jobs] drained=${drained} waiting=${stats.waiting} active=${stats.active}`,
      );
      process.exit(drained ? 0 : 1);
    });
    setTimeout(() => process.exit(1), 45_000).unref();
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('unhandledRejection', (reason) => {
    console.error('[jobs] unhandled rejection', reason);
    shutdown('unhandledRejection');
  });
}

main().catch((error) => {
  console.error('[jobs] failed to start:', error instanceof Error ? error.message : error);
  process.exit(1);
});
