import express, { type Express, type Request } from 'express';
import { InMemoryJobQueue, type JobQueue } from './queue.js';
import { Scheduler } from './scheduler.js';
import { Worker, type WorkerOptions } from './worker.js';
import { createJobsRouter, jobsErrorHandler } from './routes.js';
import { systemClock, type Clock } from './types.js';

export interface AppDependencies {
  queue?: JobQueue;
  clock?: Clock;
  worker?: WorkerOptions;
  /** Guards the admin API; it can enqueue work and replay dead jobs. */
  isAuthorized?: (req: Request) => boolean;
}

export interface BuiltApp {
  app: Express;
  queue: JobQueue;
  worker: Worker;
  scheduler: Scheduler;
}

/**
 * Builds the Express application from explicit dependencies.
 *
 * Nothing is constructed at import time, so tests spin up isolated instances
 * and production swaps the in-memory queue for Postgres or Redis without
 * touching this file.
 */
export function createApp(deps: AppDependencies = {}): BuiltApp {
  const clock = deps.clock ?? systemClock;
  const queue = deps.queue ?? new InMemoryJobQueue({ clock });
  const worker = new Worker(queue, { clock, ...deps.worker });
  const scheduler = new Scheduler(queue, { clock });

  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', 1);
  app.use(express.json({ limit: '256kb' }));

  app.get('/health', async (_req, res) => {
    const stats = await queue.stats();
    // A deep backlog is the symptom worth alerting on: the queue can be
    // "healthy" while jobs sit for hours because no worker is consuming.
    res.json({
      status: stats.oldestWaitingMs > 5 * 60_000 ? 'degraded' : 'ok',
      ...stats,
      workerId: worker.id,
      handlers: worker.registered,
      uptime: process.uptime(),
    });
  });

  app.use(
    '/jobs',
    createJobsRouter({
      queue,
      worker,
      scheduler,
      ...(deps.isAuthorized ? { isAuthorized: deps.isAuthorized } : {}),
    }),
  );

  app.use(jobsErrorHandler);
  app.use((_req, res) => {
    res.status(404).json({ error: { code: 'not_found', message: 'Endpoint not found' } });
  });

  return { app, queue, worker, scheduler };
}
