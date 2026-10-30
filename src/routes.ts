import { Router, type NextFunction, type Request, type Response } from 'express';
import { z } from 'zod';
import { InvalidJobError, JobNotFoundError, JobError } from './errors.js';
import type { JobQueue } from './queue.js';
import type { Scheduler } from './scheduler.js';
import type { Worker } from './worker.js';
import type { Job } from './types.js';

const enqueueSchema = z.object({
  name: z.string().min(1).max(128),
  payload: z.unknown().default({}),
  priority: z.enum(['low', 'normal', 'high', 'critical']).optional(),
  delayMs: z.number().int().min(0).max(30 * 24 * 3600_000).optional(),
  runAt: z.coerce.date().optional(),
  maxAttempts: z.number().int().min(1).max(20).optional(),
  dedupeKey: z.string().max(256).optional(),
  timeoutMs: z.number().int().min(100).max(10 * 60_000).optional(),
  metadata: z.record(z.string().max(40), z.string().max(500)).optional(),
});

const listSchema = z.object({
  state: z.enum(['waiting', 'active', 'completed', 'failed', 'dead', 'cancelled']).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

export interface JobsRouterDeps {
  queue: JobQueue;
  worker?: Worker;
  scheduler?: Scheduler;
  /** Guards every route; this API can enqueue work and replay dead jobs. */
  isAuthorized?: (req: Request) => boolean;
}

const asyncRoute =
  (handler: (req: Request, res: Response) => Promise<void>) =>
  (req: Request, res: Response, next: NextFunction): void => {
    void Promise.resolve(handler(req, res)).catch(next);
  };

/** Payloads can hold anything; only safe fields are returned. */
function serialize(job: Job) {
  return {
    id: job.id,
    name: job.name,
    state: job.state,
    priority: job.priority,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    runAt: job.runAt.toISOString(),
    lastError: job.lastError,
    metadata: job.metadata,
    createdAt: job.createdAt.toISOString(),
    completedAt: job.completedAt?.toISOString() ?? null,
  };
}

export function createJobsRouter(deps: JobsRouterDeps): Router {
  const router = Router();

  // Enqueuing work and replaying dead jobs are privileged operations, so the
  // whole router is gated rather than individual routes.
  router.use((req, res, next) => {
    if (deps.isAuthorized && !deps.isAuthorized(req)) {
      res.status(403).json({ error: { code: 'forbidden', message: 'Not authorized' } });
      return;
    }
    next();
  });

  router.post(
    '/',
    asyncRoute(async (req, res) => {
      const parsed = enqueueSchema.safeParse(req.body);
      if (!parsed.success) {
        throw new InvalidJobError(
          'Invalid job',
          parsed.error.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
        );
      }

      const { name, payload, ...options } = parsed.data;
      const job = await deps.queue.enqueue(name, payload, options);

      // 202: accepted for execution, not executed. Reporting 201 "created and
      // done" would be a lie the caller may act on.
      res.status(202).json({ job: serialize(job) });
    }),
  );

  router.get(
    '/stats',
    asyncRoute(async (_req, res) => {
      res.json({
        ...(await deps.queue.stats()),
        handlers: deps.worker?.registered ?? [],
        scheduled: deps.scheduler?.scheduled.map((s) => ({
          name: s.name,
          everyMs: s.everyMs,
          nextRunAt: s.nextRunAt.toISOString(),
        })) ?? [],
      });
    }),
  );

  router.get(
    '/',
    asyncRoute(async (req, res) => {
      const query = listSchema.parse(req.query);
      const jobs = await deps.queue.list(query.state, query.limit);
      res.json({ jobs: jobs.map(serialize), count: jobs.length });
    }),
  );

  router.get(
    '/:id',
    asyncRoute(async (req, res) => {
      const job = await deps.queue.find(String(req.params.id));
      if (!job) throw new JobNotFoundError(String(req.params.id));
      res.json({ job: serialize(job) });
    }),
  );

  /** Operator action once the cause of the failure is fixed. */
  router.post(
    '/:id/replay',
    asyncRoute(async (req, res) => {
      const job = await deps.queue.replay(String(req.params.id));
      res.json({ job: serialize(job) });
    }),
  );

  router.delete(
    '/:id',
    asyncRoute(async (req, res) => {
      // Only waiting jobs can be cancelled; an active one holds a lease and is
      // mid-execution, which would need cooperative interruption.
      const cancelled = await deps.queue.cancel(String(req.params.id));
      res.status(cancelled ? 204 : 409).end();
    }),
  );

  return router;
}

export const jobsErrorHandler = (
  error: unknown,
  _req: Request,
  res: Response,
  next: NextFunction,
): void => {
  if (res.headersSent) return next(error);

  if (error instanceof JobNotFoundError) {
    res.status(404).json({ error: { code: 'not_found', message: error.message } });
    return;
  }
  if (error instanceof InvalidJobError) {
    res.status(400).json({
      error: { code: 'invalid_job', message: error.message, details: error.details },
    });
    return;
  }
  if (error instanceof JobError) {
    res.status(400).json({ error: { code: 'job_error', message: error.message } });
    return;
  }
  next(error);
};
