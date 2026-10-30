/**
 * Public surface of the background job module.
 *
 * The queue, worker and scheduler have no HTTP dependency; only `routes.ts`
 * imports Express. Processing jobs from a CLI or a separate container needs
 * none of the web layer.
 */
export * from './types.js';
export * from './errors.js';
export { backoffDelay, type BackoffOptions } from './backoff.js';
export { InMemoryJobQueue, type JobQueue, type InMemoryJobQueueOptions } from './queue.js';
export { Worker, type WorkerOptions, type TickResult } from './worker.js';
export { Scheduler, nextFutureSlot, type RecurringJob, type ScheduleOptions } from './scheduler.js';
export { createJobsRouter, jobsErrorHandler, type JobsRouterDeps } from './routes.js';
export { createApp, type AppDependencies, type BuiltApp } from './app.js';
