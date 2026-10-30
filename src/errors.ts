/**
 * Failure taxonomy. The distinction drives everything the worker does with a
 * failed job, so it is explicit rather than inferred from a message string.
 */

export class JobError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
    Error.captureStackTrace?.(this, new.target);
  }
}

/**
 * Transient: the work is fine, the world was briefly not. A timeout, a 503, a
 * lock contention. Worth retrying with backoff.
 */
export class RetryableJobError extends JobError {
  readonly retryable = true;
  constructor(
    message: string,
    /** Honoured over the computed backoff when a provider says when to return. */
    readonly retryAfterMs?: number,
  ) {
    super(message);
  }
}

/**
 * Permanent: this job will fail identically forever — a malformed payload, a
 * deleted record. Retrying wastes the attempt budget and delays everything
 * queued behind it, so it is dead-lettered on the first failure.
 */
export class PermanentJobError extends JobError {
  readonly retryable = false;
}

/** A handler exceeded its time budget and was abandoned. */
export class JobTimeoutError extends JobError {
  constructor(readonly timeoutMs: number) {
    super(`Job exceeded its ${timeoutMs}ms timeout`);
  }
}

/**
 * No handler registered for this job name — usually a deploy that removed a
 * handler while jobs were still queued. Dead-lettered rather than retried,
 * because no amount of waiting will register the handler.
 */
export class UnknownHandlerError extends JobError {
  readonly status = 400;
  constructor(readonly jobName: string) {
    super(`No handler registered for job "${jobName}"`);
  }
}

export class JobNotFoundError extends JobError {
  readonly status = 404;
  constructor(readonly jobId: string) {
    super(`Job "${jobId}" not found`);
  }
}

export class InvalidJobError extends JobError {
  readonly status = 400;
  constructor(message: string, readonly details?: unknown) {
    super(message);
  }
}
