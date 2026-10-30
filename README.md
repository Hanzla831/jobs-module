# Message Queues & Background Jobs

A durable background job system for Node.js / TypeScript: leases with
visibility timeouts, at-least-once delivery, deduplication, priority, delayed
and recurring jobs, retry with jittered backoff, dead-letter capture and
replay, plus an admin API.

The queue, worker and scheduler have no HTTP dependency — only `routes.ts`
imports Express — so jobs can be processed from a separate container or a CLI
with none of the web layer.

```
src/
  types.ts      Domain model: jobs, states, priorities, handler contract
  errors.ts     Failure taxonomy — retryable vs permanent
  backoff.ts    Exponential backoff with full jitter
  queue.ts      Storage boundary + in-memory implementation
  worker.ts     Execution: concurrency, timeouts, heartbeats, shutdown
  scheduler.ts  Recurring jobs with a missed-interval policy
  routes.ts     Admin API
  app.ts        Express wiring
  server.ts     Process entry point
```

## The delivery guarantee

**At-least-once, stated plainly.** Exactly-once across a queue and a separate
database isn't achievable without distributed transactions, and pretending
otherwise is how systems double-charge people. So the contract is explicit —
handlers must be idempotent — and the queue provides the tools: deduplication
keys going in, a stable job id coming out.

```ts
await queue.enqueue('invoice.generate', { invoiceId: 7 }, {
  dedupeKey: `invoice-7`,     // a retried API call creates one job, not two
  priority: 'high',
  maxAttempts: 5,
});
```

## Decisions worth explaining

**Leases, not locks.** A claimed job holds a lease with an expiry. A worker
that is killed mid-job stops heartbeating, the lease lapses, and another worker
picks the job up — instead of it being stranded forever. The attempt counts at
*claim* time, so a worker that crashes repeatedly still exhausts its budget
rather than looping indefinitely.

**Only the lease holder can heartbeat.** Otherwise a stale worker could
resurrect a job that has already been reassigned.

**Retryable vs permanent is explicit, not inferred.** A timeout or a 503 is
worth retrying; a malformed payload will be malformed on every attempt, so it's
dead-lettered on the first failure rather than burning five attempts and
delaying everything queued behind it. A `retryAfterMs` from the dependency
overrides the computed backoff — the receiver knows better than our curve.

**Full jitter on backoff.** The delay is a uniform point in
`[0, exponential)`, not `exponential` plus noise. Without it, every job failed
by one outage retries at the same instant and the recovering dependency gets
exactly the thundering herd that knocked it over.

**Every execution races a timer.** A handler that never settles would hold its
lease forever (it keeps heartbeating) and silently consume a concurrency slot.

**A missing handler is dead-lettered, not retried.** That's usually a deploy
that removed a handler while jobs were still queued — no amount of waiting
registers it. Dead-lettering keeps the job for replay once it's restored.

**Deduplication releases on completion, not on enqueue.** While a job is
waiting or active the key collapses duplicates; once it finishes, the same key
legitimately describes new work.

**Scheduler slots carry a derived dedupe key.** Several scheduler instances
across several servers produce one job per interval, not one each — no leader
election required.

**Missed intervals have a policy.** `skip` enqueues one run and jumps to the
next future slot (right for a cache refresh — ten stale runs are pointless);
`catchUp` enqueues one job per missed interval (right for billing, where every
period owes work).

**Shutdown drains.** Stop accepting HTTP, stop claiming, then wait for
in-flight jobs. Exiting early strands active jobs until their leases expire,
delaying every retry by the full lease window. Handlers can read
`context.shuttingDown` and bail out early.

**The admin API never echoes payloads.** They routinely carry credentials.

**Health degrades on backlog age.** A queue can be perfectly "up" while jobs
sit for hours because nothing is consuming them — `oldestWaitingMs` is the
number worth alerting on.

## Running it

```bash
npm test        # 68 tests
npm run typecheck
npm run dev
```

```bash
curl -X POST localhost:3000/jobs -H 'x-jobs-token: …' \
  -d '{"name":"email.send","payload":{"to":"a@b.co"},"priority":"high"}'
```

Time is injected throughout (`Clock`), so leases, backoff, delays and schedules
are all tested with a fake clock — no timers, no sleeping, no flakes.

## Going to production

Replace `InMemoryJobQueue`; nothing else changes. The interface is already
shaped like a real broker:

- **Postgres** — `SELECT … FOR UPDATE SKIP LOCKED` for `claim`, a
  `lease_expires_at` column for reclaim.
- **Redis** — sorted set by `runAt`, `SET NX PX` for dedupe keys.
- **SQS** — visibility timeout maps directly onto the lease;
  `ChangeMessageVisibility` is `heartbeat`.
