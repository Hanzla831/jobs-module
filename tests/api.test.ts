import { beforeEach, describe, expect, it } from 'vitest';
import request from 'supertest';
import type { Express } from 'express';
import { createApp } from '../src/app.js';
import type { JobQueue } from '../src/queue.js';
import type { Worker } from '../src/worker.js';
import type { Clock } from '../src/types.js';

const NOW = new Date(Date.UTC(2026, 0, 15, 12, 0));
const clock: Clock = () => NOW;
const TOKEN = 'jobs-secret';

describe('jobs API', () => {
  let app: Express;
  let queue: JobQueue;
  let worker: Worker;

  beforeEach(() => {
    ({ app, queue, worker } = createApp({
      clock,
      isAuthorized: (req) => req.header('x-jobs-token') === TOKEN,
    }));
    worker.register('email.send', async () => {});
  });

  const post = () => request(app).post('/jobs').set('x-jobs-token', TOKEN);

  it('enqueues a job with 202', async () => {
    const response = await post().send({ name: 'email.send', payload: { to: 'a@b.co' } }).expect(202);

    // 202, not 201: accepted for execution, not executed.
    expect(response.body.job.state).toBe('waiting');
    expect(response.body.job.name).toBe('email.send');
  });

  it('requires authorization on every route', async () => {
    await request(app).post('/jobs').send({ name: 'x' }).expect(403);
    await request(app).get('/jobs/stats').expect(403);
  });

  it('validates the request body', async () => {
    const response = await post().send({ name: '' }).expect(400);
    expect(response.body.error.code).toBe('invalid_job');
    expect(Array.isArray(response.body.error.details)).toBe(true);
  });

  it('rejects an absurd delay', async () => {
    await post().send({ name: 'x', delayMs: 99 * 24 * 3600_000 }).expect(400);
  });

  it('never echoes the payload back', async () => {
    const response = await post()
      .send({ name: 'email.send', payload: { secret: 'api-key-12345' } })
      .expect(202);
    // Payloads routinely carry credentials; the admin API must not leak them.
    expect(JSON.stringify(response.body)).not.toContain('api-key-12345');
  });

  it('deduplicates through the API', async () => {
    const first = await post().send({ name: 'email.send', dedupeKey: 'k1' }).expect(202);
    const second = await post().send({ name: 'email.send', dedupeKey: 'k1' }).expect(202);
    expect(second.body.job.id).toBe(first.body.job.id);
  });

  it('lists and filters jobs', async () => {
    await post().send({ name: 'email.send' }).expect(202);
    await post().send({ name: 'email.send' }).expect(202);

    const all = await request(app).get('/jobs').set('x-jobs-token', TOKEN).expect(200);
    expect(all.body.count).toBe(2);

    const dead = await request(app)
      .get('/jobs?state=dead')
      .set('x-jobs-token', TOKEN)
      .expect(200);
    expect(dead.body.count).toBe(0);
  });

  it('fetches one job and 404s an unknown id', async () => {
    const created = await post().send({ name: 'email.send' }).expect(202);

    await request(app)
      .get(`/jobs/${created.body.job.id}`)
      .set('x-jobs-token', TOKEN)
      .expect(200);

    await request(app).get('/jobs/missing').set('x-jobs-token', TOKEN).expect(404);
  });

  it('cancels a waiting job, and 409s an active one', async () => {
    const created = await post().send({ name: 'email.send' }).expect(202);
    await request(app)
      .delete(`/jobs/${created.body.job.id}`)
      .set('x-jobs-token', TOKEN)
      .expect(204);

    const second = await post().send({ name: 'email.send' }).expect(202);
    await queue.claim('w1', 1);
    await request(app)
      .delete(`/jobs/${second.body.job.id}`)
      .set('x-jobs-token', TOKEN)
      .expect(409);
  });

  it('replays a dead job', async () => {
    const created = await post().send({ name: 'email.send' }).expect(202);
    await queue.claim('w1', 1);
    await queue.deadLetter(created.body.job.id, 'boom');

    const replayed = await request(app)
      .post(`/jobs/${created.body.job.id}/replay`)
      .set('x-jobs-token', TOKEN)
      .expect(200);

    expect(replayed.body.job.state).toBe('waiting');
    expect(replayed.body.job.attempts).toBe(0);
  });

  it('refuses to replay a job that is not dead', async () => {
    const created = await post().send({ name: 'email.send' }).expect(202);
    await request(app)
      .post(`/jobs/${created.body.job.id}/replay`)
      .set('x-jobs-token', TOKEN)
      .expect(400);
  });

  it('reports stats, handlers and schedules', async () => {
    await post().send({ name: 'email.send' }).expect(202);

    const response = await request(app).get('/jobs/stats').set('x-jobs-token', TOKEN).expect(200);
    expect(response.body.waiting).toBe(1);
    expect(response.body.handlers).toContain('email.send');
  });
});

describe('health', () => {
  it('is open and reports queue depth', async () => {
    const { app } = createApp({ clock, isAuthorized: () => false });
    const response = await request(app).get('/health').expect(200);
    expect(response.body.status).toBe('ok');
    expect(response.body.waiting).toBe(0);
  });

  it('degrades when jobs sit unprocessed', async () => {
    let now = new Date(NOW);
    const { app } = createApp({ clock: () => now });

    await request(app).post('/jobs').send({ name: 'x' }).expect(202);
    now = new Date(now.getTime() + 10 * 60_000);

    // The queue is "up" but nothing is consuming it — that is the state worth
    // alerting on.
    const response = await request(app).get('/health').expect(200);
    expect(response.body.status).toBe('degraded');
  });

  it('404s an unknown endpoint', async () => {
    const { app } = createApp({ clock });
    await request(app).get('/nope').expect(404);
  });
});
