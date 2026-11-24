import { describe, expect, it, vi } from 'vitest';
import type { RequestOptions } from '@/api/client';
import { api } from '@/api/client';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import { VersionReview } from '@/components/version-review';
import { mockLiveSnapshots } from '@/test/live';

function batchTargets(input: RequestOptions | undefined): RecoveryTarget[] {
  if (!input?.body) throw new Error('Missing evidence batch body');
  return (input.body as { targets: RecoveryTarget[] }).targets;
}

describe('VersionReview', () => {
  it('prepares 400 targets with bounded batch requests instead of 400 individual calls', async () => {
    const get = vi.spyOn(api, 'get');
    const post = vi.spyOn(api, 'post').mockImplementation(
      async (_path, input) =>
        ({
          items: batchTargets(input).map((target) => ({
            state: 'ready',
            target,
            versions: { revision: 'ready' },
          })),
        }) as never,
    );
    try {
      const review = new VersionReview({
        addController() {},
        removeController() {},
        requestUpdate() {},
        updateComplete: Promise.resolve(true),
      });
      const targets = Array.from({ length: 400 }, (_, i) => ({
        kind: 'conflict' as const,
        id: `example-${i}`,
        peer_id: null,
      }));
      await review.compare(targets);
      expect(targets.filter((target) => review.has(target))).toHaveLength(400);
      expect(get).not.toHaveBeenCalled();
      expect(post).toHaveBeenCalledTimes(8);
      for (const [path, input] of post.mock.calls) {
        expect(path).toBe('/api/admin/recovery/versions/batch');
        expect(batchTargets(input)).toHaveLength(50);
      }
    } finally {
      get.mockRestore();
      post.mockRestore();
    }
  });

  it('limits overlapping comparisons globally and discards obsolete queued work', async () => {
    let active = 0;
    let maximum = 0;
    const resolve: (() => void)[] = [];
    const get = vi.spyOn(api, 'post').mockImplementation(
      (_path, input) =>
        new Promise((done) => {
          active++;
          maximum = Math.max(maximum, active);
          resolve.push(() => {
            active--;
            done({
              items: batchTargets(input).map((target) => ({
                state: 'ready',
                target,
                versions: { revision: 'ready' },
              })),
            } as never);
          });
        }),
    );
    try {
      const review = new VersionReview({
        addController() {},
        removeController() {},
        requestUpdate() {},
        updateComplete: Promise.resolve(true),
      });
      const targets = Array.from({ length: 200 }, (_, i) => ({
        kind: 'conflict' as const,
        id: `example-${i}`,
        peer_id: null,
      }));
      const first = review.compare(targets.slice(0, 100));
      const second = review.compare(targets.slice(100));
      await Promise.resolve();
      expect(get).toHaveBeenCalledTimes(2);
      review.retain(targets.slice(0, 102));
      for (const finish of resolve.splice(0)) finish();
      await vi.waitFor(() => expect(get).toHaveBeenCalledTimes(3));
      for (const finish of resolve.splice(0)) finish();
      await Promise.all([first, second]);
      expect(maximum).toBe(2);
      review.hostDisconnected();
    } finally {
      get.mockRestore();
    }
  });

  it('keeps pushed evidence when an obsolete initial HTTP failure arrives later', async () => {
    const live = mockLiveSnapshots();
    let reject!: (reason: Error) => void;
    const get = vi
      .spyOn(api, 'post')
      .mockImplementationOnce(() => new Promise((_, fail) => (reject = fail)));
    try {
      const review = new VersionReview({
        addController() {},
        removeController() {},
        requestUpdate() {},
        updateComplete: Promise.resolve(true),
      });
      const target = { kind: 'conflict' as const, id: 'example', peer_id: null };
      const pending = review.compare([target]);
      await Promise.resolve();
      live.push('/recovery/versions/batch', {
        items: [{ state: 'ready', target, versions: { revision: 'ready' } }],
      });
      reject(new Error('Still preparing'));
      await pending;
      await vi.waitFor(() => expect(review.has(target)).toBe(true));
      expect(get).toHaveBeenCalledOnce();
      expect(review.get(target)?.revision).toBe('ready');
      review.hostDisconnected();
    } finally {
      vi.restoreAllMocks();
    }
  });

  it('accumulates ready evidence while readiness notifications retry only unavailable files', async () => {
    let round = 0;
    const post = vi.spyOn(api, 'post').mockImplementation(async (_path, input) => {
      round++;
      return {
        items: batchTargets(input).map((target) =>
          target.id === 'waiting' && round === 1
            ? { state: 'unavailable', target, code: 'peer_busy', message: 'Still preparing' }
            : { state: 'ready', target, versions: { revision: 'ready' } },
        ),
      } as never;
    });
    try {
      const review = new VersionReview({
        addController() {},
        removeController() {},
        requestUpdate() {},
        updateComplete: Promise.resolve(true),
      });
      const targets = ['ready', 'waiting'].map((id) => ({
        kind: 'conflict' as const,
        id,
        peer_id: null,
      }));
      await review.compare(targets);
      expect(review.has(targets[0])).toBe(true);
      expect(review.has(targets[1])).toBe(false);
      review.retryUnavailable(targets);
      expect(review.has(targets[0])).toBe(true);
      await vi.waitFor(() => expect(review.has(targets[1])).toBe(true));
      expect(batchTargets(post.mock.calls[1][1])).toEqual([targets[1]]);
      review.forget(targets[0]);
      expect(review.has(targets[0])).toBe(false);
    } finally {
      post.mockRestore();
    }
  });
});
