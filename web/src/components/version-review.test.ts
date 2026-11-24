import { describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { VersionReview } from '@/components/version-review';
import { mockLiveSnapshots } from '@/test/live';

describe('VersionReview', () => {
  it('limits overlapping comparisons globally and discards obsolete queued work', async () => {
    let active = 0;
    let maximum = 0;
    const resolve: (() => void)[] = [];
    const get = vi.spyOn(api, 'get').mockImplementation(
      () =>
        new Promise((done) => {
          active++;
          maximum = Math.max(maximum, active);
          resolve.push(() => {
            active--;
            done({} as never);
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
      const targets = Array.from({ length: 20 }, (_, i) => ({
        kind: 'conflict' as const,
        id: `example-${i}`,
        peer_id: null,
      }));
      const first = review.compare(targets.slice(0, 10));
      const second = review.compare(targets.slice(10));
      expect(get).toHaveBeenCalledTimes(4);
      review.retain([...targets.slice(0, 4), ...targets.slice(10, 12)]);
      for (const finish of resolve.splice(0)) finish();
      await vi.waitFor(() => expect(get).toHaveBeenCalledTimes(6));
      for (const finish of resolve.splice(0)) finish();
      await Promise.all([first, second]);
      expect(maximum).toBe(4);
      review.hostDisconnected();
    } finally {
      get.mockRestore();
    }
  });

  it('keeps pushed evidence when an obsolete initial HTTP failure arrives later', async () => {
    const live = mockLiveSnapshots();
    let reject!: (reason: Error) => void;
    const get = vi
      .spyOn(api, 'get')
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
      live.push('/recovery/versions', { revision: 'ready' });
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
});
