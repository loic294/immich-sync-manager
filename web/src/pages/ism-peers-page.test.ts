import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { mount } from '@/test/helpers';
import '@/pages/ism-peers-page';

describe('ism-peers-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('lets an administrator retry a peer showing an error', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/peers') {
        return [
          {
            id: 'peer-1',
            name: 'Example',
            url: 'https://peer.example',
            status: 'online',
            last_seen_at: null,
            last_error: 'peer folder operation needs attention',
            events_applied: 0,
          },
        ] as never;
      }
      return [] as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue(undefined as never);
    const page = await mount('ism-peers-page');
    await vi.waitFor(() => expect(page.textContent).toContain('peer folder operation'));
    const retry = [...page.querySelectorAll('button')].find(
      (button) => button.textContent?.trim() === 'Retry',
    );
    expect(retry).toBeDefined();
    retry?.click();
    await vi.waitFor(() => expect(post).toHaveBeenCalledWith('/api/admin/peers/peer-1/retry'));
  });
});
