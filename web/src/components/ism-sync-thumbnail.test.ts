import { describe, expect, it } from 'vitest';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import { mount } from '@/test/helpers';
import '@/components/ism-sync-thumbnail';

const transfer: SyncActivity = {
  id: 'transfer1',
  outgoing: false,
  peer_id: 'peer1',
  share_name: 'Photos',
  relative_path: 'Trip/a.jpg',
  location: { root_id: 'photos', path: 'Trip/a.jpg' },
  source: 'ISM: Remote',
  destination: 'ISM: Local',
  destination_path: '/library/photos/Trip/a.jpg',
  status: 'running',
  size: 100,
  bytes_done: 50,
  error: null,
  queued_at: '2026-01-01T00:00:00Z',
  finished_at: null,
  operation_id: null,
};

describe('sync thumbnails', () => {
  it.each([false, true])(
    'keeps a completed preview if the local image is unavailable (outgoing: %s)',
    async (outgoing) => {
      const thumbnail = await mount('ism-sync-thumbnail', {
        transfer: { ...transfer, outgoing, status: 'completed' },
      });
      const local = thumbnail.querySelector('img');
      expect(local?.getAttribute('src')).not.toContain('peer_id');
      local?.dispatchEvent(new Event('error'));
      await thumbnail.updateComplete;
      const source = thumbnail.querySelector('img');
      expect(source).not.toBeNull();
      expect(source?.getAttribute('src')).toContain('peer_id=peer1');

      thumbnail.transfer = { ...transfer, outgoing, status: 'completed', bytes_done: 100 };
      await thumbnail.updateComplete;
      expect(thumbnail.querySelector('img')?.getAttribute('src')).toBe(source?.getAttribute('src'));

      source?.dispatchEvent(new Event('error'));
      await thumbnail.updateComplete;
      expect(thumbnail.querySelector('img')).toBeNull();
      expect(thumbnail.textContent).toContain('JPG');
    },
  );

  it('keeps the file-type fallback after a failed preview and retries locally on completion', async () => {
    const thumbnail = await mount('ism-sync-thumbnail', { transfer });
    thumbnail.querySelector('img')?.dispatchEvent(new Event('error'));
    await thumbnail.updateComplete;
    expect(thumbnail.querySelector('img')).toBeNull();
    expect(thumbnail.textContent).toContain('JPG');

    thumbnail.transfer = { ...transfer, bytes_done: 75 };
    await thumbnail.updateComplete;
    expect(thumbnail.querySelector('img')).toBeNull();

    thumbnail.transfer = { ...transfer, status: 'completed' };
    await thumbnail.updateComplete;
    const image = thumbnail.querySelector('img');
    expect(image).not.toBeNull();
    expect(image?.getAttribute('src')).not.toContain('peer_id');
  });
});
