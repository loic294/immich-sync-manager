import { describe, expect, it } from 'vitest';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import { mount } from '@/test/helpers';
import '@/components/ism-sync-thumbnail';

const transfer: SyncActivity = {
  id: 'transfer1',
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
