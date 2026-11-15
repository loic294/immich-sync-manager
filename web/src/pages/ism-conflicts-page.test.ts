import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { ConflictView } from '@/api/generated/ConflictView';
import { mount } from '@/test/helpers';
import '@/pages/ism-conflicts-page';

const side = {
  instance: 'Example',
  sha256: null,
  size: 10,
  modified_at: null,
  deleted: false,
};
const conflict: ConflictView = {
  id: 'conflict-1',
  share_name: 'Photos',
  location: { root_id: 'photos', path: 'a.jpg' },
  kind: 'changed_on_both',
  detail: '',
  local: side,
  remote: side,
  peer_id: 'peer-1',
  status: 'open',
  created_at: '2024-01-01T00:00:00Z',
  moved_from: null,
};

describe('ism-conflicts-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('offers every resolution and shows where a moved file came from', async () => {
    vi.spyOn(api, 'get').mockResolvedValue([
      conflict,
      {
        ...conflict,
        id: 'conflict-2',
        kind: 'folder_move',
        location: { ...conflict.location, path: 'b' },
        moved_from: { root_id: 'photos', path: 'old' },
      },
    ] as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelectorAll('article')).toHaveLength(2));
    const [fileCard, folderCard] = page.querySelectorAll('article');
    const labels = (card: Element) =>
      [...card.querySelectorAll('button')].map((button) => [
        button.textContent?.trim(),
        button.disabled,
      ]);
    expect(labels(fileCard)).toEqual([
      ['Keep this instance', false],
      ['Keep the peer', false],
      ['Keep both', false],
      ['Skip', false],
    ]);
    expect(labels(folderCard).map(([label]) => label)).not.toContain('Keep both');
    expect(folderCard.textContent).toContain('Moved by the peer from');
  });
});
