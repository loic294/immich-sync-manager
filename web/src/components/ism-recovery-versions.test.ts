import { expect, it } from 'vitest';
import { mount } from '@/test/helpers';
import '@/components/ism-recovery-versions';

it('uses the receiving owner for previews and preserves sidecar roles without requiring an original', async () => {
  const page = await mount('ism-recovery-versions');
  page.target = { kind: 'conflict', id: 'choice', peer_id: 'receiving-owner' };
  const file = {
    path: 'Trip/photo.jpg',
    role: 'original',
    sha256: 'a'.repeat(64),
    size: 12,
    modified_at: null,
  };
  page.versions = {
    share: 'Photos',
    revision: 'revision',
    receiving: [
      {
        instance: 'Beta',
        path: 'Trip',
        state: 'folder',
        directories: ['Trip', 'Trip/empty'],
        files: [
          file,
          { ...file, path: 'Trip/orphan.xmp', role: 'sidecar', sha256: 'b'.repeat(64) },
        ],
      },
    ],
    source: {
      instance: 'Alpha',
      path: 'Moved',
      state: 'folder',
      directories: ['Moved', 'Moved/empty'],
      files: [{ ...file, path: 'Moved/photo.jpg' }],
    },
  };
  await page.updateComplete;
  const images = [...page.querySelectorAll('img')];
  expect(images).toHaveLength(2);
  const url = new URL(images[1].src);
  expect(url.searchParams.get('peer_id')).toBe('receiving-owner');
  expect(url.searchParams.get('source')).toBe('true');
  expect(url.searchParams.get('path')).toBe('Moved/photo.jpg');
  expect(page.textContent).toContain('sidecar');
  expect(page.textContent).toContain('Trip/empty');
  expect(page.textContent).toContain('Moved/empty');
  expect(page.textContent).toContain('Same bytes on both instances');
  images[0].dispatchEvent(new Event('error'));
  await page.updateComplete;
  expect(page.textContent).toContain('Preview unavailable');
  expect(page.querySelectorAll('img')).toHaveLength(1);
});
