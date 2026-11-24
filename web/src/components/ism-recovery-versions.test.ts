import { afterEach, expect, it, vi } from 'vitest';
import { events } from '@/api/events';
import { mount } from '@/test/helpers';
import '@/components/ism-recovery-versions';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it('uses the receiving owner for previews and preserves sidecar roles without requiring an original', async () => {
  vi.stubGlobal('IntersectionObserver', undefined);
  const fetchPreview = vi
    .spyOn(globalThis, 'fetch')
    .mockImplementation(async () => new Response(new Blob(['image']), { status: 200 }));
  vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:example');
  vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {});
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
  await vi.waitFor(() => expect(page.querySelectorAll('img')).toHaveLength(2));
  const images = [...page.querySelectorAll('img')];
  expect(images).toHaveLength(2);
  const url = new URL(String(fetchPreview.mock.calls[1][0]), window.location.origin);
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

it('reobserves a reused preview element when the target changes', async () => {
  const observe = vi.fn();
  const unobserve = vi.fn();
  vi.stubGlobal(
    'IntersectionObserver',
    class {
      observe = observe;
      unobserve = unobserve;
      disconnect() {}
    },
  );
  const page = await mount('ism-recovery-versions', {
    target: { kind: 'conflict', id: 'local', peer_id: null },
    versions: {
      share: 'Photos',
      revision: 'revision',
      receiving: [],
      source: {
        instance: 'Example',
        path: 'example.jpg',
        state: 'file',
        directories: [],
        files: [{ path: 'example.jpg', role: 'original', sha256: 'a', size: 1, modified_at: null }],
      },
    },
  });
  const element = page.querySelector('[data-preview]');
  expect(observe).toHaveBeenCalledTimes(1);
  page.target = { kind: 'conflict', id: 'peer', peer_id: 'receiver' };
  await page.updateComplete;
  expect(page.querySelector('[data-preview]')).toBe(element);
  expect(unobserve).toHaveBeenCalledWith(element);
  expect(observe).toHaveBeenCalledTimes(2);
});

it.each([
  ['preview_outside_transfer_hours', 'Preview available during allowed transfer hours.'],
  ['preview_busy', 'Preview preparation is busy'],
])(
  'retries %s previews without retransferring successful images or leaking blobs',
  async (code, message) => {
    vi.stubGlobal('IntersectionObserver', undefined);
    let blocked = true;
    const fetchPreview = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
      if (String(input).includes('source=true') && blocked) {
        return new Response(
          JSON.stringify({
            error: {
              code,
              message: 'Preview preparation is busy',
            },
          }),
          { status: 403, headers: { 'content-type': 'application/json' } },
        );
      }
      return new Response(new Blob(['image']), { status: 200 });
    });
    let next = 0;
    vi.spyOn(URL, 'createObjectURL').mockImplementation(() => `blob:example-${next++}`);
    const revoke = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {});
    vi.useFakeTimers();
    const page = await mount('ism-recovery-versions', {
      target: { kind: 'conflict', id: 'choice', peer_id: null },
      versions: {
        share: 'Photos',
        revision: 'revision',
        receiving: [
          {
            instance: 'Alpha',
            path: 'example.jpg',
            state: 'file',
            directories: [],
            files: [
              { path: 'example.jpg', role: 'original', sha256: 'a', size: 1, modified_at: null },
            ],
          },
        ],
        source: {
          instance: 'Beta',
          path: 'example.jpg',
          state: 'file',
          directories: [],
          files: [
            { path: 'example.jpg', role: 'original', sha256: 'b', size: 1, modified_at: null },
          ],
        },
      },
    });
    await vi.advanceTimersByTimeAsync(0);
    await page.updateComplete;
    expect(page.textContent).toContain(message);
    expect(fetchPreview).toHaveBeenCalledTimes(2);
    events.emit({ type: 'sync_changed' });
    await vi.advanceTimersByTimeAsync(1500);
    expect(fetchPreview).toHaveBeenCalledTimes(3);
    expect(page.querySelectorAll('img')).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetchPreview).toHaveBeenCalledTimes(4);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetchPreview).toHaveBeenCalledTimes(5);
    const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(120_000);
    expect(fetchPreview).toHaveBeenCalledTimes(5);
    blocked = false;
    visibility.mockReturnValue('visible');
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchPreview).toHaveBeenCalledTimes(6);
    await page.updateComplete;
    expect(page.querySelectorAll('img')).toHaveLength(2);
    page.remove();
    expect(revoke).toHaveBeenCalledWith('blob:example-0');
    expect(revoke).toHaveBeenCalledWith('blob:example-1');
    await vi.advanceTimersByTimeAsync(120_000);
    expect(fetchPreview).toHaveBeenCalledTimes(6);
  },
);
