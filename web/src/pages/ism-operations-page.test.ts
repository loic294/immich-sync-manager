import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events } from '@/api/events';
import type { OperationActivity } from '@/api/generated/OperationActivity';
import { mount } from '@/test/helpers';
import '@/pages/ism-operations-page';

vi.mock('@/components/confirm', () => ({
  confirmAction: vi.fn(async () => true),
}));

const OP: OperationActivity = {
  operation_id: 'op1',
  title: 'Move',
  label: 'Move selected photos',
  source: 'OPM: editor',
  destination: 'ISM: Archive',
  status: 'running',
  progress: { done: 0, total: 2 },
  created_at: '2026-01-01T00:00:00Z',
  finished_at: null,
  handled_at: null,
  recovery_resolved: false,
};

describe('operations activity', () => {
  afterEach(() => vi.restoreAllMocks());
  it('paginates on the server, resets on status changes, and refreshes from live notifications', async () => {
    const get = vi.spyOn(api, 'get').mockResolvedValue({ rows: [OP], total: 6000 } as never);
    const page = await mount('ism-operations-page');
    await vi.waitFor(() => expect(page.textContent).toContain('OPM: editor'));
    expect(get).toHaveBeenCalledWith('/api/admin/operations/page', {
      query: { status: '', limit: 250, offset: 0 },
    });
    const pagination = page.querySelector('ism-pagination');
    pagination?.dispatchEvent(
      new CustomEvent('page-change', { detail: { limit: 5000, offset: 0 } }),
    );
    await vi.waitFor(() =>
      expect(get).toHaveBeenLastCalledWith('/api/admin/operations/page', {
        query: { status: '', limit: 5000, offset: 0 },
      }),
    );
    pagination?.dispatchEvent(
      new CustomEvent('page-change', { detail: { limit: 5000, offset: 5000 } }),
    );
    await vi.waitFor(() =>
      expect(get).toHaveBeenLastCalledWith('/api/admin/operations/page', {
        query: { status: '', limit: 5000, offset: 5000 },
      }),
    );
    page.querySelector<HTMLButtonElement>('#operations-tab-completed')?.click();
    await vi.waitFor(() =>
      expect(get).toHaveBeenLastCalledWith('/api/admin/operations/page', {
        query: { status: 'completed', limit: 5000, offset: 0 },
      }),
    );
    get.mockResolvedValue({
      rows: [{ ...OP, status: 'completed', progress: { done: 2, total: 2 } }],
      total: 1,
    } as never);
    events.emit({ type: 'operation_updated', operation_id: 'op1' });
    await vi.waitFor(() => expect(page.textContent).toContain('2/2'));
  });

  it('shows live subtasks and marks attention handled without resolving recovery', async () => {
    let handled = false;
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/operations/op1/detail')
        return {
          activity: {
            ...OP,
            status: 'recovery_required',
            handled_at: handled ? '2026-01-02T00:00:00Z' : null,
          },
          operation: {
            ...OP,
            source: 'admin',
            status: 'recovery_required',
            items: [{ index: 0, kind: 'move', status: 'running', error: null, result: null }],
          },
          tasks: [
            {
              files: [],
              steps: [
                { label: 'Move original.jpg', status: 'running' },
                { label: 'Update Immich', status: 'pending' },
              ],
            },
          ],
        } as never;
      return { rows: [OP], total: 1 } as never;
    });
    const post = vi.spyOn(api, 'post').mockImplementation(async () => {
      handled = true;
      return undefined as never;
    });
    const page = await mount('ism-operations-page');
    await vi.waitFor(() => expect(page.textContent).toContain('OPM: editor'));
    page.querySelector<HTMLButtonElement>('ism-operation-table button')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('Move original.jpg'));
    expect(page.querySelector('dialog[open]')).not.toBeNull();
    [...page.querySelectorAll('button')]
      .find((b) => b.textContent?.includes('Mark handled'))
      ?.click();
    await vi.waitFor(() => expect(post).toHaveBeenCalledWith('/api/admin/operations/op1/handled'));
    expect(post).not.toHaveBeenCalledWith('/api/admin/operations/op1/resolve');
    await vi.waitFor(() => expect(page.textContent).toContain('Handled'));
  });

  it('offers a file recheck for interrupted recovery', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/operations/op1/detail')
        return {
          activity: { ...OP, status: 'recovery_required' },
          operation: {
            ...OP,
            source: 'admin',
            status: 'recovery_required',
            items: [
              {
                index: 0,
                kind: 'publish_upload',
                status: 'recovery_required',
                error: null,
                result: null,
              },
            ],
          },
          tasks: [
            {
              files: [],
              steps: [
                { label: 'check sha256 photo.jpg', status: 'done' },
                { label: 'Record changes', status: 'running' },
              ],
            },
          ],
        } as never;
      return { rows: [OP], total: 1 } as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue(undefined as never);
    const page = await mount('ism-operations-page');
    await vi.waitFor(() => expect(page.textContent).toContain('OPM: editor'));
    page.querySelector<HTMLButtonElement>('ism-operation-table button')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('Record changes'));
    [...page.querySelectorAll('button')]
      .find((button) => button.textContent?.includes('Recheck files'))
      ?.click();
    await vi.waitFor(() => expect(post).toHaveBeenCalledWith('/api/admin/operations/op1/recheck'));
  });
});
