import { describe, expect, it, vi } from 'vitest';
import { mount } from '@/test/helpers';
import '@/components/ism-pagination';

describe('pagination', () => {
  it('provides the requested page sizes and correct last-page bounds', async () => {
    const pagination = await mount('ism-pagination', { total: 501, offset: 250 });
    const change = vi.fn();
    pagination.addEventListener('page-change', change);
    const options = [...pagination.querySelectorAll('option')].map((o) => Number(o.value));
    expect(options).toEqual([25, 50, 100, 250, 500, 1000, 2500, 5000]);
    expect(pagination.querySelector('select')?.value).toBe('250');
    pagination.querySelector<HTMLButtonElement>('[aria-label="Last page"]')?.click();
    expect(change.mock.calls[0]?.[0].detail).toEqual({ offset: 500, limit: 250 });
    pagination.offset = 500;
    await pagination.updateComplete;
    expect(pagination.textContent).toContain('501–501 of 501');
    expect(pagination.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.disabled).toBe(
      true,
    );
    const select = pagination.querySelector('select');
    if (select) {
      select.value = '25';
      select.dispatchEvent(new Event('change'));
    }
    expect(change.mock.calls[1]?.[0].detail).toEqual({ offset: 0, limit: 25 });
  });
});
