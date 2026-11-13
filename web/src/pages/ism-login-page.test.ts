import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { mount } from '@/test/helpers';
import '@/pages/ism-login-page';

function fill(page: HTMLElement, values: Record<string, string>) {
  for (const [name, value] of Object.entries(values)) {
    const input = page.querySelector<HTMLInputElement>(`input[name="${name}"]`);
    if (!input) throw new Error(`no input ${name}`);
    input.value = value;
  }
  page.querySelector('form')?.requestSubmit();
}

describe('ism-login-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('rejects mismatched passwords during setup without calling the server', async () => {
    const post = vi.spyOn(api, 'post');
    const page = await mount('ism-login-page', { setup: true });
    fill(page, {
      instance_name: 'A',
      username: 'admin',
      password: 'long-enough-1',
      confirm: 'different-12',
    });
    await page.updateComplete;
    expect(page.textContent).toContain('do not match');
    expect(post).not.toHaveBeenCalled();
  });

  it('signs in and re-reads the session', async () => {
    const post = vi.spyOn(api, 'post').mockResolvedValue(undefined as never);
    const get = vi.spyOn(api, 'get').mockResolvedValue({ authenticated: true } as never);
    const page = await mount('ism-login-page');
    fill(page, { username: 'admin', password: 'secret-password' });
    await vi.waitFor(() => expect(get).toHaveBeenCalledWith('/api/admin/session'));
    expect(post).toHaveBeenCalledWith('/api/admin/login', {
      body: { username: 'admin', password: 'secret-password' },
    });
  });
});
