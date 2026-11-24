import { afterEach, describe, expect, it } from 'vitest';
import type { AttentionItem } from '@/stores/attention';
import { attentionError, blockers, notifications } from '@/stores/attention';
import { mount } from '@/test/helpers';
import '@/components/ism-attention-bell';
import '@/components/ism-blocker-banners';

const item: AttentionItem = {
  id: 'conflicts:one',
  code: 'recovery_decision',
  owner: 'local',
  title: '1 file needs a choice',
  detail: 'Choose which version to keep.',
  href: '/conflicts',
  action: 'Open conflicts',
  external: false,
  blocking: true,
  notifiable: true,
};

afterEach(() => {
  blockers.value = [];
  notifications.value = [];
  attentionError.value = false;
  localStorage.clear();
});

describe('admin attention', () => {
  it('dismisses one or all notices without removing the active banner', async () => {
    blockers.value = [item];
    notifications.value = [item, { ...item, id: 'other', title: 'Another issue' }];
    const bell = await mount('ism-attention-bell');
    const banners = await mount('ism-blocker-banners');
    bell.querySelector<HTMLButtonElement>('[aria-label="Dismiss: 1 file needs a choice"]')?.click();
    await bell.updateComplete;
    expect(bell.textContent).not.toContain('1 file needs a choice');
    expect(bell.textContent).toContain('Another issue');
    expect(banners.textContent).toContain('1 file needs a choice');
    [...bell.querySelectorAll('button')]
      .find((button) => button.textContent?.includes('Dismiss all'))
      ?.click();
    await bell.updateComplete;
    expect(bell.textContent).toContain('all caught up');
    expect(banners.querySelector('a')?.getAttribute('href')).toBe('/conflicts');
  });

  it('warns when status cannot be checked instead of claiming everything is clear', async () => {
    attentionError.value = true;
    const bell = await mount('ism-attention-bell');
    const banners = await mount('ism-blocker-banners');
    expect(bell.textContent).not.toContain('all caught up');
    expect(bell.textContent).toContain('Some notification information is unavailable.');
    expect(banners.textContent).toContain('status information is unavailable');
    expect(banners.querySelector('button')?.textContent).toContain('Retry');
  });

  it('lets receiver links navigate outside the app router', async () => {
    const remote = { ...item, href: 'https://receiver.example/conflicts', external: true };
    blockers.value = [remote];
    notifications.value = [remote];
    const bell = await mount('ism-attention-bell');
    const banners = await mount('ism-blocker-banners');
    expect(bell.querySelector('a')?.getAttribute('rel')).toBe('external');
    expect(banners.querySelector('a')?.getAttribute('rel')).toBe('external');
  });
});
