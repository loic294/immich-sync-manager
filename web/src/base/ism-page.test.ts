import { html } from 'lit';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { events } from '@/api/events';
import { IsmPage } from '@/base/ism-page';
import { mount } from '@/test/helpers';

class LivePage extends IsmPage {
  protected override reloadOn = ['sync_changed'] as const;
  loads = 0;
  wait: Promise<void> = Promise.resolve();
  protected override async load() {
    this.loads++;
    await this.wait;
  }
  protected override render() {
    return html``;
  }
}
customElements.define('test-live-page', LivePage);

describe('live page loading', () => {
  afterEach(() => vi.useRealTimers());
  it('refreshes during continuous events rather than waiting until transfers stop', async () => {
    vi.useFakeTimers();
    const page = await mount(new LivePage());
    await page.reload();
    const before = page.loads;
    for (let i = 0; i < 10; i++) {
      events.emit({ type: 'sync_changed' });
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(page.loads - before).toBeGreaterThanOrEqual(3);
  });

  it('serializes refreshes and reloads again after an in-flight request', async () => {
    const page = await mount(new LivePage());
    await page.reload();
    let release: (() => void) | undefined;
    page.wait = new Promise((resolve) => {
      release = resolve;
    });
    const before = page.loads;
    const first = page.reload();
    const second = page.reload();
    expect(page.loads).toBe(before + 1);
    release?.();
    await Promise.all([first, second]);
    expect(page.loads).toBe(before + 2);
  });
});
