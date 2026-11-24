import { html } from 'lit';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events, type Snapshot } from '@/api/events';
import { IsmPage } from '@/base/ism-page';
import { mount } from '@/test/helpers';

class LivePage extends IsmPage {
  loads = 0;
  wait: Promise<void> = Promise.resolve();
  protected override async load() {
    this.loads++;
    await this.data.sync();
    await this.wait;
  }
  protected override render() {
    return html``;
  }
}
customElements.define('test-live-page', LivePage);

describe('live page loading', () => {
  let push: ((snapshot: Snapshot | Error) => void) | undefined;
  beforeEach(() => {
    vi.spyOn(api, 'get').mockResolvedValue({});
    vi.spyOn(events, 'subscribe').mockImplementation((_url, handler) => {
      push = handler;
      return () => {
        push = undefined;
      };
    });
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });
  it('renders pushed state without making HTTP requests on notification bursts', async () => {
    vi.useFakeTimers();
    const page = await mount(new LivePage());
    await page.reload();
    const before = page.loads;
    vi.mocked(api.get).mockClear();
    for (let i = 0; i < 10; i++) {
      events.emit({ type: 'sync_changed' });
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(page.loads).toBe(before);
    expect(api.get).not.toHaveBeenCalled();
    push?.({ status: 200, data: { allowed_now: true } });
    await vi.advanceTimersByTimeAsync(0);
    expect(page.loads).toBe(before + 1);
    expect(api.get).not.toHaveBeenCalled();
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

  it('retains an explicit live error until a pushed snapshot restores the view', async () => {
    const page = await mount(new LivePage());
    await page.reload();
    vi.mocked(api.get).mockClear();
    push?.(new Error('Live connection lost'));
    await vi.waitFor(() => expect(page.loadError).toBe('Live connection lost'));
    expect(api.get).not.toHaveBeenCalled();
    push?.({ status: 200, data: {} });
    await vi.waitFor(() => expect(page.loadError).toBeUndefined());
    expect(api.get).not.toHaveBeenCalled();
  });
});
