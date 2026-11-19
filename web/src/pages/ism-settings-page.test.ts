import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { SettingsView } from '@/api/generated/SettingsView';
import { mount } from '@/test/helpers';
import '@/pages/ism-settings-page';

const SELF = 'instance-a';
const PEER = 'instance-b';

function view(primary: string | null): SettingsView {
  const candidates = [
    { instance_id: SELF, name: 'Home', this_instance: true },
    { instance_id: PEER, name: 'Office', this_instance: false },
  ];
  const chosen = candidates.find((c) => c.instance_id === primary);
  return {
    instance_id: SELF,
    instance_name: 'Home',
    files: {
      sidecar_extensions: ['xmp', 'json'],
      junk_names: ['Thumbs.db'],
      junk_prefixes: ['._'],
      name_max_len: 255,
    },
    files_revision: 1,
    primary: chosen ? { instance_id: chosen.instance_id, name: chosen.name } : null,
    files_editable: primary === null || primary === SELF,
    primary_candidates: candidates,
  };
}

const filesSection = (page: HTMLElement) =>
  page.querySelector('fieldset[data-section="files"]') as HTMLFieldSetElement;
const saveButton = (page: HTMLElement) =>
  [...page.querySelectorAll('button')].find((b) => b.textContent?.trim() === 'Save');

describe('ism-settings-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('shows the primary instance values read-only', async () => {
    vi.spyOn(api, 'get').mockResolvedValue(view(PEER) as never);
    const page = await mount('ism-settings-page');
    await vi.waitFor(() => expect(filesSection(page)).not.toBeNull());

    expect(filesSection(page).disabled).toBe(true);
    expect(page.textContent).toContain('Managed by the primary instance');
    expect(page.textContent).toContain('The file settings come from Office');
    const chips = [...filesSection(page).querySelectorAll('li span')].map((s) => s.textContent);
    expect(chips).toEqual(['xmp', 'json', 'Thumbs.db', '._']);
  });

  it('saves edited file settings while no instance is primary', async () => {
    vi.spyOn(api, 'get').mockResolvedValue(view(null) as never);
    const put = vi.spyOn(api, 'put').mockResolvedValue(view(null) as never);
    const page = await mount('ism-settings-page');
    await vi.waitFor(() => expect(filesSection(page)).not.toBeNull());
    expect(filesSection(page).disabled).toBe(false);
    expect(saveButton(page)?.disabled).toBe(true);

    const extensions = page.querySelector(
      'ism-chip-input[data-field="sidecar_extensions"] input',
    ) as HTMLInputElement;
    extensions.value = 'AAE,';
    extensions.dispatchEvent(new InputEvent('input', { bubbles: true }));
    await vi.waitFor(() => expect(saveButton(page)?.disabled).toBe(false));
    saveButton(page)?.click();

    await vi.waitFor(() =>
      expect(put).toHaveBeenCalledWith('/api/admin/settings', {
        body: {
          instance_name: 'Home',
          primary_instance_id: null,
          files: {
            sidecar_extensions: ['xmp', 'json', 'aae'],
            junk_names: ['Thumbs.db'],
            junk_prefixes: ['._'],
            name_max_len: 255,
          },
        },
      }),
    );
  });

  it('does not send file settings when another instance becomes primary', async () => {
    vi.spyOn(api, 'get').mockResolvedValue(view(null) as never);
    const put = vi.spyOn(api, 'put').mockResolvedValue(view(PEER) as never);
    const page = await mount('ism-settings-page');
    await vi.waitFor(() => expect(filesSection(page)).not.toBeNull());

    const select = page.querySelector('#settings-primary') as HTMLSelectElement;
    select.value = PEER;
    select.dispatchEvent(new Event('change', { bubbles: true }));
    await vi.waitFor(() => expect(filesSection(page).disabled).toBe(true));
    saveButton(page)?.click();

    await vi.waitFor(() =>
      expect(put).toHaveBeenCalledWith('/api/admin/settings', {
        body: { instance_name: 'Home', primary_instance_id: PEER, files: null },
      }),
    );
  });
});
