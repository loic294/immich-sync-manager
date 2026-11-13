import { html } from 'lit';
import { admin, type Settings } from '@/api/admin';
import { IsmPage } from '@/base/ism-page';
import { refreshSession } from '@/stores/session';

/** Instance name and the sidecar extensions that travel with an asset. */
export class IsmSettingsPage extends IsmPage {
  static override properties = { ...IsmPage.properties, settings: { state: true } };

  declare settings: Settings | undefined;

  protected override async load() {
    this.settings = await admin.settings();
  }

  private async onSubmit(event: SubmitEvent) {
    event.preventDefault();
    const form = new FormData(event.target as HTMLFormElement);
    const body: Settings = {
      instance_name: String(form.get('instance_name') ?? '').trim(),
      companion_extensions: String(form.get('companion_extensions') ?? '')
        .split(/[\s,]+/)
        .map((ext) => ext.trim().replace(/^\./, '').toLowerCase())
        .filter(Boolean),
    };
    if (await this.act('save', () => admin.saveSettings(body), 'Settings saved.')) {
      await refreshSession();
    }
  }

  protected override render() {
    return this.frame(() => {
      const s = this.settings;
      if (!s) return html``;
      return html`
        <form class="card card-border bg-base-100" @submit=${this.onSubmit}>
          <div class="card-body gap-4">
            <h2 class="card-title text-base">This instance</h2>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Instance name</legend>
              <input name="instance_name" class="input w-full max-w-md" required maxlength="64" .value=${s.instance_name} />
              <p class="label">Shown to peers and in the top bar.</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Companion file extensions</legend>
              <input name="companion_extensions" class="input w-full max-w-md" .value=${s.companion_extensions.join(', ')} />
              <p class="label">
                Files such as <code>IMG_0001.jpg.xmp</code> or <code>IMG_0001.xmp</code> move, rename, download and
                sync together with their asset.
              </p>
            </fieldset>
            <div class="card-actions">
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('save')}Save</button>
            </div>
          </div>
        </form>
      `;
    });
  }
}

customElements.define('ism-settings-page', IsmSettingsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-settings-page': IsmSettingsPage;
  }
}
