import { html, nothing, type TemplateResult } from 'lit';
import { live } from 'lit/directives/live.js';
import { admin } from '@/api/admin';
import type { ChangeType } from '@/api/events';
import type { FileSettings } from '@/api/generated/FileSettings';
import type { SettingsView } from '@/api/generated/SettingsView';
import { IsmPage } from '@/base/ism-page';
import type { ChipValuesChange } from '@/components/ism-chip-input';
import { refreshSession } from '@/stores/session';
import {
  ENCRYPTED_SHARE_NAME_LEN,
  isValidNameMaxLen,
  MAX_JUNK_ENTRIES,
  MAX_SIDECAR_EXTENSIONS,
  NAME_MAX_LEN,
  normalizeExtension,
  normalizeJunkEntry,
  validateExtension,
  validateJunkName,
  validateJunkPrefix,
} from '@/util/file-settings';
import '@/components/ism-chip-input';
import '@/components/ism-icon';

type ListField = 'sidecar_extensions' | 'junk_names' | 'junk_prefixes';

interface ListSpec {
  label: string;
  placeholder: string;
  hint: string;
  max: number;
  allowSpaces: boolean;
  normalize: (raw: string) => string;
  validate: (entry: string) => string | null;
}

const LIST_FIELDS: readonly ListField[] = ['sidecar_extensions', 'junk_names', 'junk_prefixes'];
const LISTS: Record<ListField, ListSpec> = {
  sidecar_extensions: {
    label: 'Sidecar extensions',
    placeholder: 'xmp',
    hint: 'Companion files that travel with a photo, for example IMG_1.xmp. Type them without the dot.',
    max: MAX_SIDECAR_EXTENSIONS,
    allowSpaces: false,
    normalize: normalizeExtension,
    validate: validateExtension,
  },
  junk_names: {
    label: 'Ignored names',
    placeholder: 'Thumbs.db',
    hint: 'Files and folders with these names are never watched, replicated to peers or indexed. Case does not matter.',
    max: MAX_JUNK_ENTRIES,
    allowSpaces: true,
    normalize: normalizeJunkEntry,
    validate: validateJunkName,
  },
  junk_prefixes: {
    label: 'Ignored prefixes',
    placeholder: '._',
    hint: 'The same for names that start with this text, for example ._ for macOS resource forks.',
    max: MAX_JUNK_ENTRIES,
    allowSpaces: true,
    normalize: normalizeJunkEntry,
    validate: validateJunkPrefix,
  },
};

const CARD = 'fieldset gap-4 rounded-box border border-base-300 bg-base-200 p-4 text-sm';
const NOTE_LISTS =
  'Applies to the next watcher pass and peer sync. Open Photo Manager clients use the same values while they are connected.';

function note(text: string | TemplateResult) {
  return html`
    <p class="flex items-start gap-2 text-xs text-base-content/70">
      <ism-icon name="info" size="xs" class="mt-0.5"></ism-icon>
      <span>${text}</span>
    </p>
  `;
}

/**
 * Instance name, the primary instance and the "Files and sidecars" settings it shares with the
 * other instances and with Open Photo Manager. While another instance is primary, its values are
 * shown read-only.
 */
export class IsmSettingsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    view: { state: true },
    name: { state: true },
    primary: { state: true },
    files: { state: true },
    dirty: { state: true },
  };

  declare view: SettingsView | undefined;
  declare name: string;
  /** The chosen primary's instance ID; empty for none. */
  declare primary: string;
  declare files: FileSettings | undefined;
  /** The form holds edits not saved yet: a reload keeps them. */
  declare dirty: boolean;

  protected override reloadOn: readonly ChangeType[] = ['settings_changed'];

  constructor() {
    super();
    this.view = undefined;
    this.name = '';
    this.primary = '';
    this.files = undefined;
    this.dirty = false;
  }

  protected override async load() {
    const view = await admin.settings();
    this.view = view;
    if (!this.dirty) {
      this.name = view.instance_name;
      this.primary = view.primary?.instance_id ?? '';
      this.files = view.files;
    } else if (!this.editable) {
      this.files = view.files;
    }
  }

  /** The file settings can be changed with the primary chosen in the form. */
  private get editable(): boolean {
    return this.primary === '' || this.primary === this.view?.instance_id;
  }

  private change(update: () => void) {
    update();
    this.dirty = true;
  }

  private setFiles(patch: Partial<FileSettings>) {
    const files = this.files;
    if (files) this.change(() => (this.files = { ...files, ...patch }));
  }

  private setList(field: ListField, values: string[]) {
    const files = this.files;
    if (!files) return;
    const next = { ...files };
    next[field] = values;
    this.change(() => (this.files = next));
  }

  private async onSubmit(event: SubmitEvent) {
    event.preventDefault();
    const files = this.files;
    if (!files || !isValidNameMaxLen(files.name_max_len)) return;
    const body = {
      instance_name: this.name.trim(),
      primary_instance_id: this.primary === '' ? null : this.primary,
      files: this.editable ? files : null,
    };
    const saved = await this.act(
      'save',
      async () => {
        await admin.saveSettings(body);
        this.dirty = false;
      },
      'Settings saved.',
    );
    if (saved) await refreshSession();
  }

  private renderInstance(view: SettingsView) {
    const current = view.primary?.instance_id ?? '';
    const chosen = view.primary_candidates.find((c) => c.instance_id === this.primary);
    let primaryHint: string;
    if (this.primary === '') {
      primaryHint = 'Every instance keeps its own file settings.';
    } else if (this.primary !== current) {
      primaryHint = chosen?.this_instance
        ? 'Saving makes this instance the reference: its file settings are copied to the paired instances.'
        : `Saving makes ${chosen?.name ?? 'that instance'} the reference: its file settings replace the ones below.`;
    } else {
      primaryHint = chosen?.this_instance
        ? 'The file settings below are copied to the paired instances and to Open Photo Manager.'
        : `The file settings come from ${chosen?.name ?? 'that instance'}. Change them there.`;
    }
    return html`
      <fieldset class=${CARD} ?disabled=${this.busy !== undefined}>
        <legend class="fieldset-legend text-base">This instance</legend>
        <div class="flex flex-col gap-1">
          <label for="settings-instance-name" class="text-sm font-medium">Instance name</label>
          <input
            id="settings-instance-name"
            class="input w-full max-w-md"
            required
            maxlength="64"
            .value=${live(this.name)}
            @input=${(e: Event) => this.change(() => (this.name = (e.target as HTMLInputElement).value))}
          />
          <p class="text-xs text-base-content/70">Shown to peers and in the top bar.</p>
        </div>
        <div class="flex flex-col gap-1">
          <label for="settings-primary" class="text-sm font-medium">Primary instance</label>
          <select
            id="settings-primary"
            class="select w-full max-w-md"
            @change=${(e: Event) => this.change(() => (this.primary = (e.target as HTMLSelectElement).value))}
          >
            <option value="" ?selected=${this.primary === ''}>None: each instance keeps its own</option>
            ${view.primary_candidates.map(
              (
                c,
              ) => html`<option value=${c.instance_id} ?selected=${this.primary === c.instance_id}>
                ${c.this_instance ? `${c.name} (this instance)` : c.name}
              </option>`,
            )}
          </select>
          <p class="text-xs text-base-content/70">${primaryHint}</p>
        </div>
      </fieldset>
    `;
  }

  private renderFiles(files: FileSettings) {
    const disabled = !this.editable || this.busy !== undefined;
    const nameMaxLenValid = isValidNameMaxLen(files.name_max_len);
    return html`
      <fieldset class=${CARD} ?disabled=${disabled} data-section="files">
        <legend class="fieldset-legend text-base">Files and sidecars</legend>
        ${
          this.editable
            ? nothing
            : html`<div role="status" class="alert alert-info alert-soft">
                <ism-icon name="lock"></ism-icon>
                <span>Managed by the primary instance. These are its current values.</span>
              </div>`
        }
        ${LIST_FIELDS.map((field) => {
          const spec = LISTS[field];
          return html`
            <ism-chip-input
              data-field=${field}
              label=${spec.label}
              placeholder=${spec.placeholder}
              hint=${spec.hint}
              ?allow-spaces=${spec.allowSpaces}
              .values=${files[field]}
              .max=${spec.max}
              .normalizeEntry=${spec.normalize}
              .validateEntry=${spec.validate}
              .disabled=${disabled}
              @values-change=${(e: ChipValuesChange) => this.setList(field, e.detail.values)}
            ></ism-chip-input>
          `;
        })}
        ${note(NOTE_LISTS)}
        <div class="flex flex-col gap-1">
          <label for="settings-name-max-len" class="text-sm font-medium">Longest generated name</label>
          <div class="input w-40 ${nameMaxLenValid ? '' : 'validator'}">
            <input
              id="settings-name-max-len"
              data-field="name_max_len"
              type="number"
              inputmode="numeric"
              min=${NAME_MAX_LEN.min}
              max=${NAME_MAX_LEN.max}
              step="1"
              .value=${live(Number.isNaN(files.name_max_len) ? '' : String(files.name_max_len))}
              ?disabled=${disabled}
              aria-invalid=${nameMaxLenValid ? nothing : 'true'}
              aria-describedby="settings-name-max-len-message"
              @input=${(e: Event) => {
                const raw = (e.target as HTMLInputElement).value.trim();
                this.setFiles({ name_max_len: raw === '' ? Number.NaN : Number(raw) });
              }}
            />
            <span class="label">bytes</span>
          </div>
          <p
            id="settings-name-max-len-message"
            class=${nameMaxLenValid ? 'text-xs text-base-content/70' : 'validator-hint mt-0'}
            role=${nameMaxLenValid ? nothing : 'alert'}
          >
            ${
              nameMaxLenValid
                ? `Names Open Photo Manager generates, such as name_1.jpg, stay within this length. Encrypted shares only allow ${ENCRYPTED_SHARE_NAME_LEN}.`
                : `Use a whole number from ${NAME_MAX_LEN.min} to ${NAME_MAX_LEN.max}.`
            }
          </p>
        </div>
      </fieldset>
    `;
  }

  protected override render() {
    return this.frame(() => {
      const view = this.view;
      const files = this.files;
      if (!view || !files) return html``;
      const valid = isValidNameMaxLen(files.name_max_len) && this.name.trim() !== '';
      return html`
        <form class="flex flex-col gap-4" @submit=${this.onSubmit}>
          ${this.renderInstance(view)} ${this.renderFiles(files)}
          <div class="flex justify-end">
            <button class="btn btn-primary" ?disabled=${this.busy !== undefined || !valid || !this.dirty}>
              ${this.spin('save')}Save
            </button>
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
