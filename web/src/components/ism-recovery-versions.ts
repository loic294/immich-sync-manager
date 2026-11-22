import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import type { RecoveryVersion } from '@/api/generated/RecoveryVersion';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import { IsmElement } from '@/base/ism-element';
import { formatBytes, formatDate } from '@/util/format';

export class IsmRecoveryVersions extends IsmElement {
  static override properties = {
    target: { attribute: false },
    versions: { attribute: false },
    failed: { state: true },
    limit: { state: true },
  };
  declare target: RecoveryTarget | undefined;
  declare versions: RecoveryVersions | undefined;
  declare failed: Set<string>;
  declare limit: number;

  constructor() {
    super();
    this.failed = new Set();
    this.limit = 10;
  }

  private side(version: RecoveryVersion, source: boolean) {
    const target = this.target;
    const versions = this.versions;
    if (!target || !versions) return nothing;
    return html`<section class="min-w-0">
      <h3 class="font-semibold">${version.instance}</h3>
      <p class="break-all text-sm text-base-content/70">${version.path}</p>
      ${
        version.state === 'absent' || version.state === 'deleted'
          ? html`<p class="mt-2">${version.state === 'deleted' ? 'Deleted' : 'No file at this location'}</p>`
          : html`<p class="mt-2 text-sm">${version.files.length} ${version.files.length === 1 ? 'file' : 'files'} ·
            ${formatBytes(version.files.reduce((sum, file) => sum + (file.size ?? 0), 0))}
            ${latest(version) ? html` · Last modified ${formatDate(latest(version))}` : nothing}</p>
          <ul class="mt-3 space-y-4">${version.files.slice(0, this.limit).map((file) => {
            const image = /\.(jpe?g|png|webp|avif)$/i.test(file.path);
            const id = `${source}:${file.path}:${file.sha256}`;
            const other = source
              ? versions.receiving.flatMap((side) => side.files)
              : versions.source.files;
            const same = other.some((candidate) => candidate.sha256 === file.sha256);
            return html`<li class="min-w-0">
              <p class="break-all text-sm font-medium">${file.path}</p>
              ${
                image && !this.failed.has(id)
                  ? html`<img class="mt-2 max-h-48 w-full object-contain object-left" loading="lazy"
                    alt=${`${version.instance}: ${file.path}`}
                    src=${admin.recoveryPreview(target, versions.revision, file.path, source)}
                    @error=${() => {
                      this.failed = new Set([...this.failed, id]);
                    }}>`
                  : image
                    ? html`<p class="mt-2 text-sm text-base-content/70">Preview unavailable</p>`
                    : nothing
              }
              <p class="mt-1 text-sm text-base-content/70">${file.role} ·
                ${file.size === null ? 'Size unavailable' : formatBytes(file.size)}
                ${file.modified_at ? html` · Modified ${formatDate(file.modified_at)}` : nothing}</p>
              <p class="text-sm">${same ? 'Same bytes on both instances' : 'Different version or only on this instance'}</p>
              <details class="mt-1 text-xs text-base-content/70"><summary class="cursor-pointer">Checksum</summary>
                <p class="break-all">${file.sha256}</p></details>
            </li>`;
          })}</ul>
          ${
            version.directories.length > 1
              ? html`<details class="mt-3 text-sm"><summary class="cursor-pointer">Folders</summary>
                  <ul>${version.directories
                    .filter((path) => path !== version.path)
                    .slice(0, this.limit)
                    .map((path) => html`<li class="break-all">${path}</li>`)}</ul></details>`
              : nothing
          }`
      }
    </section>`;
  }

  protected override render() {
    if (!this.versions) return nothing;
    return html`<div class="mt-3 border-t border-base-300 pt-3">
      <div class="grid min-w-0 grid-cols-1 gap-5 md:grid-cols-2">
        <div class="min-w-0 space-y-4">${this.versions.receiving.map((version) => this.side(version, false))}</div>
        ${this.side(this.versions.source, true)}
      </div>
      ${
        Math.max(
          this.versions.source.files.length,
          this.versions.source.directories.length - 1,
          ...this.versions.receiving.map((version) => version.files.length),
          ...this.versions.receiving.map((version) => version.directories.length - 1),
        ) > this.limit
          ? html`<button class="btn btn-sm mt-3 max-sm:min-h-11" @click=${() => {
              this.limit += 10;
            }}>Show more files</button>`
          : nothing
      }
    </div>`;
  }
}

customElements.define('ism-recovery-versions', IsmRecoveryVersions);

/** The most recent modification time among a version's files. */
function latest(version: RecoveryVersion) {
  return version.files.reduce<string | undefined>((newest, file) => {
    if (!file.modified_at) return newest;
    return !newest || Date.parse(file.modified_at) > Date.parse(newest) ? file.modified_at : newest;
  }, undefined);
}

declare global {
  interface HTMLElementTagNameMap {
    'ism-recovery-versions': IsmRecoveryVersions;
  }
}
