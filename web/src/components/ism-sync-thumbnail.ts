import { html, nothing, type PropertyValues } from 'lit';
import { admin } from '@/api/admin';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import { IsmElement } from '@/base/ism-element';
import '@/components/ism-icon';

const MEDIA =
  /\.(jpe?g|png|gif|webp|heic|heif|avif|dng|cr[23]|nef|arw|raf|orf|rw2|tiff?|mp4|mov|m4v|avi|mkv|webm|mts)$/i;

export class IsmSyncThumbnail extends IsmElement {
  static override properties = { transfer: { attribute: false }, failed: { state: true } };
  declare transfer: SyncActivity;
  declare failed: boolean;
  constructor() {
    super();
    this.failed = false;
  }
  protected override willUpdate(changes: PropertyValues) {
    const previous = changes.get('transfer') as SyncActivity | undefined;
    if (
      changes.has('transfer') &&
      (previous?.id !== this.transfer?.id ||
        (previous?.status === 'completed') !== (this.transfer?.status === 'completed'))
    )
      this.failed = false;
  }
  protected override render() {
    const t = this.transfer;
    if (!t) return nothing;
    const media = MEDIA.test(t.relative_path);
    const extension = t.relative_path.split('.').at(-1)?.toUpperCase() ?? 'FILE';
    const local = t.outgoing || t.status === 'completed';
    return html`<div class="flex size-12 shrink-0 flex-col items-center justify-center overflow-hidden rounded bg-base-200"
      title=${this.failed && media ? 'Preview unavailable. The file may not be indexed in Immich yet.' : t.relative_path}>
      ${
        media && !this.failed
          ? html`<img class="size-full object-cover" loading="lazy" decoding="async" alt=""
          src=${admin.syncThumbnail(t.share_name, t.relative_path, local ? undefined : t.peer_id)}
          @error=${() => {
            this.failed = true;
          }} />`
          : html`<ism-icon name=${media ? 'images' : 'file'} size="sm"></ism-icon><span class="max-w-12 truncate text-xs">${extension}</span>`
      }
    </div>`;
  }
}
customElements.define('ism-sync-thumbnail', IsmSyncThumbnail);
declare global {
  interface HTMLElementTagNameMap {
    'ism-sync-thumbnail': IsmSyncThumbnail;
  }
}
