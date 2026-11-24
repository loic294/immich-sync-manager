import { html, nothing, type PropertyValues } from 'lit';
import { admin } from '@/api/admin';
import { parseErrorBody } from '@/api/errors';
import { events } from '@/api/events';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import type { RecoveryVersion } from '@/api/generated/RecoveryVersion';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import { IsmElement } from '@/base/ism-element';
import { formatBytes, formatDate } from '@/util/format';

export interface VersionChoices {
  disabled: boolean;
  receivingLabel: string;
  sourceLabel: string;
  choose: (source: boolean) => void;
}

interface Preview {
  url: string;
  state: 'idle' | 'queued' | 'loading' | 'ready' | 'error';
  objectUrl?: string;
  message?: string;
  retryable?: boolean;
  controller?: AbortController;
  refresh?: boolean;
}

const PREVIEW_CONCURRENCY = 4;
const PREVIEW_RETRY_MS = 60_000;

export class IsmRecoveryVersions extends IsmElement {
  static override properties = {
    target: { attribute: false },
    versions: { attribute: false },
    choices: { attribute: false },
    limit: { state: true },
  };
  declare target: RecoveryTarget | undefined;
  declare versions: RecoveryVersions | undefined;
  declare choices: VersionChoices | undefined;
  declare limit: number;
  private readonly previews = new Map<string, Preview>();
  private readonly visible = new Set<string>();
  private readonly observed = new Map<HTMLElement, string>();
  private readonly queue: Preview[] = [];
  private active = 0;
  private observer: IntersectionObserver | undefined;
  private unsubscribe: (() => void) | undefined;
  private retryTimer: number | undefined;
  private refreshTimer: number | undefined;
  private readonly visibilityChanged = () => {
    if (document.visibilityState === 'hidden') {
      window.clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    } else {
      this.refreshVisible();
      this.updateRetryTimer();
    }
  };

  constructor() {
    super();
    this.limit = 10;
  }

  override connectedCallback() {
    super.connectedCallback();
    if (typeof IntersectionObserver !== 'undefined') {
      this.observer = new IntersectionObserver((entries) => {
        for (const entry of entries) {
          const url = (entry.target as HTMLElement).dataset.preview;
          if (!url) continue;
          if (entry.isIntersecting) {
            this.visible.add(url);
            this.enqueue(url);
          } else {
            this.hide(url);
          }
        }
        this.updateRetryTimer();
      });
    }
    this.unsubscribe = events.on((event) => {
      if (event.type !== 'sync_changed' && event.type !== 'resync') return;
      window.clearTimeout(this.refreshTimer);
      this.refreshTimer = window.setTimeout(() => this.refreshVisible(), 1500);
    });
    document.addEventListener('visibilitychange', this.visibilityChanged);
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    this.unsubscribe?.();
    this.observer?.disconnect();
    this.observer = undefined;
    this.observed.clear();
    this.visible.clear();
    document.removeEventListener('visibilitychange', this.visibilityChanged);
    window.clearTimeout(this.retryTimer);
    this.retryTimer = undefined;
    window.clearTimeout(this.refreshTimer);
    for (const preview of this.previews.values()) this.dispose(preview);
    this.previews.clear();
    this.queue.length = 0;
  }

  protected override willUpdate(changed: PropertyValues<this>) {
    if (!changed.has('target') && !changed.has('versions') && !changed.has('limit')) return;
    const target = this.target;
    const versions = this.versions;
    const urls = new Set<string>();
    if (target && versions) {
      for (const [source, sides] of [
        [false, versions.receiving],
        [true, [versions.source]],
      ] as const) {
        for (const side of sides) {
          for (const file of side.files.slice(0, this.limit)) {
            if (/\.(jpe?g|png|webp|avif)$/i.test(file.path))
              urls.add(admin.recoveryPreview(target, versions.revision, file.path, source));
          }
        }
      }
    }
    for (const [url, preview] of this.previews) {
      if (urls.has(url)) continue;
      this.dispose(preview);
      this.previews.delete(url);
      this.visible.delete(url);
    }
    for (const url of urls) {
      if (!this.previews.has(url)) this.previews.set(url, { url, state: 'idle' });
    }
  }

  protected override updated() {
    const current = new Set(this.querySelectorAll<HTMLElement>('[data-preview]'));
    const urls = new Set([...current].map((element) => element.dataset.preview));
    for (const [element, url] of this.observed) {
      if (current.has(element) && element.dataset.preview === url) continue;
      this.observer?.unobserve(element);
      this.observed.delete(element);
    }
    for (const url of this.visible) {
      if (!urls.has(url)) this.hide(url);
    }
    for (const element of current) {
      if (this.observed.has(element)) continue;
      if (!element.dataset.preview) continue;
      this.observed.set(element, element.dataset.preview);
      if (this.observer) this.observer.observe(element);
      else {
        this.visible.add(element.dataset.preview);
        this.enqueue(element.dataset.preview);
      }
    }
  }

  private hide(url: string) {
    this.visible.delete(url);
    const preview = this.previews.get(url);
    if (!preview) return;
    if (preview.state === 'loading') {
      preview.refresh = false;
      preview.controller?.abort();
    } else if (preview.state === 'queued') {
      this.dispose(preview);
      preview.state = 'idle';
    }
    this.updateRetryTimer();
  }

  private dispose(preview: Preview) {
    preview.controller?.abort();
    if (preview.objectUrl) URL.revokeObjectURL(preview.objectUrl);
    preview.objectUrl = undefined;
    const index = this.queue.indexOf(preview);
    if (index >= 0) this.queue.splice(index, 1);
  }

  private enqueue(url: string) {
    const preview = this.previews.get(url);
    if (preview?.state !== 'idle') return;
    preview.state = 'queued';
    this.queue.push(preview);
    this.pump();
  }

  private pump() {
    while (this.active < PREVIEW_CONCURRENCY && this.queue.length) {
      const preview = this.queue.shift();
      if (!preview || this.previews.get(preview.url) !== preview) continue;
      this.active++;
      preview.state = 'loading';
      void this.fetchPreview(preview).finally(() => {
        this.active--;
        if (this.previews.get(preview.url) === preview && preview.refresh) {
          preview.refresh = false;
          preview.state = 'idle';
          if (this.visible.has(preview.url)) this.enqueue(preview.url);
        } else if (this.previews.get(preview.url) === preview && preview.state === 'idle') {
          if (this.visible.has(preview.url)) this.enqueue(preview.url);
        }
        this.pump();
        this.updateRetryTimer();
        this.requestUpdate();
      });
    }
  }

  private async fetchPreview(preview: Preview) {
    const controller = new AbortController();
    preview.controller = controller;
    try {
      const response = await fetch(preview.url, { signal: controller.signal });
      if (!response.ok) {
        const error = response.headers.get('content-type')?.includes('application/json')
          ? parseErrorBody(await response.json())
          : null;
        preview.retryable = ['preview_outside_transfer_hours', 'preview_busy'].includes(
          error?.code ?? '',
        );
        preview.message =
          error?.code === 'preview_outside_transfer_hours'
            ? 'Preview available during allowed transfer hours.'
            : (error?.message ?? 'Preview unavailable.');
        preview.state = 'error';
        return;
      }
      const blob = await response.blob();
      if (controller.signal.aborted || this.previews.get(preview.url) !== preview) return;
      preview.objectUrl = URL.createObjectURL(blob);
      preview.retryable = false;
      preview.state = 'ready';
    } catch (error) {
      if (!controller.signal.aborted) {
        preview.state = 'error';
        preview.message =
          error instanceof Error ? `Preview unavailable: ${error.message}` : 'Preview unavailable.';
      }
    } finally {
      if (controller.signal.aborted) preview.state = 'idle';
      preview.controller = undefined;
    }
  }

  private refreshVisible() {
    if (document.visibilityState === 'hidden') return;
    for (const url of this.visible) {
      if (this.previews.get(url)?.state === 'error') this.refresh(url);
    }
  }

  private updateRetryTimer() {
    const needed =
      this.isConnected &&
      document.visibilityState !== 'hidden' &&
      [...this.visible].some((url) => this.previews.get(url)?.retryable);
    if (!needed) {
      window.clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    } else if (this.retryTimer === undefined) {
      this.retryTimer = window.setTimeout(() => {
        this.retryTimer = undefined;
        for (const url of this.visible) {
          if (this.previews.get(url)?.retryable) this.refresh(url);
        }
      }, PREVIEW_RETRY_MS);
    }
  }

  private refresh(url: string) {
    const preview = this.previews.get(url);
    if (!preview) return;
    if (preview.state === 'loading') {
      preview.refresh = true;
      preview.controller?.abort();
      return;
    }
    this.dispose(preview);
    preview.state = 'idle';
    preview.message = undefined;
    preview.retryable = false;
    this.enqueue(url);
    this.requestUpdate();
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
            const url = admin.recoveryPreview(target, versions.revision, file.path, source);
            const preview = this.previews.get(url);
            const other = source
              ? versions.receiving.flatMap((side) => side.files)
              : versions.source.files;
            const same = other.some((candidate) => candidate.sha256 === file.sha256);
            return html`<li class="min-w-0">
              <p class="break-all text-sm font-medium">${file.path}</p>
              ${
                image
                  ? html`<div data-preview=${url}>
                      ${
                        preview?.state === 'ready'
                          ? html`<img class="mt-2 max-h-48 w-full object-contain object-left"
                              alt=${`${version.instance}: ${file.path}`} src=${preview.objectUrl ?? ''}
                              @error=${() => {
                                this.dispose(preview);
                                preview.state = 'error';
                                preview.message = 'Preview unavailable.';
                                this.requestUpdate();
                              }}>`
                          : preview?.state === 'error'
                            ? html`<p class="mt-2 text-sm text-base-content/70">${preview.message}</p>`
                            : html`<p class="mt-2 text-sm text-base-content/70">Loading preview…</p>`
                      }
                    </div>`
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
        <div class="min-w-0 space-y-4">
          ${this.versions.receiving.map((version) => this.side(version, false))}
          ${this.choice(false)}
        </div>
        <div class="min-w-0 space-y-4">
          ${this.side(this.versions.source, true)}
          ${this.choice(true)}
        </div>
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

  private choice(source: boolean) {
    const choices = this.choices;
    if (!choices) return nothing;
    return html`<button type="button" class="btn btn-outline w-full max-sm:min-h-11"
      ?disabled=${choices.disabled} @click=${() => choices.choose(source)}>
      ${source ? choices.sourceLabel : choices.receivingLabel}
    </button>`;
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
