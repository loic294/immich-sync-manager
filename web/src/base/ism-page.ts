import { html, nothing, type TemplateResult } from 'lit';
import { isApiError } from '@/api/errors';
import { type ChangeEvent, type ChangeType, events } from '@/api/events';
import { IsmElement } from '@/base/ism-element';
import { toast } from '@/components/toast';
import { refreshSession } from '@/stores/session';
import '@/components/ism-icon';

/**
 * A page that shows server data: `load()` fetches it on connect, again on every change event named
 * in `reloadOn` (and on `resync`), and `act()` runs an action with a busy flag and toasts.
 * A 401 re-reads the session so the shell falls back to the login page.
 */
export abstract class IsmPage extends IsmElement {
  static override properties = {
    loading: { state: true },
    loadError: { state: true },
    busy: { state: true },
  };

  declare loading: boolean;
  declare loadError: string | undefined;
  /** The key of the action in flight, or `undefined`. */
  declare busy: string | undefined;

  /** Change events that make this page reload. `resync` always does. */
  protected reloadOn: readonly ChangeType[] = [];
  private stopEvents: (() => void) | undefined;
  private reloadTimer: number | undefined;
  private reloading: Promise<void> | undefined;
  private reloadAgain = false;

  constructor() {
    super();
    this.loading = true;
    this.loadError = undefined;
    this.busy = undefined;
  }

  protected abstract load(): Promise<void>;

  override connectedCallback() {
    super.connectedCallback();
    this.stopEvents = events.on((event) => this.onChange(event));
    void this.reload();
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    this.stopEvents?.();
    window.clearTimeout(this.reloadTimer);
    this.reloadTimer = undefined;
  }

  protected onChange(event: ChangeEvent) {
    if (event.type !== 'resync' && !this.reloadOn.includes(event.type)) return;
    // Coalesce bursts (one event per committed item) into one request.
    if (this.reloadTimer !== undefined) return;
    this.reloadTimer = window.setTimeout(() => {
      this.reloadTimer = undefined;
      void this.reload();
    }, 250);
  }

  reload(): Promise<void> {
    if (this.reloading) {
      this.reloadAgain = true;
      return this.reloading;
    }
    this.reloading = this.reloadLoop().finally(() => {
      this.reloading = undefined;
    });
    return this.reloading;
  }

  private async reloadLoop(): Promise<void> {
    do {
      this.reloadAgain = false;
      await this.loadOnce();
    } while (this.reloadAgain && this.isConnected);
  }

  private async loadOnce(): Promise<void> {
    try {
      await this.load();
      this.loadError = undefined;
    } catch (error) {
      if (isApiError(error) && error.status === 401) void refreshSession();
      this.loadError = error instanceof Error ? error.message : String(error);
    } finally {
      this.loading = false;
    }
  }

  /** Runs `action` once at a time per key; toasts `success` or the error. Returns success. */
  protected async act(
    key: string,
    action: () => Promise<unknown>,
    success?: string,
  ): Promise<boolean> {
    if (this.busy) return false;
    this.busy = key;
    try {
      await action();
      if (success) toast.success(success);
      await this.reload();
      return true;
    } catch (error) {
      if (isApiError(error) && error.status === 401) void refreshSession();
      toast.error(error instanceof Error ? error.message : String(error));
      return false;
    } finally {
      this.busy = undefined;
    }
  }

  /** A spinner while the first load runs, the load error, else `content()`. */
  protected frame(content: () => TemplateResult, actions?: TemplateResult): TemplateResult {
    return html`
      <div class="h-full overflow-y-auto">
        <div class="mx-auto flex max-w-6xl flex-col gap-4 p-4">
          ${actions ? html`<div class="flex flex-wrap items-center justify-end gap-2">${actions}</div>` : nothing}
          ${
            this.loadError
              ? html`<div role="alert" class="alert alert-error alert-soft">
                  <ism-icon name="circle-alert"></ism-icon>
                  <span>${this.loadError}</span>
                  <button type="button" class="btn btn-sm" @click=${() => this.reload()}>Retry</button>
                </div>`
              : nothing
          }
          ${
            this.loading
              ? html`<div class="flex justify-center p-8"><span class="loading loading-spinner loading-lg"></span></div>`
              : content()
          }
        </div>
      </div>
    `;
  }

  /** A busy-aware button label: a spinner while `key` runs. */
  protected spin(key: string) {
    return this.busy === key
      ? html`<span class="loading loading-spinner loading-xs"></span>`
      : nothing;
  }
}
