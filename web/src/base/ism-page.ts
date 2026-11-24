import { html, nothing, type TemplateResult } from 'lit';
import { createAdmin } from '@/api/admin';
import { isApiError } from '@/api/errors';
import { LiveScope } from '@/api/live';
import { IsmElement } from '@/base/ism-element';
import { toast } from '@/components/toast';
import { refreshSession } from '@/stores/session';
import '@/components/ism-icon';

/**
 * A page that shows server data: `data` owns its live snapshot subscriptions.
 * `load()` renders pushed values; `act()` runs an action with a busy flag and toasts.
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

  private readonly live = new LiveScope(() => {
    if (this.isConnected) void this.reload(true);
  });
  protected readonly data = createAdmin(this.live);
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
    void this.reload();
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    this.live.dispose();
  }

  reload(pushed = false): Promise<void> {
    if (!pushed) this.live.dispose();
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
  protected frame(
    content: () => TemplateResult,
    actions?: TemplateResult,
    split = false,
  ): TemplateResult {
    return html`
      <div class=${split ? 'h-full min-h-0 overflow-y-auto lg:overflow-hidden' : 'h-full overflow-y-auto'}>
        <div class=${split ? 'mx-auto flex max-w-6xl flex-col gap-4 p-4 lg:h-full lg:min-h-0' : 'mx-auto flex max-w-6xl flex-col gap-4 p-4'}>
          ${actions ? html`<div class="flex flex-none flex-wrap items-center justify-end gap-2">${actions}</div>` : nothing}
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
