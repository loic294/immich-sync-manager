import { effect } from '@preact/signals-core';
import { html, nothing } from 'lit';
import { IsmElement } from '@/base/ism-element';
import {
  attentionError,
  dismissAllNotifications,
  dismissNotification,
  notifications,
  refreshAttention,
} from '@/stores/attention';
import '@/components/ism-icon';

export class IsmAttentionBell extends IsmElement {
  private stopSignals: (() => void) | undefined;

  override connectedCallback() {
    super.connectedCallback();
    this.stopSignals = effect(() => {
      notifications.value;
      attentionError.value;
      this.requestUpdate();
    });
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    this.stopSignals?.();
    this.stopSignals = undefined;
  }

  protected override render() {
    const items = notifications.value;
    const count = items.length;
    return html`
      <details class="dropdown dropdown-end">
        <summary
          class="btn btn-ghost btn-sm btn-square list-none"
          aria-label=${count ? `${count} notifications` : 'Notifications'}
          title="Notifications"
        >
          <ism-icon name="bell" size="sm"></ism-icon>
          ${count ? html`<span class="badge badge-error badge-xs absolute -right-1 -top-1">${count > 99 ? '99+' : count}</span>` : nothing}
        </summary>
        <div class="dropdown-content fixed right-4 top-12 z-50 mt-2 max-h-[75vh] w-80 max-w-[calc(100vw-2rem)] overflow-y-auto rounded-box border border-base-300 bg-base-100 shadow-lg sm:absolute sm:right-0 sm:top-auto sm:w-96">
          <div class="flex items-center justify-between gap-3 border-b border-base-300 px-4 py-3">
            <h2 class="font-semibold">Notifications${count ? html` <span class="badge badge-warning badge-sm">${count}</span>` : nothing}</h2>
            ${count ? html`<button type="button" class="btn btn-ghost btn-xs" @click=${dismissAllNotifications}>Dismiss all</button>` : nothing}
          </div>
          ${
            attentionError.value
              ? html`<div role="alert" class="alert alert-error alert-soft m-3 text-sm">
                  <span>Could not refresh notifications.</span>
                  <button type="button" class="btn btn-xs" @click=${() => refreshAttention()}>Retry</button>
                </div>`
              : nothing
          }
          ${
            count === 0 && !attentionError.value
              ? html`<p class="px-4 py-6 text-center text-sm text-base-content/70">You’re all caught up.</p>`
              : items.map(
                  (
                    item,
                  ) => html`<article class="flex gap-3 border-b border-base-300 px-4 py-3 last:border-b-0">
                    <div class="min-w-0 flex-1">
                      <p class="font-medium">${item.title}</p>
                      <p class="mt-1 text-sm text-base-content/70">${item.detail}</p>
                      <a class="link link-primary mt-2 inline-flex items-center gap-1 text-sm font-medium" href=${item.href}>
                        ${item.action}<ism-icon name="arrow-right" size="xs"></ism-icon>
                      </a>
                    </div>
                    <button
                      type="button"
                      class="btn btn-ghost btn-xs btn-square"
                      aria-label=${`Dismiss: ${item.title}`}
                      title="Dismiss"
                      @click=${() => dismissNotification(item.id)}
                    ><ism-icon name="x" size="xs"></ism-icon></button>
                  </article>`,
                )
          }
          <p class="px-4 py-2 text-xs text-base-content/60">Dismissing a notice does not clear a blocker.</p>
        </div>
      </details>
    `;
  }
}

customElements.define('ism-attention-bell', IsmAttentionBell);

declare global {
  interface HTMLElementTagNameMap {
    'ism-attention-bell': IsmAttentionBell;
  }
}
