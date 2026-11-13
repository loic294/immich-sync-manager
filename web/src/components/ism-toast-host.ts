import { html, nothing } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import { IsmElement } from '@/base/ism-element';
import '@/components/ism-icon';
import type { IconName } from '@/components/ism-icon';
import {
  dismissToast,
  holdToast,
  releaseToast,
  type Toast,
  type ToastKind,
  toasts,
} from '@/stores/toasts';

/** Tailwind needs complete class strings. */
const ALERT_CLASS: Record<ToastKind, string> = {
  success: 'alert-success',
  info: 'alert-info',
  warning: 'alert-warning',
  error: 'alert-error',
};

const ICON: Record<ToastKind, IconName> = {
  success: 'circle-check',
  info: 'info',
  warning: 'triangle-alert',
  error: 'circle-x',
};

/**
 * Renders the toasts of `@/stores/toasts` (DaisyUI `toast` + `alert`). The shell owns the single
 * instance; call `toast.success(...)` etc. from `@/components/toast` to show one.
 *
 * The container is a polite live region that is always present, so screen readers announce each
 * new toast; errors use `role="alert"` and are announced immediately. A toast stays while the
 * pointer or keyboard focus is on it.
 */
export class IsmToastHost extends IsmElement {
  protected override render() {
    return html`
      <div
        class="toast toast-end toast-bottom z-50"
        role="status"
        aria-live="polite"
        aria-atomic="false"
        aria-relevant="additions"
      >
        ${repeat(
          toasts.value,
          (item) => item.id,
          (item) => this.renderToast(item),
        )}
      </div>
    `;
  }

  private renderToast({ id, kind, message, action }: Toast) {
    return html`
      <div
        class="alert ${ALERT_CLASS[kind]} max-w-md"
        role=${kind === 'error' ? 'alert' : nothing}
        @mouseenter=${() => holdToast(id)}
        @mouseleave=${() => releaseToast(id)}
        @focusin=${() => holdToast(id)}
        @focusout=${() => releaseToast(id)}
      >
        <ism-icon name=${ICON[kind]}></ism-icon>
        <span class="min-w-0 break-words">${message}</span>
        ${
          action
            ? html`<button type="button" class="btn btn-sm" @click=${() => action.run()}>
          ${action.label}
        </button>`
            : nothing
        }
        <button
          type="button"
          class="btn btn-ghost btn-xs btn-circle"
          aria-label="Dismiss notification"
          @click=${() => dismissToast(id)}
        >
          <ism-icon name="x" size="sm"></ism-icon>
        </button>
      </div>
    `;
  }
}

customElements.define('ism-toast-host', IsmToastHost);

declare global {
  interface HTMLElementTagNameMap {
    'ism-toast-host': IsmToastHost;
  }
}
