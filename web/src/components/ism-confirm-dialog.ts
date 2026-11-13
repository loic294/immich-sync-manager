import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { IsmElement } from '@/base/ism-element';

export interface ConfirmOptions {
  title: string;
  message: string;
  confirmLabel?: string;
  /** Styles the confirm button as destructive. */
  danger?: boolean;
}

/**
 * One modal confirmation. Use `confirmAction()` from `@/components/confirm`, which mounts this
 * element on demand and resolves with the user's choice.
 */
export class IsmConfirmDialog extends IsmElement {
  static override properties = { options: { attribute: false } };

  declare options: ConfirmOptions | undefined;
  private dialog = createRef<HTMLDialogElement>();
  private resolve: ((value: boolean) => void) | undefined;

  constructor() {
    super();
    this.options = undefined;
  }

  async ask(options: ConfirmOptions): Promise<boolean> {
    this.resolve?.(false);
    this.options = options;
    await this.updateComplete;
    this.dialog.value?.showModal();
    return new Promise((resolve) => {
      this.resolve = resolve;
    });
  }

  private finish(value: boolean) {
    const resolve = this.resolve;
    this.resolve = undefined;
    if (this.dialog.value?.open) this.dialog.value.close();
    resolve?.(value);
  }

  protected override render() {
    const options = this.options;
    return html`
      <dialog class="modal" ${ref(this.dialog)} @close=${() => this.finish(false)}>
        <div class="modal-box">
          ${
            options
              ? html`
                <h3 class="text-lg font-semibold">${options.title}</h3>
                <p class="py-4 whitespace-pre-line">${options.message}</p>
                <div class="modal-action">
                  <button type="button" class="btn btn-ghost" @click=${() => this.finish(false)}>Cancel</button>
                  <button
                    type="button"
                    class=${options.danger ? 'btn btn-error' : 'btn btn-primary'}
                    @click=${() => this.finish(true)}
                  >
                    ${options.confirmLabel ?? 'Confirm'}
                  </button>
                </div>
              `
              : nothing
          }
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }
}

customElements.define('ism-confirm-dialog', IsmConfirmDialog);

declare global {
  interface HTMLElementTagNameMap {
    'ism-confirm-dialog': IsmConfirmDialog;
  }
}
