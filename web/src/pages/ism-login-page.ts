import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import { errorMessage } from '@/api/errors';
import { IsmElement } from '@/base/ism-element';
import { refreshSession } from '@/stores/session';
import '@/components/ism-icon';

/**
 * First-run setup (create the administrator and name this instance) or sign-in, shown by the shell
 * instead of the app while nobody is signed in.
 */
export class IsmLoginPage extends IsmElement {
  static override properties = {
    setup: { type: Boolean },
    submitting: { state: true },
    error: { state: true },
  };

  declare setup: boolean;
  declare submitting: boolean;
  declare error: string | undefined;

  constructor() {
    super();
    this.setup = false;
    this.submitting = false;
    this.error = undefined;
  }

  private async onSubmit(event: SubmitEvent) {
    event.preventDefault();
    const form = new FormData(event.target as HTMLFormElement);
    const username = String(form.get('username') ?? '').trim();
    const password = String(form.get('password') ?? '');
    this.error = undefined;
    if (this.setup) {
      if (password.length < 10) {
        this.error = 'Use a password of at least 10 characters.';
        return;
      }
      if (password !== String(form.get('confirm') ?? '')) {
        this.error = 'The passwords do not match.';
        return;
      }
    }
    this.submitting = true;
    try {
      if (this.setup) {
        const instance_name = String(form.get('instance_name') ?? '').trim();
        await admin.setup({ username, password, instance_name });
      } else {
        await admin.login({ username, password });
      }
      await refreshSession();
    } catch (error) {
      this.error = errorMessage(error);
    } finally {
      this.submitting = false;
    }
  }

  protected override render() {
    const setup = this.setup;
    return html`
      <div class="flex min-h-screen items-center justify-center bg-base-200 p-4">
        <form class="card w-full max-w-sm bg-base-100 shadow-sm" @submit=${this.onSubmit}>
          <div class="card-body gap-3">
            <div class="flex items-center gap-2">
              <ism-icon name="folder-sync" size="lg" class="text-primary"></ism-icon>
              <h1 class="card-title">Immich Sync Manager</h1>
            </div>
            <p class="text-sm text-base-content/70">
              ${setup ? 'Create the administrator account for this instance.' : 'Sign in to continue.'}
            </p>
            ${
              setup
                ? html`<label class="floating-label">
                    <span>Instance name</span>
                    <input name="instance_name" class="input w-full" required maxlength="64" placeholder="Instance name" autocomplete="off" />
                  </label>`
                : nothing
            }
            <label class="floating-label">
              <span>Username</span>
              <input name="username" class="input w-full" required autocomplete="username" placeholder="Username" />
            </label>
            <label class="floating-label">
              <span>Password</span>
              <input
                name="password"
                type="password"
                class="input w-full"
                required
                autocomplete=${setup ? 'new-password' : 'current-password'}
                placeholder="Password"
              />
            </label>
            ${
              setup
                ? html`<label class="floating-label">
                    <span>Confirm password</span>
                    <input name="confirm" type="password" class="input w-full" required autocomplete="new-password" placeholder="Confirm password" />
                  </label>`
                : nothing
            }
            ${this.error ? html`<div role="alert" class="alert alert-error alert-soft text-sm">${this.error}</div>` : nothing}
            <button class="btn btn-primary" ?disabled=${this.submitting}>
              ${this.submitting ? html`<span class="loading loading-spinner loading-xs"></span>` : nothing}
              ${setup ? 'Create administrator' : 'Sign in'}
            </button>
          </div>
        </form>
      </div>
    `;
  }
}

customElements.define('ism-login-page', IsmLoginPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-login-page': IsmLoginPage;
  }
}
