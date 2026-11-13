import { html } from 'lit';
import { live } from 'lit/directives/live.js';
import { nextTheme, THEME_LABELS } from '@/app/theme';
import { IsmElement } from '@/base/ism-element';
import '@/components/ism-icon';
import { theme } from '@/stores/prefs';

/**
 * Cycles the `theme` preference system -> light -> dark -> system. It is one DaisyUI `swap`
 * whose checkbox has three states: indeterminate = system (monitor), unchecked = light (sun),
 * checked = dark (moon). The choice persists through the preference; `startTheme()` (started by
 * the shell) applies it to the page.
 *
 *   <ism-theme-toggle></ism-theme-toggle>
 */
export class IsmThemeToggle extends IsmElement {
  private onChange() {
    theme.value = nextTheme(theme.peek());
  }

  protected override render() {
    const pref = theme.value;
    const state = `Theme: ${THEME_LABELS[pref]}`;
    return html`
      <div class="tooltip tooltip-left" data-tip=${state}>
        <label
          class="btn btn-ghost btn-sm btn-square swap swap-rotate has-focus-visible:outline-2 has-focus-visible:outline-offset-2 has-focus-visible:outline-primary"
        >
          <input
            type="checkbox"
            aria-label="${state}. Switch to ${THEME_LABELS[nextTheme(pref)]}."
            .checked=${live(pref === 'dark')}
            .indeterminate=${live(pref === 'system')}
            @change=${this.onChange}
          />
          <ism-icon class="swap-on" name="moon"></ism-icon>
          <ism-icon class="swap-off" name="sun"></ism-icon>
          <ism-icon class="swap-indeterminate" name="monitor"></ism-icon>
        </label>
      </div>
    `;
  }
}

customElements.define('ism-theme-toggle', IsmThemeToggle);

declare global {
  interface HTMLElementTagNameMap {
    'ism-theme-toggle': IsmThemeToggle;
  }
}
