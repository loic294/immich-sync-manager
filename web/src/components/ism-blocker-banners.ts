import { html, nothing } from 'lit';
import { IsmElement } from '@/base/ism-element';
import { attentionError, blockers, refreshAttention } from '@/stores/attention';
import '@/components/ism-icon';

export class IsmBlockerBanners extends IsmElement {
  protected override render() {
    const groups = new Map<string, typeof blockers.value>();
    for (const entry of blockers.value) {
      const key = entry.id.split(':')[0];
      groups.set(key, [...(groups.get(key) ?? []), entry]);
    }
    return html`
      ${
        attentionError.value
          ? html`<section role="alert" class="alert alert-warning alert-soft flex-wrap">
        <span>Some status information is unavailable. Check again to see what may be holding work.</span>
        <button type="button" class="btn btn-sm" @click=${() => refreshAttention()}>Retry</button>
      </section>`
          : nothing
      }
      ${[...groups.values()].map((entries) => {
        const first = entries[0];
        return html`<section role="alert" class="alert alert-warning alert-soft flex-wrap gap-3">
          <ism-icon name="triangle-alert"></ism-icon>
          <div class="min-w-0 flex-1">
            <p class="font-semibold">${first.title}</p>
            <p class="text-sm">${first.detail}${entries.length > 1 ? ` ${entries.length - 1} more ${entries.length === 2 ? 'item needs' : 'items need'} attention.` : ''}</p>
          </div>
          <a class="btn btn-sm" href=${first.href}>${first.action}<ism-icon name="arrow-right" size="xs"></ism-icon></a>
        </section>`;
      })}
    `;
  }
}

customElements.define('ism-blocker-banners', IsmBlockerBanners);

declare global {
  interface HTMLElementTagNameMap {
    'ism-blocker-banners': IsmBlockerBanners;
  }
}
