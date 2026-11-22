import { html, nothing, type ReactiveController, type ReactiveControllerHost } from 'lit';
import { admin } from '@/api/admin';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import '@/components/ism-recovery-versions';

export function targetKey(target: RecoveryTarget) {
  return JSON.stringify([target.kind, target.id, target.peer_id]);
}

export interface ReviewOutcome {
  /** Targets whose comparison is current and unchanged since the previous review. */
  unchanged: RecoveryTarget[];
  /** Targets that were refreshed and now show different versions. */
  changed: RecoveryTarget[];
  failed: RecoveryTarget[];
}

const CONCURRENCY = 4;

/**
 * Per-target version comparisons. Each comparison loads, fails and retries on its own, so a
 * slow or failing target never blocks or discards the others.
 */
export class VersionReview implements ReactiveController {
  readonly versions = new Map<string, RecoveryVersions>();
  readonly errors = new Map<string, string>();
  private readonly loading = new Set<string>();

  constructor(private readonly host: ReactiveControllerHost) {
    host.addController(this);
  }

  hostConnected() {}

  get(target: RecoveryTarget) {
    return this.versions.get(targetKey(target));
  }

  has(target: RecoveryTarget) {
    const id = targetKey(target);
    return this.versions.has(id) && !this.loading.has(id) && !this.errors.has(id);
  }

  isLoading(target: RecoveryTarget) {
    return this.loading.has(targetKey(target));
  }

  get busy() {
    return this.loading.size > 0;
  }

  async compare(targets: readonly RecoveryTarget[]): Promise<ReviewOutcome> {
    const outcome: ReviewOutcome = { unchanged: [], changed: [], failed: [] };
    const queue = targets.filter((target) => !this.loading.has(targetKey(target)));
    for (const target of queue) {
      this.loading.add(targetKey(target));
      this.errors.delete(targetKey(target));
    }
    this.host.requestUpdate();
    const worker = async () => {
      for (let target = queue.shift(); target; target = queue.shift()) {
        const id = targetKey(target);
        const previous = this.versions.get(id)?.revision;
        try {
          const current = await admin.recoveryVersions(target);
          this.versions.set(id, current);
          (previous === undefined || previous === current.revision
            ? outcome.unchanged
            : outcome.changed
          ).push(target);
        } catch (error) {
          this.errors.set(id, error instanceof Error ? error.message : String(error));
          outcome.failed.push(target);
        } finally {
          this.loading.delete(id);
          this.host.requestUpdate();
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, queue.length) }, worker));
    return outcome;
  }

  /** The comparison, its progress, or why it failed with a way to try again. */
  render(target: RecoveryTarget) {
    const id = targetKey(target);
    const versions = this.versions.get(id);
    const error = this.errors.get(id);
    return html`
      ${
        this.loading.has(id)
          ? html`<p role="status" class="mt-2 flex items-center gap-2 text-sm text-base-content/70">
            <span class="loading loading-spinner loading-xs"></span>Comparing versions…</p>`
          : nothing
      }
      ${
        error && !this.loading.has(id)
          ? html`<div role="alert" class="alert alert-error alert-soft mt-2 py-2 text-sm">
            <span class="break-all">Comparison failed: ${error}</span>
            <button type="button" class="btn btn-xs max-sm:min-h-11" @click=${() => this.compare([target])}>Try again</button>
          </div>`
          : nothing
      }
      ${versions ? html`<ism-recovery-versions .target=${target} .versions=${versions}></ism-recovery-versions>` : nothing}
    `;
  }
}
