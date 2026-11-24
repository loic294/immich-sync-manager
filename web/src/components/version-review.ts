import { html, nothing, type ReactiveController, type ReactiveControllerHost } from 'lit';
import { admin } from '@/api/admin';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import type { VersionChoices } from '@/components/ism-recovery-versions';
import '@/components/ism-recovery-versions';

export function targetKey(target: RecoveryTarget) {
  return JSON.stringify([target.kind, target.id, target.peer_id]);
}

const CONCURRENCY = 4;

/**
 * Per-target version comparisons. Each comparison loads, fails and retries on its own, so a
 * slow or failing target never blocks or discards the others.
 */
export class VersionReview implements ReactiveController {
  readonly versions = new Map<string, RecoveryVersions>();
  readonly errors = new Map<string, string>();
  private readonly stale = new Set<string>();
  private readonly loading = new Set<string>();
  private readonly invalidated = new Set<string>();
  private readonly retryWhenSettled = new Set<string>();
  private readonly queue: { target: RecoveryTarget; resolve: () => void }[] = [];
  private active = 0;

  constructor(private readonly host: ReactiveControllerHost) {
    host.addController(this);
  }

  hostConnected() {}

  get(target: RecoveryTarget) {
    return this.versions.get(targetKey(target));
  }

  has(target: RecoveryTarget) {
    const id = targetKey(target);
    return (
      this.versions.has(id) && !this.stale.has(id) && !this.loading.has(id) && !this.errors.has(id)
    );
  }

  forget(target: RecoveryTarget, retry = true) {
    const id = targetKey(target);
    this.stale.add(id);
    this.errors.delete(id);
    if (this.loading.has(id)) {
      this.invalidated.add(id);
      if (retry) this.retryWhenSettled.add(id);
      else this.retryWhenSettled.delete(id);
    }
  }

  retain(targets: readonly RecoveryTarget[]) {
    const retained = new Set(targets.map(targetKey));
    for (let i = this.queue.length - 1; i >= 0; i--) {
      const job = this.queue[i];
      const id = targetKey(job.target);
      if (retained.has(id)) continue;
      this.queue.splice(i, 1);
      this.loading.delete(id);
      this.invalidated.delete(id);
      this.retryWhenSettled.delete(id);
      job.resolve();
    }
  }

  isLoading(target: RecoveryTarget) {
    return this.loading.has(targetKey(target));
  }

  retryUnavailable(targets: readonly RecoveryTarget[]) {
    const failed: RecoveryTarget[] = [];
    for (const target of targets) {
      const id = targetKey(target);
      if (this.loading.has(id)) this.retryWhenSettled.add(id);
      else if (this.errors.has(id)) failed.push(target);
    }
    if (failed.length) void this.compare(failed);
  }

  get busy() {
    return this.loading.size > 0;
  }

  async compare(targets: readonly RecoveryTarget[]): Promise<void> {
    const pending: Promise<void>[] = [];
    for (const target of targets) {
      const id = targetKey(target);
      if (this.loading.has(id)) continue;
      this.loading.add(id);
      this.errors.delete(id);
      pending.push(new Promise((resolve) => this.queue.push({ target, resolve })));
    }
    this.pump();
    this.host.requestUpdate();
    await Promise.all(pending);
  }

  private pump() {
    while (this.active < CONCURRENCY && this.queue.length) {
      const job = this.queue.shift();
      if (!job) break;
      this.active++;
      void this.fetch(job.target).finally(() => {
        this.active--;
        job.resolve();
        this.pump();
      });
    }
  }

  private async fetch(target: RecoveryTarget) {
    const id = targetKey(target);
    try {
      const current = await admin.recoveryVersions(target);
      if (!this.invalidated.has(id)) {
        this.versions.set(id, current);
        this.stale.delete(id);
      }
    } catch (error) {
      if (!this.invalidated.has(id))
        this.errors.set(id, error instanceof Error ? error.message : String(error));
    } finally {
      this.loading.delete(id);
      this.invalidated.delete(id);
      if (this.retryWhenSettled.delete(id) && !this.has(target)) {
        void this.compare([target]);
      }
      this.host.requestUpdate();
    }
  }

  /** The comparison, its progress, or why it failed with a way to try again. */
  render(target: RecoveryTarget, choices?: VersionChoices) {
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
      ${versions ? html`<ism-recovery-versions .target=${target} .versions=${versions} .choices=${choices}></ism-recovery-versions>` : nothing}
    `;
  }
}
