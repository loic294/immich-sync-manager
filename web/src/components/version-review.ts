import { html, nothing, type ReactiveController, type ReactiveControllerHost } from 'lit';
import { admin } from '@/api/admin';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import type { VersionChoices } from '@/components/ism-recovery-versions';
import '@/components/ism-recovery-versions';

export function targetKey(target: RecoveryTarget) {
  return JSON.stringify([target.kind, target.id, target.peer_id]);
}

const CONCURRENCY = 2;
const BATCH_SIZE = 50;

/**
 * Bounded batch reads with per-target readiness, errors and revision invalidation.
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
  private pumpScheduled = false;

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
    this.schedulePump();
    this.host.requestUpdate();
    await Promise.all(pending);
  }

  private schedulePump() {
    if (this.pumpScheduled) return;
    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      this.pump();
    });
  }

  private pump() {
    while (this.active < CONCURRENCY && this.queue.length) {
      const jobs = this.queue.splice(0, BATCH_SIZE);
      this.active++;
      void this.fetch(jobs.map((job) => job.target)).finally(() => {
        this.active--;
        for (const job of jobs) job.resolve();
        this.schedulePump();
      });
    }
  }

  private async fetch(targets: RecoveryTarget[]) {
    try {
      const current = await admin.recoveryVersionsBatch(targets);
      const items = new Map(current.items.map((item) => [targetKey(item.target), item]));
      if (
        current.items.length !== targets.length ||
        items.size !== targets.length ||
        targets.some((target) => !items.has(targetKey(target)))
      )
        throw new Error('The evidence batch did not match the requested files. Try again.');
      for (const target of targets) {
        const id = targetKey(target);
        const item = items.get(id);
        if (!item || this.invalidated.has(id)) continue;
        if (item.state === 'ready') {
          this.versions.set(id, item.versions);
          this.stale.delete(id);
        } else {
          this.errors.set(id, item.message);
        }
      }
    } catch (error) {
      for (const target of targets) {
        const id = targetKey(target);
        if (!this.invalidated.has(id))
          this.errors.set(id, error instanceof Error ? error.message : String(error));
      }
    } finally {
      const retry: RecoveryTarget[] = [];
      for (const target of targets) {
        const id = targetKey(target);
        this.loading.delete(id);
        this.invalidated.delete(id);
        if (this.retryWhenSettled.delete(id) && !this.has(target)) retry.push(target);
      }
      if (retry.length) void this.compare(retry);
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
