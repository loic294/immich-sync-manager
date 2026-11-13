/** DaisyUI badge colour for a status word used by operations, transfers, peers and phases. */
export function statusBadge(status: string): string {
  switch (status) {
    case 'completed':
    case 'online':
    case 'active':
    case 'idle':
    case 'ok':
    case 'done':
    case 'resolved':
      return 'badge badge-success badge-soft';
    case 'running':
    case 'preparing':
    case 'reconciling':
    case 'restoring':
    case 'transferring':
    case 'resolving':
      return 'badge badge-info badge-soft';
    case 'queued':
    case 'waiting_maintenance':
    case 'waiting_window':
    case 'pending':
    case 'partial':
    case 'skipped':
      return 'badge badge-warning badge-soft';
    case 'failed':
    case 'blocked':
    case 'recovery_required':
    case 'offline':
    case 'error':
    case 'open':
      return 'badge badge-error badge-soft';
    default:
      return 'badge badge-ghost';
  }
}
