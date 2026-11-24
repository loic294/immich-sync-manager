import { api } from '@/api/client';
import type { AccountInput } from '@/api/generated/AccountInput';
import type { AccountView } from '@/api/generated/AccountView';
import type { AttentionSnapshot } from '@/api/generated/AttentionSnapshot';
import type { AuditEntry } from '@/api/generated/AuditEntry';
import type { BackgroundActivityPage } from '@/api/generated/BackgroundActivityPage';
import type { ClientTokenCreated } from '@/api/generated/ClientTokenCreated';
import type { ClientTokenInput } from '@/api/generated/ClientTokenInput';
import type { ClientTokenView } from '@/api/generated/ClientTokenView';
import type { ConflictPage } from '@/api/generated/ConflictPage';
import type { ConflictResolution } from '@/api/generated/ConflictResolution';
import type { ImmichServerInput } from '@/api/generated/ImmichServerInput';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import type { LibraryView } from '@/api/generated/LibraryView';
import type { MappingInput } from '@/api/generated/MappingInput';
import type { MappingView } from '@/api/generated/MappingView';
import type { MountView } from '@/api/generated/MountView';
import type { Operation } from '@/api/generated/Operation';
import type { OperationAccepted } from '@/api/generated/OperationAccepted';
import type { OperationAction } from '@/api/generated/OperationAction';
import type { OperationDetail } from '@/api/generated/OperationDetail';
import type { OperationPage } from '@/api/generated/OperationPage';
import type { OverrideInput } from '@/api/generated/OverrideInput';
import type { OverrideView } from '@/api/generated/OverrideView';
import type { Overview } from '@/api/generated/Overview';
import type { PeerAcceptInput } from '@/api/generated/PeerAcceptInput';
import type { PeerInvite } from '@/api/generated/PeerInvite';
import type { PeerView } from '@/api/generated/PeerView';
import type { RecoveryBatchInput } from '@/api/generated/RecoveryBatchInput';
import type { RecoveryBatchResult } from '@/api/generated/RecoveryBatchResult';
import type { RecoveryDecisionInput } from '@/api/generated/RecoveryDecisionInput';
import type { RecoveryPage } from '@/api/generated/RecoveryPage';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import type { ScheduleView } from '@/api/generated/ScheduleView';
import type { SettingsInput } from '@/api/generated/SettingsInput';
import type { SettingsView } from '@/api/generated/SettingsView';
import type { ShareInput } from '@/api/generated/ShareInput';
import type { ShareView } from '@/api/generated/ShareView';
import type { SyncFileDetails } from '@/api/generated/SyncFileDetails';
import type { SyncPage } from '@/api/generated/SyncPage';
import type { SyncStatus } from '@/api/generated/SyncStatus';
import type { TrashEntry } from '@/api/generated/TrashEntry';
import type { WatchSettingsInput } from '@/api/generated/WatchSettingsInput';

const A = '/api/admin';
const enc = encodeURIComponent;

/** Typed wrappers over the administration API. */
export const admin = {
  backgroundActivity: (limit = 25, offset = 0) =>
    api.get<BackgroundActivityPage>(`${A}/operations/background`, { query: { limit, offset } }),
  setup: (body: { username: string; password: string; instance_name: string }) =>
    api.post<void>(`${A}/setup`, { body }),
  login: (body: { username: string; password: string }) => api.post<void>(`${A}/login`, { body }),
  overview: () => api.get<Overview>(`${A}/overview`),
  attention: () => api.get<AttentionSnapshot>(`${A}/attention`),
  settings: () => api.get<SettingsView>(`${A}/settings`),
  saveSettings: (body: SettingsInput) => api.put<SettingsView>(`${A}/settings`, { body }),

  servers: () => api.get<ImmichServerView[]>(`${A}/servers`),
  createServer: (body: ImmichServerInput) => api.post<ImmichServerView>(`${A}/servers`, { body }),
  updateServer: (id: string, body: ImmichServerInput) =>
    api.put<ImmichServerView>(`${A}/servers/${enc(id)}`, { body }),
  deleteServer: (id: string) => api.delete<void>(`${A}/servers/${enc(id)}`),
  checkServer: (id: string) => api.post<ImmichServerView>(`${A}/servers/${enc(id)}/check`),
  refreshLibraries: (id: string) =>
    api.post<{ libraries: number }>(`${A}/servers/${enc(id)}/libraries/refresh`),
  retryMaintenance: (id: string) => api.post<void>(`${A}/servers/${enc(id)}/maintenance/retry`),
  resolveMaintenance: (id: string) => api.post<void>(`${A}/servers/${enc(id)}/maintenance/resolve`),
  /** Turns Immich's file watcher and scheduled scan off again and clears the drift block. */
  reapplyImmichSettings: (id: string) =>
    api.post<void>(`${A}/servers/${enc(id)}/maintenance/reapply`),

  accounts: () => api.get<AccountView[]>(`${A}/accounts`),
  createAccount: (body: AccountInput) => api.post<AccountView>(`${A}/accounts`, { body }),
  deleteAccount: (id: string) => api.delete<void>(`${A}/accounts/${enc(id)}`),
  libraries: () => api.get<LibraryView[]>(`${A}/libraries`),

  mounts: (path?: string) => api.get<MountView[]>(`${A}/mounts`, { query: { path } }),
  mappings: () => api.get<MappingView[]>(`${A}/mappings`),
  createMapping: (body: MappingInput) => api.post<MappingView>(`${A}/mappings`, { body }),
  updateMapping: (rootId: string, body: MappingInput) =>
    api.put<MappingView>(`${A}/mappings/${enc(rootId)}`, { body }),
  deleteMapping: (rootId: string) => api.delete<void>(`${A}/mappings/${enc(rootId)}`),
  setMappingWatch: (rootId: string, body: WatchSettingsInput) =>
    api.put<MappingView>(`${A}/mappings/${enc(rootId)}/watch`, { body }),
  /** Forgets the change-detection snapshot after a pause the administrator checked. */
  resetMappingWatch: (rootId: string) =>
    api.post<MappingView>(`${A}/mappings/${enc(rootId)}/watch/reset`),

  tokens: () => api.get<ClientTokenView[]>(`${A}/tokens`),
  createToken: (body: ClientTokenInput) => api.post<ClientTokenCreated>(`${A}/tokens`, { body }),
  revokeToken: (id: string) => api.delete<void>(`${A}/tokens/${enc(id)}`),

  operations: (status?: string) =>
    api.get<Operation[]>(`${A}/operations`, { query: { status, limit: 200 } }),
  operation: (id: string) => api.get<Operation>(`${A}/operations/${enc(id)}`),
  cancelOperation: (id: string) => api.post<Operation>(`${A}/operations/${enc(id)}/cancel`),
  resolveOperation: (id: string) => api.post<void>(`${A}/operations/${enc(id)}/resolve`),
  recheckOperation: (id: string) => api.post<void>(`${A}/operations/${enc(id)}/recheck`),
  retryOperationPublication: (id: string) =>
    api.post<void>(`${A}/operations/${enc(id)}/retry-publication`),
  operationPage: (status = '', limit = 250, offset = 0) =>
    api.get<OperationPage>(`${A}/operations/page`, { query: { status, limit, offset } }),
  operationDetail: (id: string) => api.get<OperationDetail>(`${A}/operations/${enc(id)}/detail`),
  markOperationHandled: (id: string) => api.post<void>(`${A}/operations/${enc(id)}/handled`),
  recoveryPage: (limit = 250, offset = 0) =>
    api.get<RecoveryPage>(`${A}/recovery/page`, { query: { limit, offset } }),
  fixIssues: (body: RecoveryBatchInput) => api.post<RecoveryBatchResult>(`${A}/recovery`, { body }),
  decideIssues: (body: RecoveryDecisionInput) =>
    api.post<RecoveryBatchResult>(`${A}/recovery/decisions`, { body }),
  recoveryVersions: (target: RecoveryTarget) =>
    api.get<RecoveryVersions>(`${A}/recovery/versions`, {
      query: {
        kind: target.kind,
        id: target.id,
        ...(target.peer_id ? { peer_id: target.peer_id } : {}),
      },
    }),
  recoveryPreview: (target: RecoveryTarget, revision: string, path: string, source: boolean) =>
    `${A}/recovery/preview?${new URLSearchParams({
      kind: target.kind,
      id: target.id,
      revision,
      path,
      source: String(source),
      ...(target.peer_id ? { peer_id: target.peer_id } : {}),
    })}`,
  /** Runs managed actions as the administrator (same endpoint as the client API). */
  submit: (label: string, actions: OperationAction[]) =>
    api.post<OperationAccepted>('/api/v1/operations', {
      body: { label, actions, idempotency_key: crypto.randomUUID() },
    }),

  trash: (rootId?: string) => api.get<TrashEntry[]>(`${A}/trash`, { query: { root_id: rootId } }),

  peers: () => api.get<PeerView[]>(`${A}/peers`),
  invite: () => api.post<PeerInvite>(`${A}/peers/invite`),
  acceptInvite: (body: PeerAcceptInput) => api.post<PeerView>(`${A}/peers/accept`, { body }),
  deletePeer: (id: string) => api.delete<void>(`${A}/peers/${enc(id)}`),
  retryPeer: (id: string) => api.post<void>(`${A}/peers/${enc(id)}/retry`),
  shares: () => api.get<ShareView[]>(`${A}/shares`),
  createShare: (body: ShareInput) => api.post<ShareView>(`${A}/shares`, { body }),
  updateShare: (id: string, body: ShareInput) =>
    api.put<ShareView>(`${A}/shares/${enc(id)}`, { body }),
  deleteShare: (id: string) => api.delete<void>(`${A}/shares/${enc(id)}`),

  sync: () => api.get<SyncStatus>(`${A}/sync`, { query: { summary: true } }),
  syncPage: (status = 'active', limit = 250, offset = 0) =>
    api.get<SyncPage>(`${A}/sync/page`, { query: { status, limit, offset } }),
  syncTransfer: (id: string) => api.get<SyncPage>(`${A}/sync/page`, { query: { id, limit: 1 } }),
  syncFiles: (share: string, path: string, peerId?: string) =>
    api.get<SyncFileDetails>(`${A}/sync/files`, { query: { share, path, peer_id: peerId } }),
  syncThumbnail: (share: string, path: string, peerId?: string) =>
    `${A}/sync/thumbnail?${new URLSearchParams({ share, path, ...(peerId ? { peer_id: peerId } : {}) })}`,
  schedule: () => api.get<ScheduleView>(`${A}/sync/schedule`),
  saveSchedule: (body: ScheduleView) => api.put<ScheduleView>(`${A}/sync/schedule`, { body }),
  overrides: () => api.get<OverrideView[]>(`${A}/sync/overrides`),
  createOverride: (body: OverrideInput) => api.post<OverrideView>(`${A}/sync/overrides`, { body }),
  revokeOverride: (id: string) => api.delete<void>(`${A}/sync/overrides/${enc(id)}`),
  retryTransfer: (id: string) => api.post<void>(`${A}/sync/transfers/${enc(id)}/retry`),
  cancelTransfer: (id: string) => api.post<void>(`${A}/sync/transfers/${enc(id)}/cancel`),
  readdSyncOperation: (id: string, index: number) =>
    api.post<void>(`${A}/sync/operations/${enc(id)}/${index}/readd`),

  conflicts: (resolvedOnly = false, limit = 50, offset = 0, share = '', folder = '') =>
    api.get<ConflictPage>(`${A}/conflicts/page`, {
      query: { resolved_only: resolvedOnly, limit, offset, share, folder },
    }),
  resolveConflict: (id: string, resolution: ConflictResolution) =>
    api.post<void>(`${A}/conflicts/${enc(id)}/resolve`, { body: { resolution } }),

  audit: (before?: number) =>
    api.get<AuditEntry[]>(`${A}/audit`, { query: { before, limit: 200 } }),
};
