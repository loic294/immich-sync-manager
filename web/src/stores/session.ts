import { signal } from '@preact/signals-core';
import { api } from '@/api/client';
import type { SessionInfo } from '@/api/generated/SessionInfo';

/** `undefined` while the first request is in flight. */
export const session = signal<SessionInfo | undefined>(undefined);
export const sessionError = signal<string | undefined>(undefined);

export async function refreshSession(): Promise<SessionInfo | undefined> {
  try {
    const info = await api.get<SessionInfo>('/api/admin/session');
    session.value = info;
    sessionError.value = undefined;
    return info;
  } catch (error) {
    sessionError.value = error instanceof Error ? error.message : String(error);
    return undefined;
  }
}

export async function logout(): Promise<void> {
  await api.post('/api/admin/logout');
  await refreshSession();
}
