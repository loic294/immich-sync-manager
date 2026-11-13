import { oneOf, persistedSignal } from '@/stores/persisted';

/** User preferences. Every signal persists to localStorage and syncs across tabs. */

export const THEMES = ['system', 'light', 'dark'] as const;
export type ThemePref = (typeof THEMES)[number];

/** `system` follows the operating system. Applied to `<html>` by `@/app/theme`. */
export const theme = persistedSignal<ThemePref>('theme', 'system', oneOf(THEMES));
