import type { IconName } from '@/components/ism-icon';
import { overview, serversNeedAttention } from '@/stores/overview';

/** Where a route appears in the main menu. */
export type MenuGroup = 'tools' | 'footer';

export interface AppRoute {
  /** Absolute path. The address bar may add a trailing slash. */
  path: string;
  /** The custom element that renders the page. The module `load` imports must define it. */
  tag: string;
  /** Imports the module that defines `tag`. Called on the first visit only, never at startup. */
  load: () => Promise<unknown>;
  /** Menu text, page heading and document title. */
  label: string;
  icon: IconName;
  /** Menu group. Omit for a route without a menu entry. */
  menu?: MenuGroup;
  /** Assigned to the element as properties before it connects. */
  props?: Readonly<Record<string, unknown>>;
  attention?: () => boolean;
}

/** The pages of the app, in menu order. */
export const routes: readonly AppRoute[] = [
  {
    path: '/overview',
    tag: 'ism-overview-page',
    load: () => import('@/pages/ism-overview-page'),
    label: 'Overview',
    icon: 'gauge',
    menu: 'tools',
  },
  {
    path: '/servers',
    tag: 'ism-servers-page',
    load: () => import('@/pages/ism-servers-page'),
    label: 'Servers & accounts',
    icon: 'server',
    menu: 'tools',
    attention: serversNeedAttention,
  },
  {
    path: '/mappings',
    tag: 'ism-mappings-page',
    load: () => import('@/pages/ism-mappings-page'),
    label: 'Folder mappings',
    icon: 'folder-sync',
    menu: 'tools',
    attention: () => (overview.value?.roots_unavailable ?? 0) > 0,
  },
  {
    path: '/operations',
    tag: 'ism-operations-page',
    load: () => import('@/pages/ism-operations-page'),
    label: 'Operations',
    icon: 'activity',
    menu: 'tools',
    attention: () => (overview.value?.operations_attention ?? 0) > 0,
  },
  {
    path: '/sync',
    tag: 'ism-sync-page',
    load: () => import('@/pages/ism-sync-page'),
    label: 'Sync',
    icon: 'arrow-left-right',
    menu: 'tools',
  },
  {
    path: '/conflicts',
    tag: 'ism-conflicts-page',
    load: () => import('@/pages/ism-conflicts-page'),
    label: 'Conflicts',
    icon: 'git-compare-arrows',
    menu: 'tools',
    attention: () => (overview.value?.conflicts_open ?? 0) > 0,
  },
  {
    path: '/peers',
    tag: 'ism-peers-page',
    load: () => import('@/pages/ism-peers-page'),
    label: 'Peers & shares',
    icon: 'network',
    menu: 'tools',
  },
  {
    path: '/trash',
    tag: 'ism-trash-page',
    load: () => import('@/pages/ism-trash-page'),
    label: 'Trash',
    icon: 'trash-2',
    menu: 'footer',
  },
  {
    path: '/tokens',
    tag: 'ism-tokens-page',
    load: () => import('@/pages/ism-tokens-page'),
    label: 'Client tokens',
    icon: 'key-round',
    menu: 'footer',
  },
  {
    path: '/audit',
    tag: 'ism-audit-page',
    load: () => import('@/pages/ism-audit-page'),
    label: 'Audit log',
    icon: 'scroll-text',
    menu: 'footer',
  },
  {
    path: '/settings',
    tag: 'ism-settings-page',
    load: () => import('@/pages/ism-settings-page'),
    label: 'Settings',
    icon: 'settings',
    menu: 'footer',
  },
];

/** Shown for any address that no route matches. */
export const notFound: AppRoute = {
  path: '/*',
  tag: 'ism-route-placeholder',
  load: () => import('@/components/ism-route-placeholder'),
  label: 'Page not found',
  icon: 'circle-question-mark',
  props: {
    heading: 'Page not found',
    message: 'There is nothing at this address.',
    icon: 'circle-question-mark',
    actionHref: '/overview',
    actionLabel: 'Go to Overview',
  },
};

/** Addresses that forward to another route (the address bar is rewritten, not pushed). */
export const redirects: Readonly<Record<string, string>> = { '/': '/overview' };

/** The route for a normalised path, or `undefined` when there is none. */
export function routeAt(path: string, table: readonly AppRoute[] = routes): AppRoute | undefined {
  return table.find((route) => route.path === path);
}
