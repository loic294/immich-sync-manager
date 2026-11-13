import { html, nothing } from 'lit';
import { unsafeSVG } from 'lit/directives/unsafe-svg.js';
import {
  Activity,
  ArrowLeftRight,
  ArrowRight,
  Ban,
  CalendarClock,
  Check,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  CircleCheck,
  CircleQuestionMark,
  CircleX,
  Clock,
  Copy,
  Database,
  Download,
  Ellipsis,
  Eye,
  EyeOff,
  File,
  Folder,
  FolderInput,
  FolderOpen,
  FolderPlus,
  FolderSync,
  Gauge,
  GitCompareArrows,
  HardDrive,
  type IconNode,
  Images,
  Info,
  KeyRound,
  LibraryBig,
  LoaderCircle,
  Lock,
  LogOut,
  Menu,
  Monitor,
  Moon,
  Network,
  Pause,
  Pencil,
  Play,
  Plug,
  Plus,
  RefreshCw,
  RotateCcw,
  Save,
  ScrollText,
  Server,
  Settings,
  Share2,
  ShieldCheck,
  Sun,
  Trash2,
  TriangleAlert,
  Undo2,
  Unplug,
  Upload,
  Users,
  Wifi,
  WifiOff,
  X,
} from 'lucide';
import { IsmElement } from '@/base/ism-element';

/**
 * The icon set: kebab-case Lucide names (https://lucide.dev/icons). Import Lucide icons by name
 * only (never `icons` or `createIcons`), so the bundler keeps just the ones listed here. Add a
 * line (alphabetical) when a component needs another icon.
 */
const ICONS = {
  activity: Activity,
  'arrow-left-right': ArrowLeftRight,
  'arrow-right': ArrowRight,
  ban: Ban,
  'calendar-clock': CalendarClock,
  check: Check,
  'chevron-down': ChevronDown,
  'chevron-right': ChevronRight,
  'circle-alert': CircleAlert,
  'circle-check': CircleCheck,
  'circle-question-mark': CircleQuestionMark,
  'circle-x': CircleX,
  clock: Clock,
  copy: Copy,
  database: Database,
  download: Download,
  ellipsis: Ellipsis,
  eye: Eye,
  'eye-off': EyeOff,
  file: File,
  folder: Folder,
  'folder-input': FolderInput,
  'folder-open': FolderOpen,
  'folder-plus': FolderPlus,
  'folder-sync': FolderSync,
  gauge: Gauge,
  'git-compare-arrows': GitCompareArrows,
  'hard-drive': HardDrive,
  images: Images,
  info: Info,
  'key-round': KeyRound,
  'library-big': LibraryBig,
  'loader-circle': LoaderCircle,
  lock: Lock,
  'log-out': LogOut,
  menu: Menu,
  monitor: Monitor,
  moon: Moon,
  network: Network,
  pause: Pause,
  pencil: Pencil,
  play: Play,
  plug: Plug,
  plus: Plus,
  'refresh-cw': RefreshCw,
  'rotate-ccw': RotateCcw,
  save: Save,
  'scroll-text': ScrollText,
  server: Server,
  settings: Settings,
  'share-2': Share2,
  'shield-check': ShieldCheck,
  sun: Sun,
  'trash-2': Trash2,
  'triangle-alert': TriangleAlert,
  'undo-2': Undo2,
  unplug: Unplug,
  upload: Upload,
  users: Users,
  wifi: Wifi,
  'wifi-off': WifiOff,
  x: X,
} satisfies Record<string, IconNode>;

export type IconName = keyof typeof ICONS;

export const ICON_NAMES = Object.keys(ICONS) as IconName[];

export function isIconName(value: unknown): value is IconName {
  return typeof value === 'string' && Object.hasOwn(ICONS, value);
}

/** Tailwind needs these as complete strings. */
const SIZE_CLASS = {
  xs: 'size-3',
  sm: 'size-4',
  md: 'size-5',
  lg: 'size-6',
  xl: 'size-8',
} as const;

export type IconSize = keyof typeof SIZE_CLASS;

const markupCache = new Map<IconName, string>();

function markupOf(name: IconName): string {
  let markup = markupCache.get(name);
  if (markup === undefined) {
    markup = ICONS[name]
      .map(([tag, attrs]) => {
        const attributes = Object.entries(attrs).map(([key, value]) => `${key}="${value}"`);
        return `<${tag} ${attributes.join(' ')}/>`;
      })
      .join('');
    markupCache.set(name, markup);
  }
  return markup;
}

/**
 * An inline SVG icon that takes the text colour of its parent.
 *
 *   <ism-icon name="trash-2"></ism-icon>
 *   <ism-icon name="check" size="sm" label="Saved"></ism-icon>
 *
 * Decorative by default (`aria-hidden`); set `label` when the icon is the only content, then it
 * becomes `role="img"` with that accessible name.
 */
export class IsmIcon extends IsmElement {
  static override properties = {
    name: { type: String },
    size: { type: String },
    label: { type: String },
  };

  declare name: IconName;
  declare size: IconSize;
  declare label: string | undefined;

  constructor() {
    super();
    this.size = 'md';
  }

  override connectedCallback(): void {
    super.connectedCallback();
    this.classList.add('inline-flex', 'shrink-0', 'align-middle');
  }

  protected override render() {
    if (!isIconName(this.name)) return nothing;
    return html`<svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="2"
      stroke-linecap="round"
      stroke-linejoin="round"
      class="block ${SIZE_CLASS[this.size] ?? SIZE_CLASS.md}"
      role=${this.label ? 'img' : nothing}
      aria-label=${this.label || nothing}
      aria-hidden=${this.label ? nothing : 'true'}
    >${unsafeSVG(markupOf(this.name))}</svg>`;
  }
}

customElements.define('ism-icon', IsmIcon);

declare global {
  interface HTMLElementTagNameMap {
    'ism-icon': IsmIcon;
  }
}
