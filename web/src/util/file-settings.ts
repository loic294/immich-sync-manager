/**
 * The limits the server enforces on the "Files and sidecars" settings (`ism_core::file_settings`,
 * the same rules as Open Photo Manager). They are mirrored here only to flag a bad entry before
 * Save: the server is authoritative and its error is shown whenever the two disagree.
 */

export const NAME_MAX_LEN = { min: 16, max: 255 } as const;
/** Encrypted shares (eCryptfs) only allow names of this many bytes. */
export const ENCRYPTED_SHARE_NAME_LEN = 143;
export const MAX_SIDECAR_EXTENSIONS = 50;
const MAX_SIDECAR_EXTENSION_LEN = 10;
export const MAX_JUNK_ENTRIES = 200;
const MAX_JUNK_ENTRY_BYTES = 255;

const EXTENSION_PATTERN = new RegExp(`^[a-z0-9]{1,${MAX_SIDECAR_EXTENSION_LEN}}$`);
const utf8 = new TextEncoder();

/** Trim, drop one leading dot, lowercase ASCII letters. */
export function normalizeExtension(raw: string): string {
  const trimmed = raw.trim();
  const bare = trimmed.startsWith('.') ? trimmed.slice(1) : trimmed;
  return bare.replace(/[A-Z]/g, (letter) => letter.toLowerCase());
}

/** Why a normalised extension is refused (phrased to follow it), else `null`. */
export function validateExtension(extension: string): string | null {
  return EXTENSION_PATTERN.test(extension)
    ? null
    : `must be 1 to ${MAX_SIDECAR_EXTENSION_LEN} letters or digits, without a dot`;
}

/** Trim and compose (NFC). */
export function normalizeJunkEntry(raw: string): string {
  return raw.trim().normalize('NFC');
}

function validateJunkEntry(entry: string, rejectDotNames: boolean): string | null {
  if (entry.includes('/') || entry.includes('\0')) return 'must not contain / or NUL';
  if (rejectDotNames && (entry === '.' || entry === '..')) return 'must not be . or ..';
  if (utf8.encode(entry).length > MAX_JUNK_ENTRY_BYTES) {
    return `is too long (at most ${MAX_JUNK_ENTRY_BYTES} bytes)`;
  }
  return null;
}

export function validateJunkName(entry: string): string | null {
  return validateJunkEntry(entry, true);
}

/** Like a name, but `.` is fine: it matches every hidden file. */
export function validateJunkPrefix(entry: string): string | null {
  return validateJunkEntry(entry, false);
}

export function isValidNameMaxLen(value: number): boolean {
  return Number.isInteger(value) && value >= NAME_MAX_LEN.min && value <= NAME_MAX_LEN.max;
}
