//! "Files and sidecars" settings shared by ISM instances and OPM.
//!
//! # Contract
//! - [`FileSettings::normalized`] validates admin input with OPM's rules: extensions are 1-10
//!   lowercase letters or digits (a leading dot is dropped), ignored names and prefixes are single
//!   name components of at most 255 bytes, `name_max_len` is 16-255. Lists are deduplicated
//!   keeping the first spelling and the order.
//! - [`FileSettings::lenient`] cleans values received from peers or read from storage: invalid
//!   entries are dropped and limits enforced, so a bad peer can never break this instance.
//! - [`JunkRules`] matches whole names against the ignored names and name starts against the
//!   ignored prefixes, ignoring case and Unicode normalisation.
//! - [`SharedSettings`] replicates between peers. The choice of primary instance is last writer
//!   wins on `(designated_at, designated_by)`. Only the primary's values spread: an instance
//!   adopts the files of a state whose origin is the primary when it is newer than its own copy.
//!   With no primary, every instance keeps its own values.

use std::collections::HashSet;
use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::dto::FileSettings;

/// Default sidecar extensions: the metadata sidecar Immich links (`xmp`) and the files editors
/// and exporters keep next to an original.
pub const DEFAULT_SIDECAR_EXTENSIONS: &[&str] = &[
    "xmp", "json", "aae", "dop", "pp3", "xml", "on1", "arp", "cos",
];
/// Default ignored names: NAS, recycle bin and desktop metadata.
pub const DEFAULT_JUNK_NAMES: &[&str] = &[
    "@eaDir",
    "#recycle",
    "#snapshot",
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
    "$RECYCLE.BIN",
    "System Volume Information",
];
/// Default ignored prefixes (AppleDouble resource forks).
pub const DEFAULT_JUNK_PREFIXES: &[&str] = &["._"];

pub const MAX_SIDECAR_EXTENSIONS: usize = 50;
pub const MAX_SIDECAR_EXTENSION_LEN: usize = 10;
pub const MAX_JUNK_ENTRIES: usize = 200;
pub const MAX_JUNK_ENTRY_BYTES: usize = 255;
pub const NAME_MAX_LEN_RANGE: RangeInclusive<u16> = 16..=255;

/// One rejected field, named like OPM does (`sidecar_extensions[2]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

impl FieldError {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for FieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

/// Case- and normalisation-insensitive comparison key of a name.
pub fn normalize_name(name: &str) -> String {
    name.nfc()
        .collect::<String>()
        .to_lowercase()
        .nfc()
        .collect()
}

fn owned(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

impl Default for FileSettings {
    fn default() -> Self {
        Self {
            sidecar_extensions: owned(DEFAULT_SIDECAR_EXTENSIONS),
            junk_names: owned(DEFAULT_JUNK_NAMES),
            junk_prefixes: owned(DEFAULT_JUNK_PREFIXES),
            name_max_len: *NAME_MAX_LEN_RANGE.end(),
        }
    }
}

enum Entry {
    Blank,
    Valid(String),
    Invalid(&'static str),
}

fn extension(raw: &str) -> Entry {
    let trimmed = raw.trim();
    let ext = trimmed
        .strip_prefix('.')
        .unwrap_or(trimmed)
        .to_ascii_lowercase();
    if ext.is_empty() {
        Entry::Blank
    } else if ext.len() <= MAX_SIDECAR_EXTENSION_LEN
        && ext
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        Entry::Valid(ext)
    } else {
        Entry::Invalid("must be 1 to 10 lowercase letters or digits, without a dot")
    }
}

fn junk_entry(raw: &str, is_name: bool) -> Entry {
    let entry: String = raw.trim().nfc().collect();
    if entry.is_empty() {
        Entry::Blank
    } else if entry.contains(['/', '\0']) {
        Entry::Invalid("must not contain `/` or NUL")
    } else if is_name && (entry == "." || entry == "..") {
        Entry::Invalid("must not be `.` or `..`")
    } else if entry.len() > MAX_JUNK_ENTRY_BYTES {
        Entry::Invalid("is too long (at most 255 bytes)")
    } else {
        Entry::Valid(entry)
    }
}

/// Cleans one list. Strict mode reports invalid entries and overlong lists; lenient mode drops
/// them and truncates.
fn clean_list(
    field: &str,
    input: &[String],
    max: usize,
    parse: impl Fn(&str) -> Entry,
    key: impl Fn(&str) -> String,
    errors: Option<&mut Vec<FieldError>>,
) -> Vec<String> {
    let mut problems = Vec::new();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (index, raw) in input.iter().enumerate() {
        match parse(raw) {
            Entry::Blank => {}
            Entry::Invalid(message) => {
                problems.push(FieldError::new(format!("{field}[{index}]"), message));
            }
            Entry::Valid(entry) => {
                if seen.insert(key(&entry)) {
                    out.push(entry);
                }
            }
        }
    }
    if out.len() > max {
        problems.push(FieldError::new(
            field,
            format!("at most {max} entries are allowed"),
        ));
        out.truncate(max);
    }
    if let Some(errors) = errors {
        errors.extend(problems);
    }
    out
}

impl FileSettings {
    fn clean(&self, mut errors: Option<&mut Vec<FieldError>>) -> Self {
        let sidecar_extensions = clean_list(
            "sidecar_extensions",
            &self.sidecar_extensions,
            MAX_SIDECAR_EXTENSIONS,
            extension,
            str::to_owned,
            errors.as_deref_mut(),
        );
        let junk_names = clean_list(
            "junk_names",
            &self.junk_names,
            MAX_JUNK_ENTRIES,
            |raw| junk_entry(raw, true),
            normalize_name,
            errors.as_deref_mut(),
        );
        let junk_prefixes = clean_list(
            "junk_prefixes",
            &self.junk_prefixes,
            MAX_JUNK_ENTRIES,
            |raw| junk_entry(raw, false),
            normalize_name,
            errors.as_deref_mut(),
        );
        if !NAME_MAX_LEN_RANGE.contains(&self.name_max_len)
            && let Some(errors) = errors
        {
            errors.push(FieldError::new(
                "name_max_len",
                format!(
                    "must be between {} and {}",
                    NAME_MAX_LEN_RANGE.start(),
                    NAME_MAX_LEN_RANGE.end()
                ),
            ));
        }
        Self {
            sidecar_extensions,
            junk_names,
            junk_prefixes,
            name_max_len: self
                .name_max_len
                .clamp(*NAME_MAX_LEN_RANGE.start(), *NAME_MAX_LEN_RANGE.end()),
        }
    }

    /// Validates admin input and returns it normalised.
    pub fn normalized(&self) -> Result<Self, Vec<FieldError>> {
        let mut errors = Vec::new();
        let out = self.clean(Some(&mut errors));
        if errors.is_empty() {
            Ok(out)
        } else {
            Err(errors)
        }
    }

    /// Normalises stored or received values, dropping what is invalid.
    pub fn lenient(&self) -> Self {
        self.clean(None)
    }

    pub fn junk_rules(&self) -> JunkRules {
        JunkRules::new(&self.junk_names, &self.junk_prefixes)
    }
}

/// Matches names against the ignored names and prefixes. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JunkRules {
    names: Vec<String>,
    prefixes: Vec<String>,
}

impl Default for JunkRules {
    fn default() -> Self {
        FileSettings::default().junk_rules()
    }
}

fn keys(entries: &[String], is_name: bool) -> Vec<String> {
    let mut keys: Vec<String> = entries
        .iter()
        .filter_map(|raw| match junk_entry(raw, is_name) {
            Entry::Valid(entry) => Some(normalize_name(&entry)),
            _ => None,
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

impl JunkRules {
    /// Blank and invalid entries are ignored (an empty prefix would match every name).
    pub fn new(names: &[String], prefixes: &[String]) -> Self {
        Self {
            names: keys(names, true),
            prefixes: keys(prefixes, false),
        }
    }

    /// Rules that match nothing.
    pub fn none() -> Self {
        Self {
            names: Vec::new(),
            prefixes: Vec::new(),
        }
    }

    /// Whether a file or folder with this name is ignored.
    pub fn is_junk(&self, name: &str) -> bool {
        if name.is_empty() || (self.names.is_empty() && self.prefixes.is_empty()) {
            return false;
        }
        if name.is_ascii() {
            self.names.iter().any(|n| n.eq_ignore_ascii_case(name))
                || self.prefixes.iter().any(|p| {
                    name.as_bytes()
                        .get(..p.len())
                        .is_some_and(|head| head.eq_ignore_ascii_case(p.as_bytes()))
                })
        } else {
            let key = normalize_name(name);
            self.names.contains(&key) || self.prefixes.iter().any(|p| key.starts_with(p.as_str()))
        }
    }

    /// Whether any `/`-separated component of `path` is ignored.
    pub fn path_is_junk(&self, path: &str) -> bool {
        path.split('/').any(|name| self.is_junk(name))
    }
}

/// Which instance is primary, and who chose it when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Designation {
    pub instance_id: Option<String>,
    /// The primary's name when it was chosen.
    pub name: Option<String>,
    /// Milliseconds since the epoch.
    pub designated_at: i64,
    pub designated_by: String,
}

impl Designation {
    fn newer_than(&self, other: &Self) -> bool {
        (self.designated_at, &self.designated_by) > (other.designated_at, &other.designated_by)
    }
}

/// The replicated settings state of one instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSettings {
    pub primary: Designation,
    pub files: FileSettings,
    /// Grows whenever `files` change at their origin.
    pub revision: i64,
    /// The instance whose values `files` are.
    pub origin_instance_id: String,
}

/// The settings cannot be edited here: another instance is primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedByPrimary;

impl SharedSettings {
    /// The state of an instance that has never shared anything.
    pub fn initial(self_id: &str, files: FileSettings) -> Self {
        Self {
            // Equal on every instance, so it never replaces anything.
            primary: Designation {
                instance_id: None,
                name: None,
                designated_at: 0,
                designated_by: String::new(),
            },
            files,
            revision: 0,
            origin_instance_id: self_id.to_owned(),
        }
    }

    pub fn primary_id(&self) -> Option<&str> {
        self.primary.instance_id.as_deref()
    }

    /// Whether this instance may change `files`: it is primary, or there is none.
    pub fn editable_by(&self, self_id: &str) -> bool {
        self.primary_id().is_none_or(|p| p == self_id)
    }

    fn bump(&mut self, self_id: &str, now_ms: i64) {
        self.revision = now_ms.max(self.revision.saturating_add(1));
        self.origin_instance_id = self_id.to_owned();
    }

    /// Chooses the primary (`None`: every instance keeps its own values). True when it changed.
    pub fn designate(
        &mut self,
        primary: Option<(String, String)>,
        self_id: &str,
        now_ms: i64,
    ) -> bool {
        let (instance_id, name) = primary.unzip();
        if instance_id == self.primary.instance_id {
            return false;
        }
        self.primary = Designation {
            instance_id,
            name,
            designated_at: now_ms.max(self.primary.designated_at.saturating_add(1)),
            designated_by: self_id.to_owned(),
        };
        if self.primary_id() == Some(self_id) {
            self.bump(self_id, now_ms);
        }
        true
    }

    /// Replaces `files` with validated values. Ok(true) when they changed.
    pub fn set_files(
        &mut self,
        files: FileSettings,
        self_id: &str,
        now_ms: i64,
    ) -> Result<bool, ManagedByPrimary> {
        if files == self.files {
            return Ok(false);
        }
        if !self.editable_by(self_id) {
            return Err(ManagedByPrimary);
        }
        self.files = files;
        self.bump(self_id, now_ms);
        Ok(true)
    }

    /// Folds in the state a peer serves. True when anything changed.
    pub fn merge(&mut self, remote: &SharedSettings, self_id: &str, now_ms: i64) -> bool {
        let before = self.clone();
        if remote.primary.newer_than(&self.primary) {
            self.primary = remote.primary.clone();
            if self.primary_id() == Some(self_id) {
                // Newly made primary: our values become the reference, with a fresh revision.
                self.bump(self_id, now_ms);
            }
        }
        match self.primary.instance_id.clone() {
            Some(primary) if primary == self_id => {
                if self.origin_instance_id != self_id {
                    self.bump(self_id, now_ms);
                }
            }
            Some(primary) => {
                if remote.origin_instance_id == primary
                    && (self.origin_instance_id != primary || remote.revision > self.revision)
                {
                    self.files = remote.files.lenient();
                    self.revision = remote.revision;
                    self.origin_instance_id = primary;
                }
            }
            None => {}
        }
        *self != before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        owned(items)
    }

    fn files(exts: &[&str]) -> FileSettings {
        FileSettings {
            sidecar_extensions: strings(exts),
            ..FileSettings::default()
        }
    }

    #[test]
    fn defaults_are_valid() {
        let defaults = FileSettings::default();
        assert_eq!(defaults.normalized(), Ok(defaults.clone()));
        assert_eq!(defaults.lenient(), defaults);
    }

    #[test]
    fn normalizes_like_opm() {
        let input = FileSettings {
            sidecar_extensions: strings(&[" .XMP", "xmp", "", "pp3"]),
            junk_names: strings(&[" Thumbs.db ", "thumbs.DB", ""]),
            junk_prefixes: strings(&["._", "."]),
            name_max_len: 143,
        };
        let out = input.normalized().unwrap();
        assert_eq!(out.sidecar_extensions, strings(&["xmp", "pp3"]));
        assert_eq!(out.junk_names, strings(&["Thumbs.db"]));
        assert_eq!(out.junk_prefixes, strings(&["._", "."]));
        assert_eq!(out.name_max_len, 143);
    }

    #[test]
    fn rejects_invalid_entries_with_field_names() {
        let input = FileSettings {
            sidecar_extensions: strings(&["xmp", "x.y", "toolongextension"]),
            junk_names: strings(&["a/b", ".."]),
            junk_prefixes: strings(&["ok", "\0"]),
            name_max_len: 8,
        };
        let fields: Vec<String> = input
            .normalized()
            .unwrap_err()
            .into_iter()
            .map(|e| e.field)
            .collect();
        assert_eq!(
            fields,
            [
                "sidecar_extensions[1]",
                "sidecar_extensions[2]",
                "junk_names[0]",
                "junk_names[1]",
                "junk_prefixes[1]",
                "name_max_len",
            ]
        );
        let lenient = input.lenient();
        assert_eq!(lenient.sidecar_extensions, strings(&["xmp"]));
        assert!(lenient.junk_names.is_empty());
        assert_eq!(lenient.junk_prefixes, strings(&["ok"]));
        assert_eq!(lenient.name_max_len, 16);
    }

    #[test]
    fn enforces_list_sizes() {
        let many: Vec<String> = (0..60).map(|i| format!("e{i}")).collect();
        let input = FileSettings {
            sidecar_extensions: many,
            ..FileSettings::default()
        };
        assert!(input.normalized().is_err());
        assert_eq!(
            input.lenient().sidecar_extensions.len(),
            MAX_SIDECAR_EXTENSIONS
        );
    }

    #[test]
    fn junk_rules_match_names_and_prefixes() {
        let rules = JunkRules::default();
        for name in ["@eaDir", "@EADIR", ".DS_Store", "thumbs.db", "._IMG_1.jpg"] {
            assert!(rules.is_junk(name), "{name}");
        }
        for name in ["IMG_1.jpg", "Thumbs.db.jpg", "_IMG", ""] {
            assert!(!rules.is_junk(name), "{name}");
        }
        assert!(rules.path_is_junk("2024/@eaDir/x.jpg"));
        assert!(!rules.path_is_junk("2024/trip/x.jpg"));
        assert!(!JunkRules::none().is_junk(".DS_Store"));
    }

    #[test]
    fn junk_rules_ignore_unicode_forms_and_blank_prefixes() {
        let rules = JunkRules::new(&strings(&["Cafe\u{301}"]), &strings(&["", "  "]));
        assert!(rules.is_junk("caf\u{e9}"));
        assert!(rules.is_junk("CAFÉ"));
        assert!(!rules.is_junk("anything"));
    }

    const A: &str = "ism_a";
    const B: &str = "ism_b";
    const C: &str = "ism_c";

    #[test]
    fn without_a_primary_values_stay_local() {
        let mut a = SharedSettings::initial(A, files(&["xmp"]));
        let b = SharedSettings::initial(B, files(&["pp3"]));
        assert!(a.editable_by(A));
        assert!(!a.merge(&b, A, 10));
        assert_eq!(a.files.sidecar_extensions, strings(&["xmp"]));
    }

    #[test]
    fn the_primary_spreads_its_values_through_relays() {
        let mut a = SharedSettings::initial(A, files(&["xmp"]));
        let mut b = SharedSettings::initial(B, files(&["pp3"]));
        let mut c = SharedSettings::initial(C, files(&["dop"]));
        assert!(b.designate(Some((A.into(), "Alpha".into())), B, 100));
        assert!(!b.editable_by(B));
        // A learns it is primary and claims its values.
        assert!(a.merge(&b, A, 200));
        assert_eq!(a.origin_instance_id, A);
        assert!(a.revision >= 200);
        // B adopts A's values, C gets them through B.
        assert!(b.merge(&a, B, 300));
        assert_eq!(b.files.sidecar_extensions, strings(&["xmp"]));
        assert!(c.merge(&b, C, 400));
        assert_eq!(c.files.sidecar_extensions, strings(&["xmp"]));
        assert_eq!(c.primary.instance_id.as_deref(), Some(A));
        assert!(!c.merge(&b, C, 500), "nothing new");

        // Edits at A reach the others; a stale relay does not undo them.
        let stale = b.clone();
        assert_eq!(a.set_files(files(&["xmp", "aae"]), A, 600), Ok(true));
        assert!(b.merge(&a, B, 700));
        assert!(!b.merge(&stale, B, 800));
        assert_eq!(b.files.sidecar_extensions, strings(&["xmp", "aae"]));
        assert_eq!(b.set_files(files(&["json"]), B, 900), Err(ManagedByPrimary));
    }

    #[test]
    fn the_latest_designation_wins_everywhere() {
        let mut a = SharedSettings::initial(A, files(&["xmp"]));
        let mut b = SharedSettings::initial(B, files(&["pp3"]));
        a.designate(Some((A.into(), "Alpha".into())), A, 100);
        b.designate(Some((B.into(), "Beta".into())), B, 200);
        a.merge(&b, A, 300);
        b.merge(&a, B, 300);
        assert_eq!(a.primary, b.primary);
        assert_eq!(a.files, b.files);
        assert_eq!(a.primary_id(), Some(B));
        // Going back to independent instances keeps the current values.
        assert!(a.designate(None, A, 400));
        assert!(b.merge(&a, B, 500));
        assert_eq!(b.primary_id(), None);
        assert!(b.editable_by(B));
        assert_eq!(b.files.sidecar_extensions, strings(&["pp3"]));
    }

    #[test]
    fn a_renewed_designation_gets_a_fresh_revision() {
        let mut a = SharedSettings::initial(A, files(&["xmp"]));
        let mut b = SharedSettings::initial(B, files(&["pp3"]));
        a.set_files(files(&["xmp", "dop"]), A, 50).unwrap();
        b.set_files(files(&["pp3", "dop"]), B, 900).unwrap();
        b.designate(Some((A.into(), "Alpha".into())), B, 1000);
        a.merge(&b, A, 1100);
        assert!(a.revision > b.revision);
        assert!(b.merge(&a, B, 1200));
        assert_eq!(b.files.sidecar_extensions, strings(&["xmp", "dop"]));
    }
}
