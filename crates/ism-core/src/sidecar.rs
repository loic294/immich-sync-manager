//! Sidecar and companion grouping.
//!
//! A group is one original plus the files that belong to it:
//! - `photo.ext.xmp` (Immich's and darktable's convention) belongs to `photo.ext` only;
//! - `photo.xmp` (Lightroom/Capture One convention) belongs to every original with stem `photo`;
//! - editor companions (`.pp3`, `.dop`, `.on1`, `.aae`, ...) follow the same two forms.
//!
//! A companion that belongs to several originals is *shared*: moving one of them copies the
//! companion instead of moving it, so the other original keeps its own copy.

use crate::paths::split_ext;

/// Extensions that are never originals. `xmp` is the metadata sidecar Immich links.
pub const SIDECAR_EXTENSIONS: &[&str] = &["xmp"];
/// Default editor companions. Configurable per installation.
pub const DEFAULT_COMPANION_EXTENSIONS: &[&str] = &["pp3", "dop", "on1", "aae", "arp", "cos"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompanionRole {
    Sidecar,
    Companion,
}

impl CompanionRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sidecar => "sidecar",
            Self::Companion => "companion",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Companion {
    pub name: String,
    pub role: CompanionRole,
    /// Another original in the same folder also owns this file.
    pub shared: bool,
}

fn ext_role(ext: &str, companions: &[String]) -> Option<CompanionRole> {
    let lower = ext.to_ascii_lowercase();
    if SIDECAR_EXTENSIONS.contains(&lower.as_str()) {
        Some(CompanionRole::Sidecar)
    } else if companions.iter().any(|c| c.eq_ignore_ascii_case(&lower)) {
        Some(CompanionRole::Companion)
    } else {
        None
    }
}

/// True when `name` is a sidecar or companion rather than an original.
pub fn is_companion_name(name: &str, companions: &[String]) -> bool {
    matches!(split_ext(name), (_, Some(ext)) if ext_role(ext, companions).is_some())
}

/// The companions of `original` among `siblings` (names in the same folder, may include the
/// original itself).
pub fn companions_of(original: &str, siblings: &[String], companions: &[String]) -> Vec<Companion> {
    let (stem, _) = split_ext(original);
    let originals_with_stem = siblings
        .iter()
        .filter(|s| !is_companion_name(s, companions) && split_ext(s).0 == stem)
        .count()
        .max(1);
    let mut out: Vec<Companion> = siblings
        .iter()
        .filter(|name| name.as_str() != original)
        .filter_map(|name| {
            let (name_stem, ext) = split_ext(name);
            let role = ext_role(ext?, companions)?;
            if name_stem == original {
                Some(Companion {
                    name: name.clone(),
                    role,
                    shared: false,
                })
            } else if name_stem == stem {
                Some(Companion {
                    name: name.clone(),
                    role,
                    shared: originals_with_stem > 1,
                })
            } else {
                None
            }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The name a companion takes when its original is renamed from `old_original` to
/// `new_original`: `a.jpg.xmp` -> `b.jpg.xmp`, `a.xmp` -> `b.xmp`.
pub fn renamed_companion(companion: &str, old_original: &str, new_original: &str) -> String {
    if let Some(rest) = companion.strip_prefix(old_original)
        && rest.starts_with('.')
    {
        return format!("{new_original}{rest}");
    }
    let (old_stem, _) = split_ext(old_original);
    let (new_stem, _) = split_ext(new_original);
    match companion.strip_prefix(old_stem) {
        Some(rest) if rest.starts_with('.') => format!("{new_stem}{rest}"),
        _ => companion.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    fn defaults() -> Vec<String> {
        names(DEFAULT_COMPANION_EXTENSIONS)
    }

    #[test]
    fn finds_both_sidecar_conventions_and_companions() {
        let siblings = names(&["a.CR3", "a.CR3.xmp", "a.xmp", "a.pp3", "b.jpg", "b.jpg.xmp"]);
        let found = companions_of("a.CR3", &siblings, &defaults());
        let found: Vec<_> = found.iter().map(|c| (c.name.as_str(), c.shared)).collect();
        assert_eq!(
            found,
            vec![("a.CR3.xmp", false), ("a.pp3", false), ("a.xmp", false)]
        );
    }

    #[test]
    fn stem_sidecars_are_shared_between_originals() {
        let siblings = names(&["a.CR3", "a.jpg", "a.xmp", "a.jpg.xmp"]);
        let raw = companions_of("a.CR3", &siblings, &defaults());
        assert_eq!(raw.len(), 1);
        assert!(raw[0].shared, "a.xmp is shared by a.CR3 and a.jpg");
        let jpg = companions_of("a.jpg", &siblings, &defaults());
        assert!(jpg.iter().any(|c| c.name == "a.jpg.xmp" && !c.shared));
    }

    #[test]
    fn companion_names_follow_renames() {
        assert_eq!(
            renamed_companion("a.jpg.xmp", "a.jpg", "b.jpg"),
            "b.jpg.xmp"
        );
        assert_eq!(renamed_companion("a.xmp", "a.jpg", "b.jpg"), "b.xmp");
        assert_eq!(renamed_companion("zzz.xmp", "a.jpg", "b.jpg"), "zzz.xmp");
    }

    #[test]
    fn companions_are_not_originals() {
        assert!(is_companion_name("x.XMP", &defaults()));
        assert!(is_companion_name("x.jpg.pp3", &defaults()));
        assert!(!is_companion_name("x.jpg", &defaults()));
    }
}
