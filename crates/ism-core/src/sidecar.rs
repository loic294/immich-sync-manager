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

/// Extensions that are always sidecars, whatever the settings say: `xmp` is the metadata sidecar
/// Immich links. The other configured sidecar extensions
/// ([`FileSettings::sidecar_extensions`](crate::dto::FileSettings)) are companions.
pub const SIDECAR_EXTENSIONS: &[&str] = &["xmp"];

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

pub fn companion_role(name: &str, companions: &[String]) -> Option<CompanionRole> {
    split_ext(name).1.and_then(|ext| ext_role(ext, companions))
}

/// True when `name` is a sidecar or companion rather than an original.
pub fn is_companion_name(name: &str, companions: &[String]) -> bool {
    companion_role(name, companions).is_some()
}

fn edited_base(stem: &str) -> Option<&str> {
    let lower = stem.to_ascii_lowercase();
    for marker in [".edited.", "-edited-"] {
        if let Some(index) = lower.rfind(marker) {
            let suffix = &stem[index + marker.len()..];
            if !suffix.is_empty()
                && suffix.chars().any(|c| c.is_ascii_digit())
                && suffix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '+'))
            {
                return Some(&stem[..index]);
            }
        }
    }
    None
}

fn original_pair_key(name: &str) -> &str {
    let stem = split_ext(name).0;
    edited_base(stem).unwrap_or(stem)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum OriginalKind {
    Still,
    Raw,
    Video,
    Other,
}

fn original_kind(name: &str) -> OriginalKind {
    let (_, Some(extension)) = split_ext(name) else {
        return OriginalKind::Other;
    };
    match extension.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" | "heic" | "heif" | "png" | "webp" | "avif" | "tif" | "tiff" => {
            OriginalKind::Still
        }
        "3fr" | "ari" | "arw" | "bay" | "cap" | "cr2" | "cr3" | "crw" | "dcr" | "dng" | "erf"
        | "fff" | "gpr" | "iiq" | "k25" | "kdc" | "mef" | "mos" | "mrw" | "nef" | "nrw" | "orf"
        | "pef" | "ptx" | "r3d" | "raf" | "raw" | "rw2" | "rwl" | "rwz" | "sr2" | "srf" | "srw"
        | "x3f" => OriginalKind::Raw,
        "3gp" | "avi" | "m2ts" | "m4v" | "mkv" | "mov" | "mp4" | "mpeg" | "mpg" | "mts" | "ogv"
        | "ts" | "webm" => OriginalKind::Video,
        _ => OriginalKind::Other,
    }
}

fn kind_preference(name: &str) -> u8 {
    if edited_base(split_ext(name).0).is_some() {
        return 8;
    }
    match original_kind(name) {
        OriginalKind::Still
            if split_ext(name).1.is_some_and(|ext| {
                ext.eq_ignore_ascii_case("jpg") || ext.eq_ignore_ascii_case("jpeg")
            }) =>
        {
            0
        }
        OriginalKind::Still => 1,
        OriginalKind::Raw => 2,
        OriginalKind::Video => 3,
        OriginalKind::Other => 4,
    }
}

/// Selects a deterministic anchor for a companion shared by paired originals.
///
/// Paired groups are one still image plus RAW/video variants, or an original and one of its
/// timestamped edited variants. Unrelated files with the same stem remain ambiguous.
pub fn preferred_paired_original<'a>(names: &[&'a str]) -> Option<&'a str> {
    let first = *names.first()?;
    if names.len() == 1 {
        return Some(first);
    }
    let key = original_pair_key(first);
    if names
        .iter()
        .any(|name| !original_pair_key(name).eq_ignore_ascii_case(key))
    {
        return None;
    }
    let has_edit = names
        .iter()
        .any(|name| edited_base(split_ext(name).0).is_some());
    let kinds: Vec<_> = names.iter().map(|name| original_kind(name)).collect();
    let known_media = kinds.iter().all(|kind| *kind != OriginalKind::Other);
    let paired = known_media
        && (has_edit
            || kinds
                .iter()
                .filter(|kind| **kind == OriginalKind::Still)
                .count()
                == 1);
    if !paired {
        return None;
    }
    names
        .iter()
        .min_by_key(|name| {
            (
                kind_preference(name),
                name.to_ascii_lowercase(),
                (*name).to_owned(),
            )
        })
        .copied()
}

/// The companions of `original` among `siblings` (names in the same folder, may include the
/// original itself).
pub fn companions_of(original: &str, siblings: &[String], companions: &[String]) -> Vec<Companion> {
    let pair_key = original_pair_key(original);
    let originals_with_stem = siblings
        .iter()
        .filter(|s| {
            !is_companion_name(s, companions) && original_pair_key(s).eq_ignore_ascii_case(pair_key)
        })
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
            } else if !siblings.iter().any(|candidate| {
                candidate == name_stem && !is_companion_name(candidate, companions)
            }) && original_pair_key(name_stem).eq_ignore_ascii_case(pair_key)
            {
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
        _ => {
            let old_key = original_pair_key(old_original);
            let new_key = original_pair_key(new_original);
            match companion.strip_prefix(old_key) {
                Some(rest) if rest.starts_with('.') => format!("{new_key}{rest}"),
                _ => companion.to_owned(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    fn defaults() -> Vec<String> {
        names(crate::file_settings::DEFAULT_SIDECAR_EXTENSIONS)
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
    fn edited_variants_share_stem_sidecars() {
        for edited in ["a.edited.1730000000.jpg", "a-edited-1730000000.jpg"] {
            let siblings = names(&["a.jpg", edited, "a.xmp"]);
            for original in ["a.jpg", edited] {
                let companions = companions_of(original, &siblings, &defaults());
                assert!(companions.iter().any(|c| c.name == "a.xmp" && c.shared));
            }
        }
    }

    #[test]
    fn selects_safe_anchor_for_paired_originals() {
        for (originals, expected) in [
            (vec!["a.jpg", "a.CR3"], "a.jpg"),
            (vec!["a.JPG", "a.MOV"], "a.JPG"),
            (vec!["a.jpg", "a.MOV", "a.CR3"], "a.jpg"),
            (vec!["a.jpg", "a.edited.1730000000.jpg"], "a.jpg"),
            (vec!["a.jpg", "a-edited-1730000000.jpg"], "a.jpg"),
        ] {
            assert_eq!(preferred_paired_original(&originals), Some(expected));
        }
        assert_eq!(preferred_paired_original(&["a.jpg", "a.png"]), None);
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
        assert_eq!(
            companion_role("x.XMP", &defaults()),
            Some(CompanionRole::Sidecar)
        );
        assert_eq!(
            companion_role("x.jpg.pp3", &defaults()),
            Some(CompanionRole::Companion)
        );
    }
}
