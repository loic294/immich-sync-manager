//! Path mappings between the logical roots of this instance and the paths other systems use.
//!
//! One mapping ties a logical root to:
//! - the directory where the volume is mounted in the sync-manager container,
//! - the import path Immich uses for the same directory (inside the Immich container),
//! - optionally the prefix Open Photo Manager uses for it.
//!
//! Immich-only and OPM-only mappings may share container folders. Within a source, mappings
//! must not overlap, otherwise one file would have two identities.

use thiserror::Error;

use crate::paths::{PathError, RelPath, join_abs, normalize_abs_prefix, strip_abs_prefix};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootMapping {
    pub root_id: String,
    pub container_path: String,
    pub immich_import_path: Option<String>,
    pub opm_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MappingError {
    #[error("root id must be 1-64 characters of a-z, 0-9, '-' or '_'")]
    BadRootId,
    #[error("{field}: {source}")]
    BadPath {
        field: &'static str,
        source: PathError,
    },
    #[error("{field} of '{a}' overlaps with '{b}'")]
    Overlap {
        field: &'static str,
        a: String,
        b: String,
    },
    #[error("root id '{0}' is used twice")]
    DuplicateId(String),
}

pub fn validate_root_id(id: &str) -> Result<(), MappingError> {
    let ok = !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        Err(MappingError::BadRootId)
    }
}

impl RootMapping {
    /// Validates and normalises every prefix.
    pub fn normalized(self) -> Result<Self, MappingError> {
        validate_root_id(&self.root_id)?;
        let norm = |field: &'static str, value: &str| {
            normalize_abs_prefix(value).map_err(|source| MappingError::BadPath { field, source })
        };
        Ok(Self {
            container_path: norm("container_path", &self.container_path)?,
            immich_import_path: match self.immich_import_path.as_deref().map(str::trim) {
                Some(p) if !p.is_empty() => Some(norm("immich_import_path", p)?),
                _ => None,
            },
            opm_prefix: match self.opm_prefix.as_deref().map(str::trim) {
                Some(p) if !p.is_empty() => Some(norm("opm_prefix", p)?),
                _ => None,
            },
            root_id: self.root_id,
        })
    }

    pub fn container_abs(&self, rel: &RelPath) -> String {
        join_abs(&self.container_path, rel)
    }

    pub fn immich_abs(&self, rel: &RelPath) -> Option<String> {
        self.immich_import_path.as_deref().map(|p| join_abs(p, rel))
    }
}

fn overlaps(a: &str, b: &str) -> bool {
    strip_abs_prefix(a, b).is_some() || strip_abs_prefix(b, a).is_some()
}

/// Checks a complete set of (already normalised) mappings for duplicates and overlaps.
pub fn validate_set(mappings: &[RootMapping]) -> Result<(), MappingError> {
    for (i, a) in mappings.iter().enumerate() {
        for b in &mappings[i + 1..] {
            if a.root_id == b.root_id {
                return Err(MappingError::DuplicateId(a.root_id.clone()));
            }
            let independent = |immich: &RootMapping, opm: &RootMapping| {
                immich.immich_import_path.is_some()
                    && immich.opm_prefix.is_none()
                    && opm.immich_import_path.is_none()
                    && opm.opm_prefix.is_some()
            };
            let independent = independent(a, b) || independent(b, a);
            let pairs: [(&'static str, Option<&str>, Option<&str>); 3] = [
                (
                    "container_path",
                    Some(&a.container_path),
                    Some(&b.container_path),
                ),
                (
                    "immich_import_path",
                    a.immich_import_path.as_deref(),
                    b.immich_import_path.as_deref(),
                ),
                (
                    "opm_prefix",
                    a.opm_prefix.as_deref(),
                    b.opm_prefix.as_deref(),
                ),
            ];
            for (field, x, y) in pairs {
                if field == "container_path" && independent {
                    continue;
                }
                if let (Some(x), Some(y)) = (x, y)
                    && overlaps(x, y)
                {
                    return Err(MappingError::Overlap {
                        field,
                        a: a.root_id.clone(),
                        b: b.root_id.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Which side an absolute path comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSide {
    Container,
    Immich,
    Opm,
}

/// Resolves an absolute path of one side to `(root_id, relative path)`.
/// Ambiguous container paths require an explicit source or root ID.
pub fn resolve<'a>(
    mappings: &'a [RootMapping],
    side: PathSide,
    abs: &str,
) -> Option<(&'a RootMapping, RelPath)> {
    let mut matches = mappings.iter().filter_map(|m| {
        let prefix = match side {
            PathSide::Container => Some(m.container_path.as_str()),
            PathSide::Immich => m.immich_import_path.as_deref(),
            PathSide::Opm => m.opm_prefix.as_deref(),
        }?;
        let rest = strip_abs_prefix(abs, prefix)?;
        RelPath::parse(&rest).ok().map(|rel| (m, rel))
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(id: &str, container: &str, immich: Option<&str>) -> RootMapping {
        RootMapping {
            root_id: id.into(),
            container_path: container.into(),
            immich_import_path: immich.map(Into::into),
            opm_prefix: None,
        }
        .normalized()
        .unwrap()
    }

    #[test]
    fn overlapping_roots_are_rejected() {
        let set = [
            m("a", "/mnt/photos", None),
            m("b", "/mnt/photos/2024", None),
        ];
        assert!(matches!(
            validate_set(&set),
            Err(MappingError::Overlap { .. })
        ));
        let ok = [m("a", "/mnt/photos", None), m("b", "/mnt/photos2", None)];
        assert!(validate_set(&ok).is_ok());
    }

    #[test]
    fn resolves_immich_paths() {
        let set = [m("photos", "/mnt/photos", Some("/external/photos"))];
        let (root, rel) = resolve(&set, PathSide::Immich, "/external/photos/2024/a.jpg").unwrap();
        assert_eq!(root.root_id, "photos");
        assert_eq!(rel.as_str(), "2024/a.jpg");
        assert_eq!(root.container_abs(&rel), "/mnt/photos/2024/a.jpg");
        assert!(resolve(&set, PathSide::Immich, "/external/photosX/a.jpg").is_none());
        assert!(resolve(&set, PathSide::Immich, "/external/photos/../etc").is_none());
    }

    #[test]
    fn different_sources_can_share_container_folders() {
        for folder in ["/mnt/photos", "/mnt/photos/2024"] {
            let mut opm = m("opm", "/mnt/photos", None);
            opm.opm_prefix = Some("/opm/photos".into());
            let set = [opm, m("immich", folder, Some("/external/photos"))];
            assert_eq!(validate_set(&set), Ok(()));
            let (root, rel) = resolve(&set, PathSide::Opm, "/opm/photos/2024/a.jpg").unwrap();
            assert_eq!(root.root_id, "opm");
            assert_eq!(rel.as_str(), "2024/a.jpg");
            let (root, _) = resolve(&set, PathSide::Immich, "/external/photos/a.jpg").unwrap();
            assert_eq!(root.root_id, "immich");
            assert!(resolve(&set, PathSide::Container, "/mnt/photos/2024/a.jpg").is_none());
        }
    }

    #[test]
    fn same_source_container_overlap_is_rejected() {
        let mut opm = m("opm", "/mnt/photos", None);
        opm.opm_prefix = Some("/opm/photos".into());
        let mut other = m("other", "/mnt/photos/2024", None);
        other.opm_prefix = Some("/opm/archive".into());
        assert!(validate_set(&[opm, other]).is_err());
        assert!(
            validate_set(&[
                m("a", "/mnt/photos", Some("/external/a")),
                m("b", "/mnt/photos/2024", Some("/external/b")),
            ])
            .is_err()
        );
    }

    #[test]
    fn root_ids_are_restricted() {
        assert!(validate_root_id("photos-2024").is_ok());
        assert!(validate_root_id("Photos").is_err());
        assert!(validate_root_id("").is_err());
    }
}
