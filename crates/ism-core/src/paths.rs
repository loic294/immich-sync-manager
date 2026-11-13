//! Logical paths: case-exact, `/`-separated, relative to a logical root.
//!
//! Validation is syntactic and conservative. It never rewrites a name (no Unicode or case
//! normalisation of what is on disk); it only rejects names that are unsafe or that would behave
//! differently on the hosts we support (Linux, Synology, Docker Desktop on Windows bind mounts).

use std::fmt;

use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PathError {
    #[error("the path must be relative")]
    Absolute,
    #[error("the path contains an empty, '.' or '..' segment")]
    BadSegment,
    #[error("the path contains a control character or backslash")]
    BadCharacter,
    #[error("the name '{0}' is reserved on Windows hosts")]
    Reserved(String),
    #[error("a name ends with a space or a dot, which Windows hosts cannot store")]
    TrailingDotOrSpace,
    #[error("the path is too long")]
    TooLong,
}

const MAX_PATH_BYTES: usize = 4096;
const MAX_SEGMENT_BYTES: usize = 255;
const WINDOWS_RESERVED: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// A validated relative path. The empty path is the root itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelPath(String);

impl RelPath {
    pub fn root() -> Self {
        Self(String::new())
    }

    pub fn parse(raw: &str) -> Result<Self, PathError> {
        if raw.is_empty() {
            return Ok(Self::root());
        }
        if raw.starts_with('/') {
            return Err(PathError::Absolute);
        }
        if raw.len() > MAX_PATH_BYTES {
            return Err(PathError::TooLong);
        }
        for segment in raw.split('/') {
            validate_segment(segment)?;
        }
        Ok(Self(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|s| !s.is_empty())
    }

    pub fn name(&self) -> Option<&str> {
        self.segments().last()
    }

    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        Some(match self.0.rfind('/') {
            Some(index) => Self(self.0[..index].to_owned()),
            None => Self::root(),
        })
    }

    /// Appends one validated segment.
    pub fn join(&self, name: &str) -> Result<Self, PathError> {
        validate_segment(name)?;
        Ok(if self.is_root() {
            Self(name.to_owned())
        } else {
            Self(format!("{}/{name}", self.0))
        })
    }

    /// True when `self` is `other` or lies below it, compared segment by segment (so `a/bc` is
    /// not inside `a/b`).
    pub fn starts_with(&self, other: &RelPath) -> bool {
        if other.is_root() {
            return true;
        }
        self.0 == other.0
            || (self.0.starts_with(&other.0) && self.0.as_bytes().get(other.0.len()) == Some(&b'/'))
    }

    /// `self` with the `from` prefix replaced by `to`. `None` when `self` is not below `from`.
    pub fn rebase(&self, from: &RelPath, to: &RelPath) -> Option<Self> {
        if !self.starts_with(from) {
            return None;
        }
        let rest = self.0[from.0.len()..].trim_start_matches('/');
        Some(match (to.is_root(), rest.is_empty()) {
            (_, true) => to.clone(),
            (true, false) => Self(rest.to_owned()),
            (false, false) => Self(format!("{}/{rest}", to.0)),
        })
    }

    /// A key that collides for names a case-insensitive or normalising file system (Windows,
    /// macOS, some SMB shares) would treat as the same file.
    pub fn collision_key(&self) -> String {
        self.0.nfc().collect::<String>().to_lowercase()
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_segment(segment: &str) -> Result<(), PathError> {
    if segment.is_empty() || segment == "." || segment == ".." {
        return Err(PathError::BadSegment);
    }
    if segment.len() > MAX_SEGMENT_BYTES {
        return Err(PathError::TooLong);
    }
    if segment
        .chars()
        .any(|c| c.is_control() || c == '\\' || c == '/')
    {
        return Err(PathError::BadCharacter);
    }
    if segment.ends_with(' ') || segment.ends_with('.') {
        return Err(PathError::TrailingDotOrSpace);
    }
    let stem = segment
        .split('.')
        .next()
        .unwrap_or(segment)
        .to_ascii_uppercase();
    if WINDOWS_RESERVED.contains(&stem.as_str()) {
        return Err(PathError::Reserved(segment.to_owned()));
    }
    Ok(())
}

/// Splits `name` into stem and extension at the last dot (`a.b.jpg` -> `a.b`, `jpg`).
pub fn split_ext(name: &str) -> (&str, Option<&str>) {
    match name.rfind('.') {
        Some(index) if index > 0 => (&name[..index], Some(&name[index + 1..])),
        _ => (name, None),
    }
}

/// Normalises an absolute POSIX prefix such as an Immich import path or a container mount:
/// requires a leading `/`, collapses repeated separators and removes a trailing one.
pub fn normalize_abs_prefix(raw: &str) -> Result<String, PathError> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('/') {
        return Err(PathError::Absolute);
    }
    let mut out = String::new();
    for segment in trimmed.split('/').filter(|s| !s.is_empty()) {
        if segment == "." || segment == ".." {
            return Err(PathError::BadSegment);
        }
        if segment.chars().any(|c| c.is_control()) {
            return Err(PathError::BadCharacter);
        }
        out.push('/');
        out.push_str(segment);
    }
    if out.is_empty() {
        out.push('/');
    }
    Ok(out)
}

/// The relative part of absolute `path` below absolute `prefix`, compared by segments.
pub fn strip_abs_prefix(path: &str, prefix: &str) -> Option<String> {
    if prefix == "/" {
        return Some(path.trim_start_matches('/').to_owned());
    }
    let rest = path.strip_prefix(prefix)?;
    if rest.is_empty() {
        return Some(String::new());
    }
    rest.strip_prefix('/').map(str::to_owned)
}

/// Joins an absolute prefix and a relative path.
pub fn join_abs(prefix: &str, rel: &RelPath) -> String {
    match (prefix == "/", rel.is_root()) {
        (_, true) => prefix.to_owned(),
        (true, false) => format!("/{rel}"),
        (false, false) => format!("{prefix}/{rel}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_paths() {
        assert_eq!(RelPath::parse("/a"), Err(PathError::Absolute));
        assert_eq!(RelPath::parse("a//b"), Err(PathError::BadSegment));
        assert_eq!(RelPath::parse("a/../b"), Err(PathError::BadSegment));
        assert_eq!(RelPath::parse("a\\b"), Err(PathError::BadCharacter));
        assert!(matches!(
            RelPath::parse("a/con.txt"),
            Err(PathError::Reserved(_))
        ));
        assert_eq!(RelPath::parse("a/b."), Err(PathError::TrailingDotOrSpace));
        assert!(RelPath::parse("2024/Trip/IMG_0001.CR3").is_ok());
    }

    #[test]
    fn containment_is_segment_aware() {
        let base = RelPath::parse("a/b").unwrap();
        assert!(RelPath::parse("a/b/c").unwrap().starts_with(&base));
        assert!(RelPath::parse("a/b").unwrap().starts_with(&base));
        assert!(!RelPath::parse("a/bc").unwrap().starts_with(&base));
        assert!(RelPath::parse("x").unwrap().starts_with(&RelPath::root()));
    }

    #[test]
    fn rebase_moves_subtrees() {
        let p = RelPath::parse("old/x/y.jpg").unwrap();
        let moved = p
            .rebase(
                &RelPath::parse("old").unwrap(),
                &RelPath::parse("new/deeper").unwrap(),
            )
            .unwrap();
        assert_eq!(moved.as_str(), "new/deeper/x/y.jpg");
        assert_eq!(
            p.rebase(&RelPath::parse("old").unwrap(), &RelPath::root())
                .unwrap()
                .as_str(),
            "x/y.jpg"
        );
    }

    #[test]
    fn absolute_prefixes() {
        assert_eq!(normalize_abs_prefix("/a//b/").unwrap(), "/a/b");
        assert!(normalize_abs_prefix("a/b").is_err());
        assert!(normalize_abs_prefix("/a/../b").is_err());
        assert_eq!(
            strip_abs_prefix("/a/b/c.jpg", "/a/b").as_deref(),
            Some("c.jpg")
        );
        assert_eq!(strip_abs_prefix("/a/bc/c.jpg", "/a/b"), None);
        assert_eq!(join_abs("/m", &RelPath::parse("x/y").unwrap()), "/m/x/y");
    }

    #[test]
    fn collision_key_folds_case_and_normalisation() {
        let composed = RelPath::parse("Caf\u{e9}.jpg").unwrap();
        let decomposed = RelPath::parse("cafe\u{301}.JPG").unwrap();
        assert_eq!(composed.collision_key(), decomposed.collision_key());
    }
}
