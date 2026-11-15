//! Paths the watcher never reports: ISM's own folders, partial or temporary files, and the
//! Immich library's exclusion patterns.

use crate::files::HIDDEN;

/// ISM folders and files (sentinel, trash, staging, in-flight copies) and partial downloads.
pub fn ignored(rel: &str) -> bool {
    rel.split('/').any(|name| {
        HIDDEN.contains(&name)
            || name.starts_with(".ism")
            || name == ".DS_Store"
            || name.starts_with("~$")
            || name.starts_with(".~")
            || name.ends_with('~')
            || [
                ".part",
                ".partial",
                ".tmp",
                ".temp",
                ".crdownload",
                ".download",
                ".swp",
            ]
            .iter()
            .any(|ext| name.len() > ext.len() && name.to_ascii_lowercase().ends_with(ext))
    })
}

/// Whether `path` matches an Immich exclusion pattern (`**` crosses folders, `*` and `?` do not).
pub fn excluded(patterns: &[String], path: &str) -> bool {
    patterns.iter().any(|p| glob(p.as_bytes(), path.as_bytes()))
}

fn glob(p: &[u8], s: &[u8]) -> bool {
    match p {
        [] => s.is_empty(),
        [b'*', b'*', b'/', rest @ ..] => {
            // `**/` matches zero or more whole folders.
            glob(rest, s)
                || s.iter()
                    .enumerate()
                    .any(|(i, c)| *c == b'/' && glob(rest, &s[i + 1..]))
        }
        [b'*', b'*', rest @ ..] => (0..=s.len()).any(|i| glob(rest, &s[i..])),
        [b'*', rest @ ..] => {
            let end = s.iter().position(|c| *c == b'/').unwrap_or(s.len());
            (0..=end).any(|i| glob(rest, &s[i..]))
        }
        [b'?', rest @ ..] => s.first().is_some_and(|c| *c != b'/') && glob(rest, &s[1..]),
        [c, rest @ ..] => s.first() == Some(c) && glob(rest, &s[1..]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ism_and_partial_files_are_ignored() {
        for rel in [
            ".ism-root",
            "a/.ism-trash/x.jpg",
            ".ism-staging/up/1",
            "a/.ism-copy-1.jpg",
            "a/b.jpg.part",
            "a/~$doc.docx",
            "a/.DS_Store",
            "a/b.crdownload",
        ] {
            assert!(ignored(rel), "{rel}");
        }
        for rel in ["a/b.jpg", "2024/trip/IMG_0001.HEIC", "a/.part"] {
            assert!(!ignored(rel), "{rel}");
        }
    }

    #[test]
    fn exclusion_globs() {
        let p = vec![
            "**/.ism-trash/**".to_owned(),
            "**/@eaDir/**".to_owned(),
            "/library/raw/*.tmp".to_owned(),
        ];
        assert!(excluded(&p, "/library/a/.ism-trash/x.jpg"));
        assert!(excluded(&p, "/library/@eaDir/x/y.jpg"));
        assert!(excluded(&p, "/library/raw/a.tmp"));
        assert!(!excluded(&p, "/library/raw/sub/a.tmp"));
        assert!(!excluded(&p, "/library/a/b.jpg"));
        assert!(excluded(&["**/*.xmp".to_owned()], "/l/a/b.xmp"));
        assert!(excluded(&["/l/a?c.jpg".to_owned()], "/l/abc.jpg"));
    }
}
