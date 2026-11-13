//! Immich's path-derived checksum of external-library assets (`checksumAlgorithm = 'sha1-path'`).
//!
//! Immich v3.4.1 stores `sha1("path:" + originalPath)` for external assets it imported. When such
//! an asset moves, the checksum must follow the new path, otherwise the next scan treats the moved
//! file as a different asset. Content checksums (`'sha1'`) are never rewritten.

use sha1::{Digest, Sha1};

/// The 20-byte checksum Immich stores for an external asset at `original_path`.
pub fn immich_path_checksum(original_path: &str) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(b"path:");
    hasher.update(original_path.as_bytes());
    hasher.finalize().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_immich_formula() {
        // sha1("path:/a.jpg"), computed independently.
        let expected = {
            let mut h = Sha1::new();
            h.update("path:/a.jpg");
            h.finalize().to_vec()
        };
        assert_eq!(immich_path_checksum("/a.jpg"), expected);
        assert_eq!(immich_path_checksum("/a.jpg").len(), 20);
        assert_ne!(
            immich_path_checksum("/a.jpg"),
            immich_path_checksum("/b.jpg")
        );
    }
}
