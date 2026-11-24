//! Which file names Immich v3.4.1 accepts as assets (`server/src/utils/mime-types.ts`, `isAsset`).

use crate::paths::split_ext;

const IMAGE: &[&str] = &[
    "3fr", "ari", "arw", "cap", "cin", "cr2", "cr3", "crw", "dcr", "dng", "erf", "fff", "iiq",
    "k25", "kdc", "mrw", "nef", "nrw", "orf", "ori", "pef", "psd", "raf", "raw", "rw2", "rwl",
    "sr2", "srf", "srw", "x3f", "avif", "bmp", "gif", "jfif", "jpeg", "jpg", "png", "webp", "heic",
    "heif", "hif", "insp", "jp2", "jpe", "jxl", "mpo", "svg", "tif", "tiff",
];

const VIDEO: &[&str] = &[
    "3gp", "3gpp", "avi", "flv", "insv", "m2t", "m2ts", "m4v", "mkv", "mov", "mp4", "mpe", "mpeg",
    "mpg", "mts", "mxf", "ts", "vob", "webm", "wmv",
];

/// True when Immich would import `name` as an image or video asset, judged like Immich by its
/// last extension only: `photo.JPG.bak` is not an asset. The fork's `files/add` passes the
/// normalized absolute path to `isAsset`, so a dotfile such as `.jpg` has no extension there
/// (`getFilenameExtension` only treats a bare relative `.jpg` argument as an extension).
pub fn is_immich_asset_name(name: &str) -> bool {
    let (_, Some(extension)) = split_ext(name) else {
        return false;
    };
    let extension = extension.to_ascii_lowercase();
    IMAGE.contains(&extension.as_str()) || VIDEO.contains(&extension.as_str())
}

#[cfg(test)]
mod tests {
    use super::is_immich_asset_name;

    #[test]
    fn only_the_last_extension_decides() {
        assert!(is_immich_asset_name("example-photo.JPG"));
        assert!(is_immich_asset_name("clip.MP4"));
        assert!(is_immich_asset_name("raw.arw"));
        assert!(!is_immich_asset_name("example-photo.JPG.myliobak"));
        assert!(!is_immich_asset_name("photo.jpg.xmp"));
        assert!(!is_immich_asset_name("notes.txt"));
        assert!(!is_immich_asset_name(".jpg"));
        assert!(is_immich_asset_name(".hidden.jpg"));
        assert!(is_immich_asset_name("..jpg"));
        assert!(!is_immich_asset_name("README"));
    }
}
