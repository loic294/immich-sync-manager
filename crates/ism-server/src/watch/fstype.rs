//! File system type detection, so `auto` mode polls network shares.

use std::path::Path;

/// The file system type name of `path` and whether it is a network file system. On targets
/// without `statfs` support the type is unknown and treated as local.
pub fn detect(path: &Path) -> (Option<String>, bool) {
    imp::detect(path)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::path::Path;

    const KNOWN: &[(u32, &str, bool)] = &[
        (0x6969, "nfs", true),
        (0x517B, "smb", true),
        (0xFF53_4D42, "cifs", true),
        (0xFE53_4D42, "smb2", true),
        (0x6573_5546, "fuse", true),
        (0x00C3_6400, "ceph", true),
        (0x5346_414F, "afs", true),
        (0x0102_1997, "9p", true),
        // Host folders shared into a VM (Docker Desktop): host-side changes arrive late,
        // incomplete (new folders lack the directory flag) or not at all.
        (0x6A65_6A63, "virtiofs", true),
        (0x564C, "ncp", true),
        (0xEF53, "ext4", false),
        (0x5846_5342, "xfs", false),
        (0x9123_683E, "btrfs", false),
        (0x2FC1_2FC1, "zfs", false),
        (0x794C_7630, "overlay", false),
        (0x0102_1994, "tmpfs", false),
    ];

    pub fn detect(path: &Path) -> (Option<String>, bool) {
        let Ok(st) = rustix::fs::statfs(path) else {
            return (None, false);
        };
        #[allow(clippy::unnecessary_cast)]
        let magic = st.f_type as u32;
        match KNOWN.iter().find(|(m, _, _)| *m == magic) {
            Some((_, name, network)) => (Some((*name).to_owned()), *network),
            None => (Some(format!("0x{magic:x}")), false),
        }
    }
}

#[cfg(target_vendor = "apple")]
mod imp {
    use std::path::Path;

    const NETWORK: &[&str] = &[
        "nfs", "smbfs", "afpfs", "webdav", "cifs", "macfuse", "osxfuse",
    ];

    pub fn detect(path: &Path) -> (Option<String>, bool) {
        let Ok(st) = rustix::fs::statfs(path) else {
            return (None, false);
        };
        let name: String = st
            .f_fstypename
            .iter()
            .take_while(|c| **c != 0)
            .map(|c| char::from(*c as u8))
            .collect();
        let network = NETWORK.contains(&name.as_str());
        (Some(name), network)
    }
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
mod imp {
    use std::path::Path;

    pub fn detect(_path: &Path) -> (Option<String>, bool) {
        (None, false)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_temporary_folder_is_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, network) = super::detect(dir.path());
        assert!(!network);
    }
}
