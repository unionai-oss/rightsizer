//! Filesystem detection (SPEC §8.1: sqlite is "refused on network
//! filesystems"; a local object-store directory on a network filesystem is
//! *shared*). Uses `statfs(2)`: the filesystem type name on macOS/BSD, the
//! magic number on Linux. [`classify`] is pure so it can be tested with
//! injected types.

use std::path::Path;

/// What kind of filesystem a path is on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FsKind {
    /// A local disk filesystem.
    Local,
    /// A network or FUSE filesystem (sqlite locking is unreliable there).
    Network(String),
    /// Could not tell (unsupported OS or `statfs` failed).
    Unknown,
}

impl FsKind {
    /// Network or FUSE.
    pub fn is_network(&self) -> bool {
        matches!(self, FsKind::Network(_))
    }
}

/// Network / FUSE filesystem type names (macOS, BSD; Linux `/proc/mounts` names).
const NETWORK_NAMES: &[&str] = &[
    "nfs", "nfs4", "smbfs", "smb", "smb2", "smb3", "cifs", "afpfs", "webdav", "macfuse", "osxfuse", "fusefs", "fuse", "fuseblk",
    "9p", "ceph", "glusterfs", "lustre", "gpfs", "afs", "coda", "sshfs", "s3fs", "gcsfuse", "beegfs", "ocfs2", "gfs2", "davfs",
];

/// Linux `statfs.f_type` magic numbers of network / FUSE / cluster filesystems.
const NETWORK_MAGIC: &[(i64, &str)] = &[
    (0x6969, "nfs"),
    (0x517B, "smb"),
    (0xFF53_4D42, "cifs"),
    (0xFE53_4D42, "smb2"),
    (0x6573_5546, "fuse"),
    (0x00C3_6400, "ceph"),
    (0x5346_414F, "afs"),
    (0x7375_7245, "coda"),
    (0x0102_1997, "9p"),
    (0x0116_1970, "gfs2"),
    (0x7461_636F, "ocfs2"),
    (0x0BD0_0BD0, "lustre"),
    (0x4750_4653, "gpfs"),
    (0x1916_1023, "beegfs"),
];

/// Classify a filesystem from its type name (macOS/BSD) and/or magic
/// number (Linux). FUSE counts as network: most FUSE mounts are remote and
/// none guarantee POSIX advisory locks.
pub fn classify(name: Option<&str>, magic: Option<i64>) -> FsKind {
    if let Some(n) = name {
        let lower = n.to_ascii_lowercase();
        let base = lower.split(['.', ':']).next().unwrap_or(&lower);
        if NETWORK_NAMES.contains(&base) || base.starts_with("fuse") || base.ends_with("fuse") {
            return FsKind::Network(n.to_string());
        }
    }
    if let Some(m) = magic {
        // f_type is a signed long on some ABIs: compare the low 32 bits.
        let m32 = m & 0xFFFF_FFFF;
        if let Some((_, n)) = NETWORK_MAGIC.iter().find(|(x, _)| *x == m32) {
            return FsKind::Network((*n).to_string());
        }
    }
    if name.is_some() || magic.is_some() { FsKind::Local } else { FsKind::Unknown }
}

/// The filesystem `path` is on (its nearest existing ancestor if it does not exist).
pub fn detect(path: &Path) -> FsKind {
    let mut p = path;
    loop {
        if p.exists() {
            break;
        }
        match p.parent() {
            Some(q) if !q.as_os_str().is_empty() => p = q,
            _ => {
                p = Path::new(".");
                break;
            }
        }
    }
    detect_existing(p)
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "openbsd"))]
fn detect_existing(p: &Path) -> FsKind {
    match nix::sys::statfs::statfs(p) {
        Ok(s) => classify(Some(s.filesystem_type_name()), None),
        Err(_) => FsKind::Unknown,
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn detect_existing(p: &Path) -> FsKind {
    match nix::sys::statfs::statfs(p) {
        #[allow(clippy::unnecessary_cast)]
        Ok(s) => classify(None, Some(s.filesystem_type().0 as i64)),
        Err(_) => FsKind::Unknown,
    }
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "linux",
    target_os = "android"
)))]
fn detect_existing(_: &Path) -> FsKind {
    FsKind::Unknown
}

/// Whether `path` is under the system temp directory (not durable).
pub fn is_temp(path: &Path) -> bool {
    let canon = |p: &Path| p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
    let p = canon(path);
    [std::env::temp_dir(), "/tmp".into(), "/dev/shm".into(), "/private/tmp".into(), "/private/var/folders".into()]
        .iter()
        .any(|t| p.starts_with(canon(t)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_names_and_magic() {
        for n in ["nfs", "smbfs", "macfuse", "osxfuse", "afpfs", "webdav", "fuse.sshfs", "fuseblk", "cifs", "NFS"] {
            assert!(classify(Some(n), None).is_network(), "{n}");
        }
        for n in ["apfs", "hfs", "ext4", "xfs", "btrfs", "tmpfs", "zfs"] {
            assert_eq!(classify(Some(n), None), FsKind::Local, "{n}");
        }
        assert_eq!(classify(None, Some(0x6969)), FsKind::Network("nfs".into()));
        assert_eq!(classify(None, Some(0xFF53_4D42)), FsKind::Network("cifs".into()));
        assert_eq!(classify(None, Some(0xFFFF_FFFF_FF53_4D42u64 as i64)), FsKind::Network("cifs".into()), "sign-extended f_type");
        assert_eq!(classify(None, Some(0x6573_5546)), FsKind::Network("fuse".into()));
        assert_eq!(classify(None, Some(0xEF53)), FsKind::Local, "ext4");
        assert_eq!(classify(None, None), FsKind::Unknown);
    }

    #[test]
    fn detect_local_tempdir() {
        let d = tempfile::tempdir().unwrap();
        let k = detect(&d.path().join("does/not/exist.db"));
        assert!(!k.is_network(), "{k:?}");
        assert!(is_temp(d.path()));
    }
}
