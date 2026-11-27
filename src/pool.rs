//! A pool: a directory on a node that holds volume directories.
//!
//! Formatting and mounting are the user's job. The agent only inspects what it was given: whether
//! the path is the root of a mounted filesystem (so that volumes never land on the root
//! filesystem when a disk failed to mount) and whether project quota is on.

use std::io;
use std::path::{Path, PathBuf};

/// What `/proc/self/mountinfo` says about the mount at a pool path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// Block device of the filesystem, the handle quota calls take.
    pub device: PathBuf,
    pub fs_type: String,
    /// The mount shows the root of its filesystem, not a subdirectory of some other mount.
    pub is_filesystem_root: bool,
    /// Project quota accounting and enforcement are on.
    pub project_quota: bool,
    /// Nothing is written through this mount, whatever the filesystem allows.
    pub read_only: bool,
}

/// Why a pool cannot hold volumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unusable {
    /// The path is not the root of a mounted filesystem.
    NotMountpoint,
    /// The filesystem is mounted without project quota.
    NoQuota,
}

/// Per-pool checks the user may switch off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Requirements {
    pub mountpoint: bool,
    pub quota: bool,
}

/// mountinfo escapes space, tab, newline and backslash in paths as `\ooo`.
fn unescape(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let octal =
            bytes.get(i + 1..i + 4).filter(|d| bytes[i] == b'\\' && d.iter().all(|c| (b'0'..=b'7').contains(c)));
        match octal {
            Some(d) => {
                out.push((d[0] - b'0') << 6 | (d[1] - b'0') << 3 | (d[2] - b'0'));
                i += 4;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Finds the mount whose mount point is exactly `path`. Of several mounts stacked on one path
/// the last one is the visible one.
pub fn find_mount(mountinfo: &str, path: &Path) -> Option<Mount> {
    mountinfo.lines().rev().find_map(|line| {
        // <id> <parent> <major:minor> <root> <mount point> <options> [optional...] - <type> <source> <super options>
        let (before, after) = line.split_once(" - ")?;
        let mut before = before.split(' ');
        let root = before.nth(3)?;
        let mount_point = before.next()?;
        if Path::new(&unescape(mount_point)) != path {
            return None;
        }
        let options = before.next().unwrap_or("");
        let mut after = after.split(' ');
        let fs_type = after.next()?;
        let source = after.next()?;
        let super_options = after.next().unwrap_or("");
        Some(Mount {
            device: PathBuf::from(unescape(source)),
            fs_type: fs_type.to_string(),
            is_filesystem_root: root == "/",
            project_quota: super_options.split(',').any(|o| o == "prjquota" || o == "pquota"),
            read_only: options.split(',').any(|o| o == "ro"),
        })
    })
}

/// Where `directory` is mounted besides the place it lives in: the mount points of the bind
/// mounts that show it or something below it. A volume that is mounted somewhere is in use.
pub fn binds_of(mountinfo: &str, directory: &Path) -> Vec<PathBuf> {
    struct Entry {
        device: String,
        root: PathBuf,
        mount_point: PathBuf,
    }
    let entries: Vec<Entry> = mountinfo
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_once(" - ")?.0.split(' ');
            let device = fields.nth(2)?.to_string();
            let root = PathBuf::from(unescape(fields.next()?));
            let mount_point = PathBuf::from(unescape(fields.next()?));
            Some(Entry { device, root, mount_point })
        })
        .collect();
    // The mount the directory is seen through: the deepest one above it, the last of equals.
    let through = entries
        .iter()
        .filter(|e| directory.starts_with(&e.mount_point))
        .max_by_key(|e| e.mount_point.components().count());
    let Some(through) = through else { return Vec::new() };
    let below = directory.strip_prefix(&through.mount_point).unwrap_or(Path::new(""));
    // What the directory is called inside its filesystem: a bind mount shows it as its root.
    let inside = if below.as_os_str().is_empty() { through.root.clone() } else { through.root.join(below) };
    entries
        .iter()
        .filter(|e| e.device == through.device && e.root.starts_with(&inside))
        .filter(|e| !directory.starts_with(&e.mount_point))
        .map(|e| e.mount_point.clone())
        .collect()
}

/// Decides whether the pool at `path` may hold volumes.
pub fn check(mountinfo: &str, path: &Path, requirements: Requirements) -> Result<Option<Mount>, Unusable> {
    let mount = find_mount(mountinfo, path);
    if requirements.mountpoint && !mount.as_ref().is_some_and(|m| m.is_filesystem_root) {
        return Err(Unusable::NotMountpoint);
    }
    if requirements.quota && !mount.as_ref().is_some_and(|m| m.project_quota) {
        return Err(Unusable::NoQuota);
    }
    Ok(mount)
}

/// Size and usage of the filesystem as seen from `path`. Inside a directory with a project quota
/// the kernel reports the quota limit as the size, so a pod sees the size of its volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
    pub total_inodes: u64,
    pub free_inodes: u64,
    pub used_inodes: u64,
}

pub fn stats(path: &Path) -> io::Result<Stats> {
    let fs = rustix::fs::statfs(path)?;
    let block = fs.f_bsize as u64;
    let (blocks, free, available) = (fs.f_blocks as u64, fs.f_bfree as u64, fs.f_bavail as u64);
    let (inodes, inodes_free) = (fs.f_files as u64, fs.f_ffree as u64);
    Ok(Stats {
        total_bytes: blocks * block,
        available_bytes: available * block,
        used_bytes: blocks.saturating_sub(free) * block,
        total_inodes: inodes,
        free_inodes: inodes_free,
        used_inodes: inodes.saturating_sub(inodes_free),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTINFO: &str = "\
22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/mapper/main-root rw
901 22 253:3 / /mnt/pool rw,relatime shared:40 - xfs /dev/mapper/nvme-pool rw,attr2,inode64,logbufs=8,logbsize=32k,prjquota
902 22 253:4 / /mnt/plain rw,relatime - xfs /dev/mapper/nvme-plain rw,attr2,inode64,noquota
903 22 259:2 /opt/volumes /mnt/on-root rw,relatime - ext4 /dev/mapper/main-root rw
904 22 253:5 / /mnt/my\\040disk rw - xfs /dev/mapper/nvme-my\\040disk rw,pquota
905 22 253:6 / /mnt/stacked rw - xfs /dev/old rw,noquota
906 905 253:7 / /mnt/stacked rw - xfs /dev/new rw,prjquota";

    const ALL: Requirements = Requirements { mountpoint: true, quota: true };

    #[test]
    fn pool_on_its_own_filesystem_with_project_quota_is_usable() {
        let mount = check(MOUNTINFO, Path::new("/mnt/pool"), ALL).unwrap().unwrap();
        assert_eq!(mount.device, Path::new("/dev/mapper/nvme-pool"));
        assert_eq!(mount.fs_type, "xfs");
    }

    #[test]
    fn pool_without_project_quota_is_refused_unless_quota_is_switched_off() {
        assert_eq!(check(MOUNTINFO, Path::new("/mnt/plain"), ALL), Err(Unusable::NoQuota));
        let relaxed = Requirements { mountpoint: true, quota: false };
        assert!(check(MOUNTINFO, Path::new("/mnt/plain"), relaxed).is_ok());
    }

    #[test]
    fn directory_of_another_filesystem_is_not_a_mountpoint() {
        // A bind mount of a subdirectory of the root filesystem, and a path that is no mount at all.
        assert_eq!(check(MOUNTINFO, Path::new("/mnt/on-root"), ALL), Err(Unusable::NotMountpoint));
        assert_eq!(check(MOUNTINFO, Path::new("/mnt/missing"), ALL), Err(Unusable::NotMountpoint));
    }

    #[test]
    fn plain_directory_is_accepted_when_both_checks_are_switched_off() {
        let none = Requirements { mountpoint: false, quota: false };
        assert_eq!(check(MOUNTINFO, Path::new("/mnt/missing"), none), Ok(None));
    }

    #[test]
    fn escaped_paths_are_decoded() {
        let mount = find_mount(MOUNTINFO, Path::new("/mnt/my disk")).unwrap();
        assert_eq!(mount.device, Path::new("/dev/mapper/nvme-my disk"));
        assert!(mount.project_quota);
    }

    #[test]
    fn bind_mounts_of_a_directory_are_found_by_what_they_show() {
        let mountinfo = "\
22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/mapper/main-root rw
901 22 253:3 / /mnt/pool rw,relatime shared:40 - xfs /dev/mapper/nvme-pool rw,prjquota
950 22 253:3 /volumes/pv-1/data /var/lib/kubelet/pods/uid-1/volumes/kubernetes.io~csi/pv-1/mount rw - xfs /dev/mapper/nvme-pool rw,prjquota
951 22 253:3 /volumes/pv-1/data/sub /var/lib/kubelet/pods/uid-1/volume-subpaths/pv-1/app/0 rw - xfs /dev/mapper/nvme-pool rw,prjquota
952 22 253:3 /volumes/pv-10/data /var/lib/kubelet/pods/uid-2/volumes/kubernetes.io~csi/pv-10/mount rw - xfs /dev/mapper/nvme-pool rw,prjquota
960 22 259:2 /var/lib/plain /var/lib/plain rw - ext4 /dev/mapper/main-root rw
961 22 259:2 /var/lib/plain/volumes/pv-2/data /var/lib/kubelet/pods/uid-3/volumes/kubernetes.io~csi/pv-2/mount rw - ext4 /dev/mapper/main-root rw";
        let binds = |directory: &str| binds_of(mountinfo, Path::new(directory));
        assert_eq!(
            binds("/mnt/pool/volumes/pv-1/data"),
            [
                Path::new("/var/lib/kubelet/pods/uid-1/volumes/kubernetes.io~csi/pv-1/mount"),
                Path::new("/var/lib/kubelet/pods/uid-1/volume-subpaths/pv-1/app/0"),
            ]
        );
        // A pool that is a plain directory of the root filesystem, seen through its own mount.
        assert_eq!(
            binds("/var/lib/plain/volumes/pv-2/data"),
            [Path::new("/var/lib/kubelet/pods/uid-3/volumes/kubernetes.io~csi/pv-2/mount")]
        );
        assert_eq!(binds("/mnt/pool/volumes/pv-3/data"), Vec::<PathBuf>::new());
        assert_eq!(binds("/nowhere/at/all"), Vec::<PathBuf>::new());
    }

    #[test]
    fn read_only_is_what_the_mount_says_not_what_the_filesystem_does() {
        let mountinfo = "\
901 22 253:3 / /mnt/pool rw,relatime shared:40 - xfs /dev/mapper/nvme-pool rw,prjquota
950 22 253:3 /volumes/pv-1/data /pods/uid-1/mount ro,relatime shared:41 - xfs /dev/mapper/nvme-pool rw,prjquota
951 22 253:3 /volumes/pv-2/data /pods/uid-2/mount rw,relatime - xfs /dev/mapper/nvme-pool rw,prjquota
952 22 253:9 / /mnt/sealed rw,relatime - xfs /dev/mapper/nvme-sealed ro";
        let read_only = |path: &str| find_mount(mountinfo, Path::new(path)).unwrap().read_only;
        assert!(read_only("/pods/uid-1/mount"));
        assert!(!read_only("/pods/uid-2/mount"));
        assert!(!read_only("/mnt/pool"));
        assert!(!read_only("/mnt/sealed"), "the options of the filesystem are not the ones of the mount");
    }

    #[test]
    fn the_last_mount_stacked_on_a_path_wins() {
        let mount = find_mount(MOUNTINFO, Path::new("/mnt/stacked")).unwrap();
        assert_eq!(mount.device, Path::new("/dev/new"));
    }
}
