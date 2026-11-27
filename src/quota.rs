//! Project quota of a volume directory.
//!
//! A volume directory gets a project id of its own and the inherit flag, so everything created
//! below it is charged to that project. The kernel then enforces the limit (for root as well on
//! XFS) and keeps the usage counters: reading them is one syscall, no tree walk.
//!
//! Limits go through the generic quota commands, which XFS and ext4 both implement, and through
//! `quotactl_fd` (Linux 5.14): it addresses the filesystem by an open directory, so the block
//! device does not have to be visible in the container of the agent.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

// The numbers of the ioctls below are written out as the architectures the driver is built for
// encode them; some others put the direction of an ioctl into other bits.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
compile_error!("the ioctl numbers of the quota module are those of x86_64 and aarch64");

const FS_IOC_FSGETXATTR: u32 = 0x801C_581F;
const FS_IOC_FSSETXATTR: u32 = 0x401C_5820;
const FS_XFLAG_PROJINHERIT: u32 = 0x200;

const PRJQUOTA: u32 = 2;
const Q_GETQUOTA: u32 = 0x80_0007;
const Q_SETQUOTA: u32 = 0x80_0008;
const QIF_BLIMITS: u32 = 1;
const QIF_ILIMITS: u32 = 4;
/// Block limits of the generic quota interface are counted in units of this size.
const QUOTA_BLOCK: u64 = 1024;

/// `struct fsxattr` of linux/fs.h.
#[repr(C)]
#[derive(Default)]
struct FsXAttr {
    xflags: u32,
    extsize: u32,
    nextents: u32,
    projid: u32,
    cowextsize: u32,
    pad: [u8; 8],
}

/// `struct if_dqblk` of linux/quota.h.
#[repr(C)]
#[derive(Default)]
struct DqBlk {
    bhardlimit: u64,
    bsoftlimit: u64,
    curspace: u64,
    ihardlimit: u64,
    isoftlimit: u64,
    curinodes: u64,
    btime: u64,
    itime: u64,
    valid: u32,
}

/// Project of a file or directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Project {
    pub id: u32,
    /// New entries of the directory get the same project.
    pub inherit: bool,
}

/// Hard limits of a project; zero means unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Limits {
    pub bytes: u64,
    pub inodes: u64,
}

impl Limits {
    /// The limits as the quota keeps them: the bytes rounded up to the quota block.
    pub fn as_kept(self) -> Self {
        Self { bytes: self.bytes.div_ceil(QUOTA_BLOCK).saturating_mul(QUOTA_BLOCK), ..self }
    }
}

/// Limits and current usage of a project; a zero limit means unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    pub limit_bytes: u64,
    pub used_bytes: u64,
    pub limit_inodes: u64,
    pub used_inodes: u64,
}

fn get_attr(file: &File) -> io::Result<FsXAttr> {
    let mut attr = FsXAttr::default();
    // SAFETY: the ioctl fills a `struct fsxattr`, and `attr` is one that lives across the call.
    let rc = unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSGETXATTR as _, &mut attr as *mut FsXAttr) };
    if rc == 0 { Ok(attr) } else { Err(io::Error::last_os_error()) }
}

/// Reads the project of `path`.
pub fn project(path: &Path) -> io::Result<Project> {
    let attr = get_attr(&File::open(path)?)?;
    Ok(Project { id: attr.projid, inherit: attr.xflags & FS_XFLAG_PROJINHERIT != 0 })
}

/// Puts `path` into project `id`. A directory also gets the inherit flag; a file does not:
/// ext4 rejects the flag on anything but a directory.
pub fn set_project(path: &Path, id: u32) -> io::Result<()> {
    let file = File::open(path)?;
    let mut attr = get_attr(&file)?;
    attr.projid = id;
    if file.metadata()?.is_dir() {
        attr.xflags |= FS_XFLAG_PROJINHERIT;
    }
    // SAFETY: the ioctl reads a `struct fsxattr`, and `attr` is one that lives across the call.
    let rc = unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSSETXATTR as _, &attr as *const FsXAttr) };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

fn quotactl(command: u32, filesystem: &Path, project: u32, block: &mut DqBlk) -> io::Result<()> {
    let directory = File::open(filesystem)?;
    let command = (command << 8) | PRJQUOTA;
    // SAFETY: quotactl_fd takes a descriptor, two integers and a pointer to a `struct if_dqblk`;
    // `directory` and `block` outlive the call.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_quotactl_fd,
            directory.as_raw_fd(),
            command as libc::c_uint,
            project as libc::c_uint,
            block as *mut DqBlk,
        )
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

/// Sets the hard limits of `project` on the filesystem that holds `filesystem`. The byte limit is
/// rounded up to the quota block.
pub fn set_limits(filesystem: &Path, project: u32, limits: Limits) -> io::Result<()> {
    let mut block = DqBlk {
        bhardlimit: limits.bytes.div_ceil(QUOTA_BLOCK),
        ihardlimit: limits.inodes,
        valid: QIF_BLIMITS | QIF_ILIMITS,
        ..Default::default()
    };
    quotactl(Q_SETQUOTA, filesystem, project, &mut block)
}

/// Reads the limits and the usage of `project` on the filesystem that holds `filesystem`.
pub fn usage(filesystem: &Path, project: u32) -> io::Result<Usage> {
    let mut block = DqBlk::default();
    match quotactl(Q_GETQUOTA, filesystem, project, &mut block) {
        Ok(()) => {}
        // XFS keeps no record of a project that has neither usage nor limits.
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => return Ok(Usage::default()),
        Err(e) => return Err(e),
    }
    Ok(Usage {
        limit_bytes: block.bhardlimit * QUOTA_BLOCK,
        used_bytes: block.curspace,
        limit_inodes: block.ihardlimit,
        used_inodes: block.curinodes,
    })
}

/// Reads the hard limits of `project` on the filesystem that holds `filesystem`.
pub fn limits(filesystem: &Path, project: u32) -> io::Result<Limits> {
    let usage = usage(filesystem, project)?;
    Ok(Limits { bytes: usage.limit_bytes, inodes: usage.limit_inodes })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_is_kept_in_quota_blocks() {
        assert_eq!(Limits { bytes: 1, inodes: 7 }.as_kept(), Limits { bytes: 1024, inodes: 7 });
        assert_eq!(Limits { bytes: 1 << 30, inodes: 0 }.as_kept(), Limits { bytes: 1 << 30, inodes: 0 });
        assert_eq!(Limits::default().as_kept(), Limits::default());
    }

    #[test]
    fn kernel_structures_have_the_kernel_layout() {
        assert_eq!(std::mem::size_of::<FsXAttr>(), 28);
        assert_eq!(std::mem::size_of::<DqBlk>(), 72);
        assert_eq!(std::mem::offset_of!(FsXAttr, projid), 12);
        assert_eq!(std::mem::offset_of!(DqBlk, valid), 64);
    }
}
