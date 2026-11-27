//! Bind mounts of volume directories into the paths the kubelet gives to pods.

use std::io;
use std::path::Path;

use rustix::io::Errno;
use rustix::mount::{MountFlags, UnmountFlags};

/// Bind-mounts directory `source` onto directory `target`. The mount is there as it was asked
/// for, or it is not there.
pub fn bind(source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
    bind_in_steps(
        read_only,
        || Ok(rustix::mount::mount_bind(source, target)?),
        || set_read_only(target, true),
        || unmount(target),
    )
}

/// The kernel ignores MS_RDONLY on the bind itself, so a read-only mount takes two calls, and
/// the second may fail. The mount of the first is then writable: left in place, it would be
/// taken for the one that was asked for by the next call that finds the path mounted.
fn bind_in_steps(
    read_only: bool,
    bind: impl FnOnce() -> io::Result<()>,
    protect: impl FnOnce() -> io::Result<()>,
    undo: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    bind()?;
    if !read_only {
        return Ok(());
    }
    let Err(error) = protect() else { return Ok(()) };
    match undo() {
        Ok(()) => Err(error),
        Err(left) => Err(io::Error::other(format!("{error}; the writable mount could not be removed either: {left}"))),
    }
}

/// Makes the bind mount at `target` read-only, or writable again.
pub fn set_read_only(target: &Path, read_only: bool) -> io::Result<()> {
    let protection = if read_only { MountFlags::RDONLY } else { MountFlags::empty() };
    // A remount of a bind mount takes no options of the filesystem.
    Ok(rustix::mount::mount_remount(target, MountFlags::BIND | protection, "")?)
}

/// Unmounts `target`. A path that is not mounted, or does not exist, is already unmounted.
pub fn unmount(target: &Path) -> io::Result<()> {
    match rustix::mount::unmount(target, UnmountFlags::empty()) {
        Ok(()) | Err(Errno::INVAL | Errno::NOENT) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    /// Runs the steps of a bind with a second step that ends as `protected` says, and returns
    /// the result with the steps that were taken.
    fn steps(
        read_only: bool,
        protected: io::Result<()>,
        undone: io::Result<()>,
    ) -> (io::Result<()>, Vec<&'static str>) {
        let taken = RefCell::new(Vec::new());
        let step = |name: &'static str, result: io::Result<()>| {
            let taken = &taken;
            move || {
                taken.borrow_mut().push(name);
                result
            }
        };
        let result = bind_in_steps(read_only, step("bind", Ok(())), step("protect", protected), step("undo", undone));
        (result, taken.into_inner())
    }

    fn refused() -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::EPERM))
    }

    #[test]
    fn mount_that_could_not_be_made_read_only_is_not_left_writable() {
        let (result, taken) = steps(true, refused(), Ok(()));
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EPERM), "the error is the one of the remount");
        assert_eq!(taken, ["bind", "protect", "undo"]);

        // Nothing more can be done when the mount does not go away either: both reasons are told.
        let (result, taken) = steps(true, refused(), Err(io::Error::from_raw_os_error(libc::EBUSY)));
        let error = result.unwrap_err().to_string();
        assert!(error.contains("could not be removed"), "{error}");
        assert_eq!(taken, ["bind", "protect", "undo"]);
    }

    #[test]
    fn mount_that_is_as_it_was_asked_for_is_left_in_place() {
        let (result, taken) = steps(true, Ok(()), Ok(()));
        assert!(result.is_ok());
        assert_eq!(taken, ["bind", "protect"]);
        let (result, taken) = steps(false, refused(), Ok(()));
        assert!(result.is_ok());
        assert_eq!(taken, ["bind"], "a writable mount takes one step");
    }
}
