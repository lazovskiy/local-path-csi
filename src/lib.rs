//! local-path-csi: a CSI driver for node-local directory volumes.

// What the kernel has no safe call for is in `quota`; nothing else talks to it in its own terms.
#![deny(unsafe_code)]

pub mod agent;
pub mod api;
pub mod compression;
pub mod config;
pub mod controller;
pub mod events;
pub mod http;
pub mod identity;
pub mod init;
pub mod leader;
pub mod metrics;
pub mod mount;
pub mod moving;
pub mod node;
pub mod pool;
#[allow(unsafe_code)]
pub mod quota;
pub mod restore;
pub mod server;
pub mod sightings;
pub mod store;
pub mod tls;
pub mod transfer;
pub mod volumes;
pub mod webhook;

/// CSI gRPC bindings generated from `proto/csi.proto`.
#[allow(clippy::all, clippy::pedantic, rustdoc::all)]
pub mod csi {
    tonic::include_proto!("csi.v1");
}

/// API group of `LocalPathVolume` and `LocalPathNode` when `--api-group` is not given.
pub const DEFAULT_API_GROUP: &str = "local-path-csi.domik.fi";

/// Name the driver reports to the kubelet and writes into every PersistentVolume when
/// `--driver-name` is not given. A cluster keeps the name it was installed with: a PersistentVolume
/// names its driver, and nothing rewrites those.
pub const DEFAULT_DRIVER_NAME: &str = "local-path.csi.domik.fi";

/// Why the process cannot go by the group and the driver name it was given.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("{what} {name:?} is not a DNS name of at most {max} characters")]
    NotDns { what: &'static str, name: String, max: usize },
    #[error("the api group and the driver name are already set")]
    AlreadySet,
}

/// The group, the driver name and the strings built from them. Set once, before anything asks.
struct Names {
    api_group: String,
    driver_name: String,
    topology_node: String,
    topology_storage: String,
    finalizer: String,
    restore_label: String,
    restore_approval: String,
}

impl Names {
    /// `api_group` and `driver_name` are DNS names; the driver name is at most 63 characters
    /// because it is the name of the CSIDriver object.
    fn build(api_group: &str, driver_name: &str) -> Result<Self, NameError> {
        check_dns("api group", api_group, 253)?;
        check_dns("driver name", driver_name, 63)?;
        Ok(Self {
            api_group: api_group.to_string(),
            driver_name: driver_name.to_string(),
            topology_node: format!("{driver_name}/node"),
            topology_storage: format!("{driver_name}/storage"),
            finalizer: format!("{api_group}/data"),
            restore_label: format!("{api_group}/restore-pv"),
            restore_approval: format!("{api_group}/restore-approved"),
        })
    }
}

static NAMES: std::sync::OnceLock<Names> = std::sync::OnceLock::new();

fn names() -> &'static Names {
    NAMES.get_or_init(|| {
        Names::build(DEFAULT_API_GROUP, DEFAULT_DRIVER_NAME).expect("the compiled defaults are valid DNS names")
    })
}

/// Remembers the group and the driver name for this process. A second call is an error, and so is
/// a call made after something has already read the defaults.
pub fn configure(api_group: &str, driver_name: &str) -> Result<(), NameError> {
    let names = Names::build(api_group, driver_name)?;
    NAMES.set(names).map_err(|_| NameError::AlreadySet)
}

fn check_dns(what: &'static str, name: &str, max: usize) -> Result<(), NameError> {
    let labels_ok = !name.is_empty()
        && name.split('.').all(|label| {
            let bytes = label.as_bytes();
            !bytes.is_empty()
                && bytes.len() <= 63
                && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
                && bytes[0].is_ascii_alphanumeric()
                && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        });
    if name.len() <= max && labels_ok { Ok(()) } else { Err(NameError::NotDns { what, name: name.to_string(), max }) }
}

/// API group the process talks to.
pub fn api_group() -> &'static str {
    &names().api_group
}

/// Name the driver reports and writes into every PersistentVolume.
pub fn driver_name() -> &'static str {
    &names().driver_name
}

/// Topology key carrying the node name: pins a `Pinned` volume to its node.
pub fn topology_node() -> &'static str {
    &names().topology_node
}

/// Topology key every node with an agent carries: a `Floating` volume is reachable from all of them.
pub fn topology_storage() -> &'static str {
    &names().topology_storage
}

/// Finalizer of a LocalPathVolume: removed by the agent once the data is gone from the node.
pub fn finalizer() -> &'static str {
    &names().finalizer
}

/// Label of a LocalPathVolume an agent recreated from a directory that had lost its object: no
/// pod is given such a volume. The controller removes the label once an administrator has
/// approved the volume.
pub fn restore_label() -> &'static str {
    &names().restore_label
}

/// Annotation of such a LocalPathVolume, with the value `true`: an administrator agrees that the
/// directory is the volume it says it is. The volume is given to pods again, and one that has
/// lost its PersistentVolume gets it back, reserved for the claim its directory names. An agent
/// is not let to write it.
pub fn restore_approval() -> &'static str {
    &names().restore_approval
}

/// Runs `work` off the threads that serve requests and drive the tasks, and returns what it
/// returns. Anything that touches a disk goes through here: a disk that hangs, or an unmount
/// that takes its time, would otherwise stop the thread it is called on, and with enough of
/// them the gRPC server and everything else in the process.
pub async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        // Nothing cancels the task: it ends without a result only when `work` panicked.
        Err(failed) => std::panic::resume_unwind(failed.into_panic()),
    }
}

/// Seconds since the Unix epoch.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Parses `0`, `90s`, `30m`, `24h` or `7d` into seconds.
pub fn parse_duration(text: &str) -> Option<u64> {
    if text == "0" {
        return Some(0);
    }
    let unit = match text.chars().last()? {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        _ => return None,
    };
    let number = &text[..text.len() - 1];
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    number.parse::<u64>().ok()?.checked_mul(unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn work_on_a_disk_is_done_off_the_thread_that_serves() {
        // The runtime of a test has one thread: whatever ran on it would stop everything else.
        let serving = std::thread::current().id();
        let working = blocking(|| std::thread::current().id()).await;
        assert_ne!(working, serving);
    }

    #[test]
    fn approval_of_a_restore_is_named_after_the_group() {
        let names = Names::build("example.com", "path.csi.example.com").unwrap();
        assert_eq!(names.restore_approval, "example.com/restore-approved");
    }

    #[test]
    fn names_follow_the_group_and_the_driver() {
        let names = Names::build("example.com", "path.csi.example.com").unwrap();
        assert_eq!(names.finalizer, "example.com/data");
        assert_eq!(names.restore_label, "example.com/restore-pv");
        assert_eq!(names.topology_node, "path.csi.example.com/node");
        assert_eq!(names.topology_storage, "path.csi.example.com/storage");
        assert!(Names::build("", DEFAULT_DRIVER_NAME).is_err());
        assert!(Names::build("Example.com", DEFAULT_DRIVER_NAME).is_err());
        assert!(Names::build(DEFAULT_API_GROUP, &"a".repeat(64)).is_err());
        assert!(Names::build(DEFAULT_API_GROUP, "path.csi.example.com").is_ok());
    }
}
