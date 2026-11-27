//! What a request may ask for: the capabilities of a volume, its size, its node and the parameters
//! of its StorageClass.

use std::collections::HashMap;

use tonic::Status;

use crate::api::{AccessMode, Claim, LocalPathVolume, LocalPathVolumeSpec, Mobility};
use crate::csi::volume_capability::AccessType;
use crate::csi::volume_capability::access_mode::Mode;
use crate::csi::{CapacityRange, Topology, TopologyRequirement, Volume, VolumeCapability};
use crate::{parse_duration, topology_node, topology_storage};

/// StorageClass parameters.
pub(super) const PARAM_POOL: &str = "pool";
const PARAM_MOBILITY: &str = "mobility";
const PARAM_DIR_MODE: &str = "dirMode";
const PARAM_INODES_PER_GIB: &str = "inodesPerGiB";
const PARAM_DELETE_GRACE_PERIOD: &str = "deleteGracePeriod";
/// Parameters the provisioner adds on its own, among them the claim the volume is created for.
const PROVISIONER_PREFIX: &str = "csi.storage.k8s.io/";
pub(super) const PARAM_CLAIM_NAME: &str = "csi.storage.k8s.io/pvc/name";
pub(super) const PARAM_CLAIM_NAMESPACE: &str = "csi.storage.k8s.io/pvc/namespace";

const DEFAULT_DIR_MODE: &str = "0770";

/// Key of the volume context that tells the agent which pool the volume is in.
pub const CONTEXT_POOL: &str = "pool";

/// A volume is a directory: it cannot be handed to a pod as a block device. A capability that
/// does not say how the volume is used is taken for a mount.
pub(crate) fn check_access_type(capability: &VolumeCapability) -> Result<(), Status> {
    match capability.access_type {
        Some(AccessType::Block(_)) => Err(Status::invalid_argument(
            "volumeMode Block is not supported: a volume is a directory, use volumeMode Filesystem",
        )),
        Some(AccessType::Mount(_)) | None => Ok(()),
    }
}

/// What a new volume may be asked to do: to be a directory, bind-mounted as it is, on one node.
pub(super) fn check_capabilities(capabilities: &[VolumeCapability]) -> Result<(), Status> {
    if capabilities.is_empty() {
        return Err(Status::invalid_argument("volume capabilities are required"));
    }
    for capability in capabilities {
        check_access_type(capability)?;
        // The options of a mount are those of the filesystem of the pool, which the driver does
        // not mount: taking them and applying none would be a silent lie.
        if let Some(AccessType::Mount(mount)) = &capability.access_type
            && !mount.mount_flags.is_empty()
        {
            return Err(Status::invalid_argument(format!(
                "mount options {} are not supported: a volume is a directory of a filesystem that is mounted already, remove mountOptions from the StorageClass",
                mount.mount_flags.join(",")
            )));
        }
        // A directory on one node cannot be mounted on several nodes at once.
        let mode = capability.access_mode.as_ref().map(|m| m.mode()).unwrap_or(Mode::Unknown);
        match mode {
            Mode::SingleNodeWriter | Mode::SingleNodeSingleWriter | Mode::SingleNodeMultiWriter => {}
            other => {
                return Err(Status::invalid_argument(format!(
                    "access mode {} is not supported: a volume lives on one node, use ReadWriteOnce or ReadWriteOncePod",
                    other.as_str_name()
                )));
            }
        }
    }
    Ok(())
}

/// A claim for one pod only is recorded as such: a volume brought back from its directory gets
/// a PersistentVolume with the same access mode.
pub(super) fn access_mode(capabilities: &[VolumeCapability]) -> AccessMode {
    let single_pod =
        |c: &VolumeCapability| c.access_mode.as_ref().is_some_and(|m| m.mode() == Mode::SingleNodeSingleWriter);
    if capabilities.iter().any(single_pod) { AccessMode::ReadWriteOncePod } else { AccessMode::ReadWriteOnce }
}

/// The node the scheduler selected for the first consumer comes first in `preferred`.
pub(super) fn selected_node(requirement: Option<&TopologyRequirement>) -> Result<String, Status> {
    let key = topology_node();
    requirement
        .into_iter()
        .flat_map(|r| r.preferred.iter().chain(r.requisite.iter()))
        .find_map(|t| t.segments.get(key).cloned())
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "no {key} in accessibility requirements: the StorageClass must use WaitForFirstConsumer"
            ))
        })
}

pub(super) fn requested_size(range: Option<&CapacityRange>) -> Result<u64, Status> {
    let (required, limit) = range.map(|r| (r.required_bytes, r.limit_bytes)).unwrap_or((0, 0));
    if required < 0 || limit < 0 || (limit > 0 && required > limit) {
        return Err(Status::invalid_argument("capacity range is inconsistent"));
    }
    match if required > 0 { required } else { limit } {
        0 => Err(Status::invalid_argument("a volume size is required")),
        size => Ok(size as u64),
    }
}

pub(super) fn parse_spec(
    parameters: &HashMap<String, String>,
    node: String,
    size_bytes: u64,
    access_mode: AccessMode,
) -> Result<LocalPathVolumeSpec, Status> {
    let invalid = |name: &str, problem: String| Status::invalid_argument(format!("parameter {name}: {problem}"));
    for name in parameters.keys().filter(|n| !n.starts_with(PROVISIONER_PREFIX)) {
        if ![PARAM_POOL, PARAM_MOBILITY, PARAM_DIR_MODE, PARAM_INODES_PER_GIB, PARAM_DELETE_GRACE_PERIOD]
            .contains(&name.as_str())
        {
            return Err(invalid(name, "unknown parameter".into()));
        }
    }
    let pool = parameters.get(PARAM_POOL).filter(|p| !p.is_empty()).cloned();
    let pool = pool.ok_or_else(|| invalid(PARAM_POOL, "the pool of the StorageClass is required".into()))?;
    let mobility = match parameters.get(PARAM_MOBILITY).map(String::as_str) {
        None | Some("Pinned") => Mobility::Pinned,
        Some("Floating") => Mobility::Floating,
        Some(other) => {
            return Err(invalid(PARAM_MOBILITY, format!("unknown value {other:?}, expected Pinned or Floating")));
        }
    };
    let inodes_per_gib = match parameters.get(PARAM_INODES_PER_GIB) {
        None => 0,
        Some(value) => {
            value.parse().map_err(|_| invalid(PARAM_INODES_PER_GIB, format!("{value:?} is not a number")))?
        }
    };
    let delete_grace_seconds = match parameters.get(PARAM_DELETE_GRACE_PERIOD) {
        None => 0,
        Some(value) => parse_duration(value).ok_or_else(|| {
            invalid(PARAM_DELETE_GRACE_PERIOD, format!("{value:?} is not a duration like 0, 30m, 24h or 7d"))
        })?,
    };
    let claim = match (parameters.get(PARAM_CLAIM_NAMESPACE), parameters.get(PARAM_CLAIM_NAME)) {
        (Some(namespace), Some(name)) => Some(Claim { namespace: namespace.clone(), name: name.clone() }),
        _ => None,
    };
    let dir_mode = parameters.get(PARAM_DIR_MODE).cloned().unwrap_or_else(|| DEFAULT_DIR_MODE.to_string());
    let spec = LocalPathVolumeSpec {
        pool,
        size_bytes,
        mobility,
        node,
        inodes_per_gib,
        dir_mode,
        delete_grace_seconds,
        access_mode,
        storage_class: None,
        claim,
    };
    if spec.mode().is_none() {
        return Err(invalid(PARAM_DIR_MODE, format!("{:?} is not an octal mode like 0770", spec.dir_mode)));
    }
    Ok(spec)
}

pub(super) fn csi_volume(name: &str, volume: &LocalPathVolume) -> Volume {
    let segments = match volume.spec.mobility {
        Mobility::Pinned => HashMap::from([(topology_node().to_string(), volume.data_node().to_string())]),
        Mobility::Floating => HashMap::from([(topology_storage().to_string(), "true".to_string())]),
    };
    Volume {
        volume_id: name.to_string(),
        capacity_bytes: volume.spec.size_bytes as i64,
        volume_context: HashMap::from([(CONTEXT_POOL.to_string(), volume.spec.pool.clone())]),
        accessible_topology: vec![Topology { segments }],
        ..Default::default()
    }
}
