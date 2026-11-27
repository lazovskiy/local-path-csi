//! Volumes an agent recreated from their directories wait here for an administrator: see
//! [`step`]. Approved, a volume that has lost its PersistentVolume gets it back.
//!
//! That PersistentVolume keeps its data when released (`Retain`) and is reserved for the claim
//! the volume was created for: only a claim of that name and namespace can bind to it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    CSIPersistentVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, ObjectReference,
    PersistentVolume, PersistentVolumeSpec, VolumeNodeAffinity,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use serde_json::json;
use tokio::sync::watch;

use crate::api::{AccessMode, LocalPathVolume, Mobility, Phase};
use crate::controller::CONTEXT_POOL;
use crate::events::{Events, Notice};
use crate::{driver_name, restore_approval, restore_label, topology_node, topology_storage};

const INTERVAL: Duration = Duration::from_secs(10);

/// The PersistentVolume of a volume that was brought back from its directory.
pub fn persistent_volume(volume: &LocalPathVolume) -> Option<PersistentVolume> {
    let name = volume.metadata.name.clone()?;
    let spec = &volume.spec;
    let (key, value) = match spec.mobility {
        Mobility::Pinned => (topology_node(), volume.data_node().to_string()),
        Mobility::Floating => (topology_storage(), "true".to_string()),
    };
    let node = NodeSelectorRequirement { key: key.into(), operator: "In".into(), values: Some(vec![value]) };
    let access_mode = match spec.access_mode {
        AccessMode::ReadWriteOnce => "ReadWriteOnce",
        AccessMode::ReadWriteOncePod => "ReadWriteOncePod",
    };
    let mut persistent_volume = PersistentVolume::default();
    persistent_volume.metadata.name = Some(name.clone());
    persistent_volume.spec = Some(PersistentVolumeSpec {
        capacity: Some(BTreeMap::from([("storage".to_string(), Quantity(spec.size_bytes.to_string()))])),
        access_modes: Some(vec![access_mode.to_string()]),
        // Whatever the StorageClass says: data that has survived the loss of its objects is not
        // removed because a claim was deleted.
        persistent_volume_reclaim_policy: Some("Retain".into()),
        storage_class_name: spec.storage_class.clone(),
        volume_mode: Some("Filesystem".into()),
        claim_ref: spec.claim.as_ref().map(|claim| ObjectReference {
            api_version: Some("v1".into()),
            kind: Some("PersistentVolumeClaim".into()),
            namespace: Some(claim.namespace.clone()),
            name: Some(claim.name.clone()),
            ..Default::default()
        }),
        csi: Some(CSIPersistentVolumeSource {
            driver: driver_name().into(),
            volume_handle: name,
            volume_attributes: Some(BTreeMap::from([(CONTEXT_POOL.to_string(), spec.pool.clone())])),
            ..Default::default()
        }),
        node_affinity: Some(VolumeNodeAffinity {
            required: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm { match_expressions: Some(vec![node]), match_fields: None }],
            }),
        }),
        ..Default::default()
    });
    Some(persistent_volume)
}

/// What is done about a volume an agent brought back from a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Nobody has approved it: it stays as it is, and no pod is given it.
    Wait,
    /// Approved, and it has no PersistentVolume: it gets one, and is a volume like any other.
    Restore,
    /// Approved, and its PersistentVolume is still there: it is a volume like any other again.
    Admit,
}

/// Whether an administrator has approved the volume: the annotation, with the value `true`.
pub fn approved(volume: &LocalPathVolume) -> bool {
    let annotations = volume.metadata.annotations.as_ref();
    annotations.and_then(|all| all.get(restore_approval())).is_some_and(|value| value == "true")
}

/// What is done about `volume`, which an agent brought back from a directory.
///
/// That the directory is the volume of that name, and whose volume it was, is the word of the
/// agent of its node, and an agent that was taken over can say anything: make a directory of
/// its own under the name of a volume whose object was lost, or name any claim of the cluster
/// as the one the data belongs to. Nothing in the cluster can tell. So nothing in the cluster
/// decides: not the claim that still names the volume, not the PersistentVolume that is still
/// there. An administrator does - an agent is not let to write the annotation - and until then
/// the volume is counted, measured and can be deleted, and no pod is given it.
pub fn step(volume: &LocalPathVolume, has_persistent_volume: bool) -> Step {
    match (approved(volume), has_persistent_volume) {
        (false, _) => Step::Wait,
        (true, false) => Step::Restore,
        (true, true) => Step::Admit,
    }
}

/// Runs until the process is stopped, in the replica that leads. Should two replicas run it for
/// a moment, nothing is lost: creating a PersistentVolume that exists and removing a label that
/// is gone change nothing.
pub async fn run(client: kube::Client, events: Arc<Events>, mut leading: watch::Receiver<bool>) {
    let volumes: Api<LocalPathVolume> = Api::all(client.clone());
    let restored = ListParams::default().labels(restore_label());
    loop {
        if leading.wait_for(|leads| *leads).await.is_err() {
            return;
        }
        match volumes.list(&restored).await {
            Ok(list) => {
                // In place, with its limits: the agent says so before anything is built on it.
                for volume in list.items.iter().filter(|v| v.phase() == Some(Phase::Orphaned)) {
                    if let Err(e) = restore(&client, &events, volume).await {
                        let name = volume.metadata.name.as_deref().unwrap_or_default();
                        tracing::warn!(volume = name, error = %e, "cannot bring back the PersistentVolume");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "cannot list LocalPathVolumes"),
        }
        tokio::time::sleep(INTERVAL).await;
    }
}

async fn restore(client: &kube::Client, events: &Events, volume: &LocalPathVolume) -> Result<(), kube::Error> {
    let volumes: Api<LocalPathVolume> = Api::all(client.clone());
    let persistent_volumes: Api<PersistentVolume> = Api::all(client.clone());
    let Some(name) = volume.metadata.name.clone() else { return Ok(()) };
    let existing = persistent_volumes.get_opt(&name).await?;
    match step(volume, existing.is_some()) {
        // Nobody has approved it yet: the volume waits as it is, and its status says for what.
        Step::Wait => return Ok(()),
        Step::Admit => {
            tracing::info!(volume = %name, node = %volume.data_node(), "approved: its PersistentVolume is still there");
        }
        Step::Restore => {
            let Some(persistent_volume) = persistent_volume(volume) else { return Ok(()) };
            match persistent_volumes.create(&PostParams::default(), &persistent_volume).await {
                Ok(_) => {
                    tracing::info!(volume = %name, node = %volume.data_node(), "approved: PersistentVolume brought back");
                    let reserved = match &volume.spec.claim {
                        Some(claim) => format!("reserved for a claim {}/{}", claim.namespace, claim.name),
                        None => "reserved for no claim".to_string(),
                    };
                    let note = format!(
                        "PersistentVolume {name} is brought back with reclaimPolicy Retain, {reserved}, as approved on the LocalPathVolume"
                    );
                    events.volume(volume, &Notice::normal("PersistentVolumeRestored", &note)).await;
                }
                Err(kube::Error::Api(response)) if response.code == 409 => {}
                Err(e) => return Err(e),
            }
        }
    }
    let patch = json!({"metadata": {"labels": {restore_label(): null}}});
    volumes.patch(&name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{Claim, LocalPathVolumeSpec, LocalPathVolumeStatus};

    fn volume(mobility: Mobility, access_mode: AccessMode) -> LocalPathVolume {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 50 << 30,
            mobility,
            node: "a".into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode,
            storage_class: Some("local".into()),
            claim: Some(Claim { namespace: "infra".into(), name: "storage-postgres-0".into() }),
        };
        let mut volume = LocalPathVolume::new("pvc-1", spec);
        volume.status = Some(LocalPathVolumeStatus { phase: Some(Phase::Ready), ..Default::default() });
        volume
    }

    #[test]
    fn restored_volume_keeps_its_data_and_waits_for_its_former_claim() {
        let persistent_volume = persistent_volume(&volume(Mobility::Pinned, AccessMode::ReadWriteOnce)).unwrap();
        assert_eq!(persistent_volume.metadata.name.as_deref(), Some("pvc-1"));
        let spec = persistent_volume.spec.unwrap();
        assert_eq!(spec.persistent_volume_reclaim_policy.as_deref(), Some("Retain"));
        assert_eq!(spec.storage_class_name.as_deref(), Some("local"));
        assert_eq!(spec.capacity.unwrap()["storage"], Quantity("53687091200".into()));
        assert_eq!(spec.access_modes.unwrap(), ["ReadWriteOnce"]);
        let claim = spec.claim_ref.unwrap();
        assert_eq!((claim.namespace.as_deref(), claim.name.as_deref()), (Some("infra"), Some("storage-postgres-0")));
        // No uid: the claim that binds is a new object with the old name.
        assert_eq!(claim.uid, None);
        let csi = spec.csi.unwrap();
        assert_eq!((csi.driver.as_str(), csi.volume_handle.as_str()), (driver_name(), "pvc-1"));
        assert_eq!(csi.volume_attributes.unwrap()[CONTEXT_POOL], "default");
    }

    fn affinity(persistent_volume: PersistentVolume) -> (String, Vec<String>) {
        let terms = persistent_volume.spec.unwrap().node_affinity.unwrap().required.unwrap().node_selector_terms;
        let requirement = terms[0].match_expressions.clone().unwrap().remove(0);
        (requirement.key, requirement.values.unwrap())
    }

    #[test]
    fn restored_volume_is_reachable_from_where_its_mobility_says() {
        let pinned = persistent_volume(&volume(Mobility::Pinned, AccessMode::ReadWriteOnce)).unwrap();
        assert_eq!(affinity(pinned), (topology_node().to_string(), vec!["a".to_string()]));
        let floating = persistent_volume(&volume(Mobility::Floating, AccessMode::ReadWriteOnce)).unwrap();
        assert_eq!(affinity(floating), (topology_storage().to_string(), vec!["true".to_string()]));
    }

    #[test]
    fn restored_volume_keeps_the_access_mode_of_its_claim() {
        let single_pod = persistent_volume(&volume(Mobility::Pinned, AccessMode::ReadWriteOncePod)).unwrap();
        assert_eq!(single_pod.spec.unwrap().access_modes.unwrap(), ["ReadWriteOncePod"]);
    }

    /// The claim `infra/storage-postgres-0` - the one the directory of `volume()` names - as
    /// the API server has it.
    fn approved(mut volume: LocalPathVolume, value: &str) -> LocalPathVolume {
        volume.metadata.annotations = Some([(crate::restore_approval().to_string(), value.to_string())].into());
        volume
    }

    fn reserved_for(persistent_volume: PersistentVolume) -> Option<(String, String, Option<String>)> {
        let claim = persistent_volume.spec.unwrap().claim_ref?;
        Some((claim.namespace.unwrap(), claim.name.unwrap(), claim.uid))
    }

    #[test]
    fn what_a_directory_says_of_its_claim_is_not_enough_for_a_persistent_volume() {
        // The directory, and so the agent of its node, names the claim: anybody who has taken
        // the node over could name any claim of the cluster there.
        let volume = volume(Mobility::Pinned, AccessMode::ReadWriteOnce);
        assert_eq!(step(&volume, false), Step::Wait);
    }

    #[test]
    fn persistent_volume_that_is_still_there_does_not_speak_for_the_volume() {
        // Only the LocalPathVolume was lost. Which directory is the volume of that name is
        // still the word of an agent: the pods of its claim would be given whatever is in it.
        let volume = volume(Mobility::Pinned, AccessMode::ReadWriteOnce);
        assert_eq!(step(&volume, true), Step::Wait);
        // Approved, it lacks nothing.
        assert_eq!(step(&approved(volume, "true"), true), Step::Admit);
    }

    #[test]
    fn administrator_approves_a_volume_for_the_claim_its_directory_names() {
        let volume = approved(volume(Mobility::Pinned, AccessMode::ReadWriteOnce), "true");
        assert_eq!(step(&volume, false), Step::Restore);
        // Reserved by name, for a claim that is yet to be created.
        let persistent_volume = persistent_volume(&volume).unwrap();
        assert_eq!(reserved_for(persistent_volume), Some(("infra".into(), "storage-postgres-0".into(), None)));
        // Only `true` is an approval.
        for value in ["", "false", "yes", "True"] {
            let volume = approved(self::volume(Mobility::Pinned, AccessMode::ReadWriteOnce), value);
            for has_persistent_volume in [false, true] {
                assert_eq!(step(&volume, has_persistent_volume), Step::Wait, "{value:?} was taken for an approval");
            }
        }
    }

    #[test]
    fn volume_without_a_claim_on_record_comes_back_only_when_approved() {
        let mut nameless = volume(Mobility::Pinned, AccessMode::ReadWriteOnce);
        nameless.spec.claim = None;
        assert_eq!(step(&nameless, false), Step::Wait);
        let nameless = approved(nameless, "true");
        assert_eq!(step(&nameless, false), Step::Restore);
        assert_eq!(reserved_for(persistent_volume(&nameless).unwrap()), None);
    }
}
