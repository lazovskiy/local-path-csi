//! Custom resources of the driver: the controller and the agents talk only through them.

use std::borrow::Cow;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta, Time};
use k8s_openapi::jiff::Timestamp;
use kube::{CustomResourceExt, Resource};
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use compiled::{LocalPathNodeSpec, LocalPathVolumeSpec};

/// Access mode of the claim a volume was created for.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum AccessMode {
    #[default]
    ReadWriteOnce,
    ReadWriteOncePod,
}

/// Whether a volume stays on its node or follows its pod.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum Mobility {
    /// The PersistentVolume is bound to the node: a pod waits for that node.
    #[default]
    Pinned,
    /// The PersistentVolume is reachable from every storage node: the data moves after the pod.
    Floating,
}

/// The resources as `kube` describes them to the API server. It builds a definition only together
/// with a type bound to the group written in the attribute, and the group of the driver is a flag:
/// those types stay in this module, and the driver works with the ones declared below it.
mod compiled {
    use kube::CustomResource;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::{AccessMode, Claim, LocalPathNodeStatus, LocalPathVolumeStatus, Mobility};

    /// A volume: a directory in a pool on one node. Named after its PersistentVolume.
    #[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
    #[kube(group = "local-path-csi.domik.fi", version = "v1alpha1", kind = "LocalPathVolume")]
    #[kube(status = "LocalPathVolumeStatus", shortname = "lpv")]
    #[kube(doc = "A directory in a pool on one node, named after its PersistentVolume.")]
    #[kube(printcolumn = r#"{"name":"Pool","type":"string","jsonPath":".spec.pool"}"#)]
    #[kube(printcolumn = r#"{"name":"Node","type":"string","jsonPath":".status.node"}"#)]
    #[kube(printcolumn = r#"{"name":"Size","type":"integer","jsonPath":".spec.sizeBytes"}"#)]
    #[kube(printcolumn = r#"{"name":"Used","type":"integer","jsonPath":".status.usedBytes"}"#)]
    #[kube(printcolumn = r#"{"name":"Mobility","type":"string","jsonPath":".spec.mobility"}"#)]
    #[kube(printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#)]
    #[kube(printcolumn = r#"{"name":"Attached","type":"string","jsonPath":".status.attachedNode","priority":1}"#)]
    #[serde(rename_all = "camelCase")]
    pub struct LocalPathVolumeSpec {
        /// Pool that holds the volume.
        pub pool: String,
        /// Size of the volume: the quota limit, or the reservation in a pool without quota.
        pub size_bytes: u64,
        #[serde(default)]
        pub mobility: Mobility,
        /// Node the volume has to be on.
        pub node: String,
        /// Files and directories allowed per GiB of size; zero means unlimited.
        #[serde(default, rename = "inodesPerGiB")]
        pub inodes_per_gib: u64,
        /// Permissions of the volume directory, octal, for example `0770`.
        pub dir_mode: String,
        /// Seconds a deleted volume is kept in the trash of its pool before its data is removed.
        #[serde(default)]
        pub delete_grace_seconds: u64,
        #[serde(default)]
        pub access_mode: AccessMode,
        /// StorageClass of the claim.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub storage_class: Option<String>,
        /// The PersistentVolumeClaim the volume was created for.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub claim: Option<Claim>,
    }

    /// Storage of one node, reported by its agent. Named after the node.
    #[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq, JsonSchema)]
    #[kube(group = "local-path-csi.domik.fi", version = "v1alpha1", kind = "LocalPathNode")]
    #[kube(status = "LocalPathNodeStatus", shortname = "lpn")]
    #[kube(doc = "Storage of one node, reported by its agent. Named after the node.")]
    #[kube(printcolumn = r#"{"name":"Pools","type":"string","jsonPath":".status.pools[*].name"}"#)]
    #[kube(printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.pools[*].state"}"#)]
    #[kube(printcolumn = r#"{"name":"Allocatable","type":"string","jsonPath":".status.pools[*].allocatableBytes"}"#)]
    #[kube(printcolumn = r#"{"name":"Reported","type":"date","jsonPath":".status.reportedAt"}"#)]
    pub struct LocalPathNodeSpec {}
}

/// Declares a resource as the driver reads and writes it: the one `compiled` describes under the
/// same name, in the API group of the process.
macro_rules! resource {
    ($(#[$attribute:meta])* $name:ident, $spec:ident, $status:ident) => {
        $(#[$attribute])*
        #[derive(Deserialize, Clone, Debug, PartialEq)]
        pub struct $name {
            pub metadata: ObjectMeta,
            pub spec: $spec,
            #[serde(default)]
            pub status: Option<$status>,
        }

        impl $name {
            /// Spec based constructor.
            pub fn new(name: &str, spec: $spec) -> Self {
                let metadata = ObjectMeta { name: Some(name.to_string()), ..ObjectMeta::default() };
                Self { metadata, spec, status: None }
            }
        }

        impl Resource for $name {
            type DynamicType = ();
            type Scope = <compiled::$name as Resource>::Scope;

            fn group(_: &()) -> Cow<'_, str> {
                Cow::Borrowed(crate::api_group())
            }

            fn kind(_: &()) -> Cow<'_, str> {
                <compiled::$name as Resource>::kind(&())
            }

            fn version(_: &()) -> Cow<'_, str> {
                <compiled::$name as Resource>::version(&())
            }

            fn plural(_: &()) -> Cow<'_, str> {
                <compiled::$name as Resource>::plural(&())
            }

            fn meta(&self) -> &ObjectMeta {
                &self.metadata
            }

            fn meta_mut(&mut self) -> &mut ObjectMeta {
                &mut self.metadata
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut obj = serializer.serialize_struct(stringify!($name), 4 + usize::from(self.status.is_some()))?;
                obj.serialize_field("apiVersion", &Self::api_version(&()))?;
                obj.serialize_field("kind", &Self::kind(&()))?;
                obj.serialize_field("metadata", &self.metadata)?;
                obj.serialize_field("spec", &self.spec)?;
                if let Some(status) = &self.status {
                    obj.serialize_field("status", status)?;
                }
                obj.end()
            }
        }
    };
}

resource!(
    /// A volume: a directory in a pool on one node. Named after its PersistentVolume.
    LocalPathVolume,
    LocalPathVolumeSpec,
    LocalPathVolumeStatus
);

resource!(
    /// Storage of one node, reported by its agent. Named after the node.
    #[derive(Default)]
    LocalPathNode,
    LocalPathNodeSpec,
    LocalPathNodeStatus
);

/// What is written next to the data of a volume: enough to bring the volume back when its
/// LocalPathVolume and PersistentVolume are gone.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Passport {
    pub name: String,
    pub spec: LocalPathVolumeSpec,
    /// How many times the volume had changed its node when this copy was written: of two copies
    /// of one volume the one with the larger number is the newer.
    #[serde(default)]
    pub moves: u64,
}

impl Passport {
    pub fn of(volume: &LocalPathVolume) -> Option<Self> {
        Some(Self { name: volume.metadata.name.clone()?, spec: volume.spec.clone(), moves: volume.moves() })
    }

    pub fn to_json(&self) -> String {
        let mut text = serde_json::to_string_pretty(self).expect("passport serializes");
        text.push('\n');
        text
    }

    pub fn parse(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    pub namespace: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum Phase {
    /// The agent has not created the directory yet.
    Pending,
    /// The directory exists and carries the requested limits.
    Ready,
    /// The directory was there and is gone: the agent does not recreate an empty one.
    Lost,
    /// The directory is there, with its limits, and nothing in the cluster vouches for it: an
    /// agent brought the volume back from the directory, and no pod is given it until an
    /// administrator approves it.
    Orphaned,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalPathVolumeStatus {
    /// Node that holds the data. Written by the controller only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Node the volume is published to. Written by the controller only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attached_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,
    /// Size the agent has applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_bytes: Option<u64>,
    /// Why the volume is not ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Node whose agent wrote `phase`, `capacityBytes` and `usedBytes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported_by: Option<String>,
    /// How many times the volume has changed its node. Written by the controller when it hands
    /// the volume over, and by an agent that brings a volume back from its directory.
    #[serde(default)]
    pub moves: u64,
    /// The copy being made on the node the volume was sent to. Written by the agent of that node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer: Option<Transfer>,
    /// What is known of the volume, each condition with the generation of the volume it was
    /// seen at. `Ready` is written by the agent of the node that holds the data, with `phase`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "conditions_schema")]
    pub conditions: Vec<Condition>,
}

/// The condition that tells whether the directory of a volume is there with the limits of its
/// spec. Its reasons are below.
pub const READY: &str = "Ready";
/// The directory is there with the limits of the generation the condition names.
pub const REASON_PROVISIONED: &str = "Provisioned";
/// The pool of the volume is not there or has failed its checks: nothing in it is looked at.
pub const REASON_POOL_NOT_READY: &str = "PoolNotReady";
pub const REASON_INVALID_DIR_MODE: &str = "InvalidDirMode";
/// The volume was handed over to a node whose copy of it was not completed.
pub const REASON_COPY_INCOMPLETE: &str = "CopyIncomplete";
/// The directory was there and is gone.
pub const REASON_DIRECTORY_LOST: &str = "DirectoryLost";
/// An agent brought the volume back from its directory, and no administrator has approved it.
pub const REASON_AWAITING_APPROVAL: &str = "AwaitingApproval";

/// The schema of `metav1.Condition` in a list that is a map by `type`: what `kubectl wait` and
/// server-side apply expect of conditions, and each writer owns the conditions of its types.
fn conditions_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema = serde_json::json!({
        "type": "array",
        "x-kubernetes-list-type": "map",
        "x-kubernetes-list-map-keys": ["type"],
        "items": {
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "status": {"type": "string", "enum": ["True", "False", "Unknown"]},
                "reason": {"type": "string"},
                "message": {"type": "string"},
                "observedGeneration": {"type": "integer", "format": "int64"},
                "lastTransitionTime": {"type": "string", "format": "date-time"},
            },
            "required": ["type", "status", "reason", "message", "lastTransitionTime"],
        },
    });
    serde_json::from_value(schema).expect("the schema of conditions is a schema")
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum TransferState {
    /// The node receives other volumes and has not started this one.
    Queued,
    Copying,
    /// The copy is complete and verified: the controller may hand the volume over.
    Copied,
    /// The last attempt failed; the agent tries again.
    Failed,
}

/// A copy of a volume on its way from the node that holds the data to the node the volume was
/// sent to.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Transfer {
    /// Node the data is copied from.
    pub source: String,
    /// Node that receives the copy.
    pub target: String,
    pub state: TransferState,
    /// `metadata.generation` of the volume as the node read it before it went over the data for
    /// the last time. Every change of where the volume has to be gives it a new generation: a
    /// report about another one is about a copy made for a move that is no more.
    #[serde(default)]
    pub observed_generation: Option<i64>,
    /// Bytes the copy occupies so far, when the pool accounts them.
    #[serde(default)]
    pub received_bytes: Option<u64>,
    /// When the node started on this volume, RFC 3339.
    #[serde(default, deserialize_with = "readable_time")]
    #[schemars(schema_with = "time_schema")]
    pub started_at: Option<Time>,
    /// Why the copy is not being made, or why the last attempt failed.
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalPathNodeStatus {
    #[serde(default)]
    pub pools: Vec<PoolStatus>,
    /// When the agent wrote this report, RFC 3339. The controller places nothing on a node whose
    /// report is old: its agent is not running.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "readable_time")]
    #[schemars(schema_with = "time_schema")]
    pub reported_at: Option<Time>,
    /// Where the agent serves volumes to the agents of other nodes, `host:port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_address: Option<String>,
    /// Certificate of the agent, PEM: the agents of two nodes recognise each other by what
    /// their LocalPathNodes say.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_certificate: Option<String>,
}

impl LocalPathNodeStatus {
    /// Seconds since the agent reported, by the clock of the caller; `None` when the node has
    /// never reported or the time is unreadable.
    pub fn report_age(&self, unix_now: u64) -> Option<u64> {
        let reported = self.reported_at.as_ref()?.0.as_second();
        // A report from the future (clocks differ) is as fresh as it gets.
        Some(unix_now.saturating_sub(u64::try_from(reported).ok()?))
    }
}

/// A Unix time as the objects of Kubernetes carry one.
pub fn time(unix_seconds: u64) -> Time {
    let seconds = i64::try_from(unix_seconds).unwrap_or(i64::MAX);
    Time(Timestamp::from_second(seconds).unwrap_or(Timestamp::UNIX_EPOCH))
}

/// RFC 3339 form of a Unix time, the one `kubectl` shows as an age.
pub fn timestamp(unix_seconds: u64) -> String {
    time(unix_seconds).0.to_string()
}

/// Reads a time of a status. One that is no time is read as none: an object with one field
/// spoiled by hand must not become unreadable as a whole, to every watch that carries it.
fn readable_time<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Time>, D::Error> {
    let written = Option::<String>::deserialize(deserializer)?;
    Ok(written.and_then(|written| written.parse::<Timestamp>().ok()).map(Time))
}

fn time_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({"type": "string", "format": "date-time", "nullable": true})
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum PoolState {
    Ready,
    /// The path does not exist on the node.
    Missing,
    /// The path is not the root of a mounted filesystem.
    NotMountpoint,
    /// The filesystem is mounted without project quota.
    NoQuota,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolStatus {
    pub name: String,
    pub path: String,
    pub state: PoolState,
    /// Volumes of the pool are limited by project quota.
    pub quota: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// Size of the filesystem.
    pub capacity_bytes: u64,
    /// What volume sizes may add up to: the size of the filesystem times the overcommit ratio.
    pub allocatable_bytes: u64,
    /// Free space of the filesystem right now.
    pub available_bytes: u64,
}

impl LocalPathVolumeSpec {
    /// Limit on the number of files and directories of the volume; zero means unlimited.
    pub fn inode_limit(&self) -> u64 {
        const GIB: u64 = 1 << 30;
        self.size_bytes.div_ceil(GIB).saturating_mul(self.inodes_per_gib)
    }

    /// Permissions of the volume directory, or `None` when `dir_mode` is not an octal mode.
    pub fn mode(&self) -> Option<u32> {
        u32::from_str_radix(&self.dir_mode, 8).ok().filter(|mode| *mode <= 0o7777)
    }
}

impl LocalPathVolume {
    /// Node that holds the data: decided by the controller, the requested node until then.
    pub fn data_node(&self) -> &str {
        self.status.as_ref().and_then(|s| s.node.as_deref()).unwrap_or(&self.spec.node)
    }

    /// The condition of this type, when somebody has written one.
    pub fn condition(&self, kind: &str) -> Option<&Condition> {
        self.status.as_ref()?.conditions.iter().find(|condition| condition.type_ == kind)
    }

    pub fn phase(&self) -> Option<Phase> {
        self.status.as_ref().and_then(|s| s.phase)
    }

    /// Whether an agent brought the volume back from a directory and the controller has not
    /// given it a PersistentVolume yet.
    pub fn brought_back(&self) -> bool {
        self.metadata.labels.as_ref().is_some_and(|labels| labels.contains_key(crate::restore_label()))
    }

    /// How many times the volume has changed its node.
    pub fn moves(&self) -> u64 {
        self.status.as_ref().map(|s| s.moves).unwrap_or(0)
    }

    /// The node the volume is on its way to, when it is not where its data is.
    pub fn destination(&self) -> Option<&str> {
        Some(self.spec.node.as_str()).filter(|node| *node != self.data_node())
    }

    /// The patch that takes the finalizer of the driver off the volume and lets it go; `None`
    /// when the volume does not carry it.
    ///
    /// A JSON patch that names the finalizer by its place in the list and holds only while that
    /// place has it. A list written back as a whole would drop a finalizer somebody added
    /// between the read and the write; this patch fails instead, and whoever sent it reads the
    /// volume again.
    pub fn without_finalizer(&self) -> Option<json_patch::Patch> {
        let place = self.metadata.finalizers.iter().flatten().position(|f| f == crate::finalizer())?;
        let path = format!("/metadata/finalizers/{place}");
        let operations = serde_json::json!([
            {"op": "test", "path": path, "value": crate::finalizer()},
            {"op": "remove", "path": path},
        ]);
        Some(serde_json::from_value(operations).expect("the operations are a JSON patch"))
    }

    /// The copy the destination reports, when it is a copy of the data as it is now for the
    /// node the volume is sent to now.
    ///
    /// The nodes alone do not tell: a volume sent to a node, called back and sent there again
    /// still carries the report of the first time, and the data may have changed in between.
    /// The generation does: it changes with every change of `spec`, and the volume is given to
    /// a pod only where `spec.node` says.
    pub fn transfer(&self) -> Option<&Transfer> {
        let transfer = self.status.as_ref()?.transfer.as_ref()?;
        let this_move = transfer.source == self.data_node()
            && Some(transfer.target.as_str()) == self.destination()
            && transfer.observed_generation.is_some()
            && transfer.observed_generation == self.metadata.generation;
        this_move.then_some(transfer)
    }
}

/// The CustomResourceDefinitions of the driver in the API group of this process, as
/// `(file name, JSON document)`. JSON is YAML: `kubectl apply` takes the files as they are, and the
/// chart renders the ones in its `files/crds/` with the group of its values.
pub fn crds() -> Vec<(String, String)> {
    let group = crate::api_group();
    [volume_crd(group), node_crd(group)]
        .into_iter()
        .map(|crd| (format!("{}.yaml", crd.metadata.name.as_deref().unwrap_or_default()), render(crd)))
        .collect()
}

fn render(crd: CustomResourceDefinition) -> String {
    let mut text = serde_json::to_string_pretty(&crd).expect("CRD serializes");
    text.push('\n');
    text
}

/// A definition `kube` built, moved to `group`.
fn in_group(mut crd: CustomResourceDefinition, group: &str) -> CustomResourceDefinition {
    crd.metadata.name = Some(format!("{}.{group}", crd.spec.names.plural));
    crd.spec.group = group.to_string();
    // The chart carries the definitions as templates, and helm deletes what a release made:
    // without this an uninstall would take every LocalPathVolume with the definition, and an
    // agent removes the data of a volume that is deleted.
    crd.metadata.annotations = Some([("helm.sh/resource-policy".to_string(), "keep".to_string())].into());
    crd
}

fn volume_crd(group: &str) -> CustomResourceDefinition {
    in_group(compiled::LocalPathVolume::crd(), group)
}

fn node_crd(group: &str) -> CustomResourceDefinition {
    in_group(compiled::LocalPathNode::crd(), group)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `kubectl wait --for=condition=...` and server-side apply go by this: a list that is a map
    /// by `type`, of conditions in the shape of `metav1.Condition`.
    #[test]
    fn conditions_of_a_volume_are_a_map_by_type_in_the_definition() {
        let definition = serde_json::to_value(volume_crd("example.com")).unwrap();
        let status = &definition["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"];
        let conditions = &status["properties"]["conditions"];
        assert_eq!(conditions["type"], "array");
        assert_eq!(conditions["x-kubernetes-list-type"], "map");
        assert_eq!(conditions["x-kubernetes-list-map-keys"], serde_json::json!(["type"]));
        let condition = &conditions["items"];
        for field in ["type", "status", "reason", "message", "lastTransitionTime"] {
            assert!(condition["required"].as_array().unwrap().iter().any(|required| required == field), "{field}");
            assert_eq!(condition["properties"][field]["type"], "string", "{field}");
        }
        assert_eq!(condition["properties"]["observedGeneration"]["type"], "integer");
        assert!(
            !status["required"].as_array().is_some_and(|r| r.iter().any(|f| f == "conditions")),
            "old objects have none"
        );
    }

    #[test]
    fn time_of_a_report_is_written_as_the_agents_before_wrote_it() {
        let status = LocalPathNodeStatus { reported_at: Some(time(1_795_000_000)), ..Default::default() };
        let written = serde_json::to_value(&status).unwrap();
        assert_eq!(written["reportedAt"], "2026-11-18T11:06:40Z");
        let read: LocalPathNodeStatus = serde_json::from_value(written).unwrap();
        assert_eq!(read, status);
        // Null, as a merge patch of the controller leaves a field it has taken back.
        let read: Transfer = serde_json::from_value(serde_json::json!({
            "source": "a", "target": "b", "state": "Copying", "startedAt": null,
        }))
        .unwrap();
        assert_eq!(read.started_at, None);
        let read: Transfer = serde_json::from_value(serde_json::json!({
            "source": "a", "target": "b", "state": "Copying", "startedAt": "2026-11-18T11:06:40Z",
        }))
        .unwrap();
        assert_eq!(read.started_at, Some(time(1_795_000_000)));
    }

    #[test]
    fn times_of_a_status_are_dates_to_the_api_server() {
        let node = serde_json::to_value(node_crd("example.com")).unwrap();
        let reported = &node["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]["properties"]
            ["reportedAt"];
        assert_eq!(
            (&reported["type"], &reported["format"], &reported["nullable"]),
            (&"string".into(), &"date-time".into(), &true.into())
        );
        assert!(reported["description"].as_str().is_some_and(|text| text.contains("RFC 3339")));
    }

    #[test]
    fn volume_without_conditions_is_read_and_written_as_before() {
        let read: LocalPathVolumeStatus = serde_json::from_value(serde_json::json!({"phase": "Ready"})).unwrap();
        assert!(read.conditions.is_empty());
        assert_eq!(serde_json::to_value(&read).unwrap(), serde_json::json!({"phase": "Ready", "moves": 0}));
    }

    #[test]
    fn report_age_is_measured_from_the_time_the_agent_wrote() {
        assert_eq!(timestamp(1_795_000_000), "2026-11-18T11:06:40Z");
        let status =
            LocalPathNodeStatus { pools: Vec::new(), reported_at: Some(time(1_795_000_000)), ..Default::default() };
        assert_eq!(status.report_age(1_795_000_090), Some(90));
        assert_eq!(status.report_age(1_794_999_000), Some(0));
        assert_eq!(LocalPathNodeStatus::default().report_age(1_795_000_000), None);
        // A time that is none cannot be held any more: it is lost when the object is read.
        let garbled: LocalPathNodeStatus =
            serde_json::from_value(serde_json::json!({"reportedAt": "yesterday"})).unwrap();
        assert_eq!(garbled.report_age(1_795_000_000), None);
    }

    #[test]
    fn finalizer_is_taken_off_by_its_place_and_the_others_are_not_named() {
        let spec = serde_json::json!({"pool": "default", "sizeBytes": 1, "node": "a", "dirMode": "0770"});
        let mut volume = LocalPathVolume::new("pv-1", serde_json::from_value(spec).unwrap());
        let ours = crate::finalizer().to_string();
        let mut patch = |finalizers: Option<Vec<&str>>| {
            volume.metadata.finalizers = finalizers.map(|list| list.into_iter().map(str::to_string).collect());
            volume.without_finalizer().map(|patch| serde_json::to_value(patch).unwrap())
        };
        assert_eq!(
            patch(Some(vec!["example.com/backup", &ours, "example.com/audit"])),
            Some(serde_json::json!([
                {"op": "test", "path": "/metadata/finalizers/1", "value": ours},
                {"op": "remove", "path": "/metadata/finalizers/1"},
            ]))
        );
        assert_eq!(
            patch(Some(vec![&ours])),
            Some(serde_json::json!([
                {"op": "test", "path": "/metadata/finalizers/0", "value": ours},
                {"op": "remove", "path": "/metadata/finalizers/0"},
            ]))
        );
        // Released already: nothing to send.
        assert_eq!(patch(Some(vec!["example.com/backup"])), None);
        assert_eq!(patch(Some(vec![])), None);
        assert_eq!(patch(None), None);
    }

    #[test]
    fn report_of_a_copy_counts_only_for_the_move_it_was_made_for() {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 1 << 30,
            mobility: Mobility::Floating,
            node: "b".into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode: AccessMode::ReadWriteOnce,
            storage_class: None,
            claim: None,
        };
        let report = |observed_generation| Transfer {
            source: "a".into(),
            target: "b".into(),
            state: TransferState::Copied,
            observed_generation,
            received_bytes: None,
            started_at: None,
            message: None,
        };
        let mut volume = LocalPathVolume::new("pv-1", spec);
        volume.metadata.generation = Some(4);
        let mut reported = |transfer: Transfer, generation: i64| {
            volume.metadata.generation = Some(generation);
            volume.status =
                Some(LocalPathVolumeStatus { node: Some("a".into()), transfer: Some(transfer), ..Default::default() });
            volume.transfer().is_some()
        };
        assert!(reported(report(Some(4)), 4));
        // Sent to node b, called back and sent to node b again: the spec has changed twice.
        assert!(!reported(report(Some(4)), 6));
        // An agent that does not say what it has copied has copied nothing that counts.
        assert!(!reported(report(None), 4));
        assert!(!reported(Transfer { source: "c".into(), ..report(Some(4)) }, 4));
        assert!(!reported(Transfer { target: "c".into(), ..report(Some(4)) }, 4));
    }

    #[test]
    fn passport_carries_the_whole_volume_through_the_disk() {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 10 << 30,
            mobility: Mobility::Floating,
            node: "a".into(),
            inodes_per_gib: 1000,
            dir_mode: "0750".into(),
            delete_grace_seconds: 3600,
            access_mode: AccessMode::ReadWriteOncePod,
            storage_class: Some("local".into()),
            claim: Some(Claim { namespace: "infra".into(), name: "data".into() }),
        };
        let passport = Passport::of(&LocalPathVolume::new("pvc-1", spec.clone())).unwrap();
        assert_eq!(Passport::parse(&passport.to_json()), Some(Passport { name: "pvc-1".into(), spec, moves: 0 }));
        assert_eq!(Passport::parse("{}"), None);
        assert_eq!(Passport::parse("not json"), None);
    }

    #[test]
    fn a_crd_takes_the_group_it_is_asked_for() {
        let crd = volume_crd("example.com");
        assert_eq!(crd.spec.group, "example.com");
        assert_eq!(crd.metadata.name.as_deref(), Some("localpathvolumes.example.com"));
        assert_eq!(crd.spec.names.plural, "localpathvolumes");
        assert_eq!(crd.spec.names.kind, "LocalPathVolume");
        let node = node_crd("example.com");
        assert_eq!(node.spec.names.plural, "localpathnodes");
        assert_eq!(node.metadata.name.as_deref(), Some("localpathnodes.example.com"));
    }

    #[test]
    fn a_definition_outlives_the_release_that_made_it() {
        for crd in [volume_crd("example.com"), node_crd("example.com")] {
            let annotations = crd.metadata.annotations.unwrap_or_default();
            assert_eq!(annotations.get("helm.sh/resource-policy").map(String::as_str), Some("keep"));
        }
    }

    #[test]
    fn the_chart_carries_the_definitions_of_this_build() {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("chart/files/crds");
        let generated = crds();
        for (file, text) in &generated {
            let path = directory.join(file);
            let kept = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            assert_eq!(&kept, text, "{} is stale: cargo run -- crds --out chart/files/crds", path.display());
        }
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), generated.len());
    }

    #[test]
    fn an_object_is_written_with_the_api_version_of_this_process() {
        let spec = LocalPathVolumeSpec {
            pool: "default".into(),
            size_bytes: 1,
            mobility: Mobility::Pinned,
            node: "a".into(),
            inodes_per_gib: 0,
            dir_mode: "0770".into(),
            delete_grace_seconds: 0,
            access_mode: AccessMode::ReadWriteOnce,
            storage_class: None,
            claim: None,
        };
        let text = serde_json::to_value(LocalPathVolume::new("pv-1", spec)).unwrap();
        assert_eq!(text["apiVersion"], "local-path-csi.domik.fi/v1alpha1");
        assert_eq!(text["kind"], "LocalPathVolume");
    }
}
