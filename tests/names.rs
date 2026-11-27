//! The API group and the driver name given on the command line reach everything built from them.
//! A file of its own: the names are set once per process, and the other tests run with the
//! defaults.

use kube::Resource;
use local_path_csi::api::{AccessMode, LocalPathNode, LocalPathVolume, LocalPathVolumeSpec, Mobility};

#[test]
fn configured_names_are_the_ones_the_process_uses() {
    local_path_csi::configure("example.com", "path.csi.example.com").unwrap();

    // What the API server is asked for.
    assert_eq!(LocalPathVolume::url_path(&(), None), "/apis/example.com/v1alpha1/localpathvolumes");
    assert_eq!(LocalPathNode::url_path(&(), None), "/apis/example.com/v1alpha1/localpathnodes");

    // What an object says of itself.
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
    let object = serde_json::to_value(LocalPathVolume::new("pv-1", spec)).unwrap();
    assert_eq!(object["apiVersion"], "example.com/v1alpha1");
    assert_eq!(object["kind"], "LocalPathVolume");

    // What is written on objects and nodes.
    assert_eq!(local_path_csi::api_group(), "example.com");
    assert_eq!(local_path_csi::finalizer(), "example.com/data");
    assert_eq!(local_path_csi::restore_label(), "example.com/restore-pv");
    assert_eq!(local_path_csi::driver_name(), "path.csi.example.com");
    assert_eq!(local_path_csi::topology_node(), "path.csi.example.com/node");
    assert_eq!(local_path_csi::topology_storage(), "path.csi.example.com/storage");

    // The definitions follow the group.
    let files: Vec<String> = local_path_csi::api::crds().into_iter().map(|(file, _)| file).collect();
    assert_eq!(files, ["localpathvolumes.example.com.yaml", "localpathnodes.example.com.yaml"]);

    // The names of a running process do not change.
    assert!(local_path_csi::configure("other.example.com", "path.csi.example.com").is_err());
    assert_eq!(local_path_csi::api_group(), "example.com");
}
