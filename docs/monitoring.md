# Monitoring

local-path-csi exports Prometheus metrics, ships alerts and a Grafana dashboard, and writes
Kubernetes events. To enable the PodMonitor, the PrometheusRule and the dashboard, see the
`metrics.*` values in the [chart README](../chart/README.md#metrics).

## Metrics

The controller and the agents serve `/metrics` on `metrics.port` (default `9753`).

- The agents read their values from the disk and the quota at scrape time.
- The controller answers from what its watches have seen of the custom resources.

Every controller replica exports the same controller series. Aggregate them with `max by (...)`.

### Agent metrics

Every agent metric has the `node` label.

| Metric | Description |
|---|---|
| `local_path_csi_volume_size_bytes`, `local_path_csi_volume_used_bytes` | Size and usage of a volume. Labels: `node`, `pool`, `persistentvolume`, `namespace`, `persistentvolumeclaim`. Usage is absent in a pool without quota. |
| `local_path_csi_volume_inodes_used`, `local_path_csi_volume_inodes_limit` | Files of a volume, and their limit if there is one. |
| `local_path_csi_pool_ready` | `1` for a pool that can hold volumes. The `state` label is `Ready`, `Missing` (no such path), `NotMountpoint` or `NoQuota`. |
| `local_path_csi_pool_capacity_bytes`, `local_path_csi_pool_allocatable_bytes`, `local_path_csi_pool_available_bytes` | The filesystem of a pool: its size, the total size that volumes may add up to, and its free space. |
| `local_path_csi_pool_trash_volumes`, `local_path_csi_pool_trash_bytes` | The trash of a pool. |
| `local_path_csi_agent_errors_total` | Failures of the agent, by `operation`: `reconcile` or `report`. |
| `local_path_csi_agent_volumes_total` | Directories the agent handled, by `action`: `removed`, `trashed`, `purged`, `restored` or `received`. |
| `local_path_csi_agent_seconds_since_last_pass` | Seconds since the agent last went through all volumes of its node. It does this at least every 30 seconds. The node report is sent separately, so an agent stuck in a pass still looks alive everywhere else. |
| `local_path_csi_transfers_running` | Volumes the node is receiving now, including the ones that wait for their turn. |
| `local_path_csi_transfers_total` | Volumes the node has received (`result="completed"`) or failed to receive in an attempt (`result="failed"`). |
| `local_path_csi_transfer_seconds_total`, `local_path_csi_transfer_received_bytes_total` | Time spent on received volumes, and their size. |
| `local_path_csi_transfer_uncompressed_bytes_total`, `local_path_csi_transfer_compressed_bytes_total` | Per `persistentvolume`, on the receiving node: bytes rsync sent for the volume, and bytes that crossed the network for them. Kept while the volume is on the node or on its way to it. |

### Controller metrics

| Metric | Description |
|---|---|
| `local_path_csi_volume_moving` | `1` for a volume on its way to another node. Labels: `persistentvolume`, `namespace`, `persistentvolumeclaim`, `source`, `target` and `state` (`Pending`, `Queued`, `Copying`, `Copied`, `Failed`). |
| `local_path_csi_volume_moving_received_bytes` | What has arrived of a volume on its way. The same labels without `state`. Absent in a pool without quota. |
| `local_path_csi_volumes` | Volumes by `node`, `pool`, `mobility` and `phase`. |
| `local_path_csi_pool_allocated_bytes` | Total size of the volumes of a pool, including the volumes on their way to the node. |
| `local_path_csi_pool_free_bytes` | Free space of a pool as reported to the scheduler. |
| `local_path_csi_node_agent_alive`, `local_path_csi_node_report_age_seconds` | Whether the agent of a node reports, and the age of its last report. |
| `local_path_csi_node_volumes_without_agent` | Volumes on a node whose agent does not report. They cannot be created, resized, deleted or attached to a pod. |

## Alerts

The alerts are in [`chart/files/alerts.yaml`](../chart/files/alerts.yaml), a plain Prometheus rule
file. They fire when:

- the controller is gone;
- a node has volumes but no agent;
- a volume is lost or stays pending;
- a pool is not ready, almost fully allocated or low on space;
- a volume is almost full, in bytes or in files;
- an agent keeps failing or does not finish a pass;
- a volume does not get to the node of its pod.

## Events

Events are written on the `LocalPathVolume` and its claim, or on the `LocalPathNode`. Events about
a move are also written on the pods that wait for the volume.

| Reason | When |
|---|---|
| `PoolNotReady`, `PoolReady` | The state of a pool changes. The event says what to do. |
| `VolumeWaiting` | The agent cannot act on a volume: the pool is missing or not ready, `dirMode` is not octal, or the copy of a volume handed over to the node is not complete. |
| `VolumeLost` | The directory of a ready volume is gone. |
| `MovedToTrash` | A deleted volume, or the copy a volume left behind when it moved, is kept in the trash until the time in the event. |
| `VolumeRestored` | A volume was restored from its directory and waits for an administrator. |
| `PersistentVolumeRestored` | A restored volume was approved and got its PersistentVolume back. |
| `DeletedWithoutNode` | A volume of a node that left the cluster was deleted. Its data stays on the disk. |
| `VolumeMoving`, `VolumeMoved` | A volume moves to the node of its pod: the start, progress every 30 seconds, the arrival. |
| `VolumeMoveFailed` | A copy failed and will be retried, or the target node cannot take the volume: it does not fit, the pool is not ready, or the agent does not report. |
| `VolumeMoveCancelled` | The pod is gone or was scheduled elsewhere. The volume stays where it was. |
| `VolumeCopyKept` | A node has a copy of a volume that is not older than the one the cluster uses. The copy is left for a person to check. |
