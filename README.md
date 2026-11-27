# local-path-csi

A CSI driver for Kubernetes that keeps volumes in directories on the nodes, like
local-path-provisioner, and adds what that one lacks:

- **the size of a claim is a real limit** - XFS project quota, enforced by the kernel for every
  user including root; `df` in the pod shows the size of the claim, not of the disk;
- **usage metrics** - the kubelet reports `kubelet_volume_stats_*` for every volume, and the driver
  exports its own for every volume and pool, read from the quota in one syscall instead of walking
  the tree; alerts and a Grafana dashboard come with the chart;
- **online expansion** - growing a claim raises the limit of the mounted volume;
- **capacity-aware scheduling** - a new volume is placed only on a node whose pool has room;
- **fsGroup** - the kubelet changes the group of the files when a pod comes with another one;
- **volumes that follow their pod** - with `mobility: Floating` a pod may start on another storage
  node: its volume is copied there first, and `kubectl drain` evacuates the data with the pods;
- **data is not dropped on a doubt** - a deleted volume may wait in a trash, and a volume directory
  that lost its Kubernetes objects is brought back as a volume instead of being cleaned up.

## Requirements

- Kubernetes: tested on 1.35; the three latest minor versions are the target. Linux 5.14 or newer
  on the storage nodes (`quotactl_fd`).
- On every storage node, a pool: a filesystem you formatted and mounted, at the same path on all
  of them. For quotas it must be XFS mounted with `prjquota` (the option cannot be added by a
  remount, the filesystem has to be unmounted first). ext4 is not supported: it does not hold a
  process with `CAP_SYS_RESOURCE` - root in a privileged container - to the limit.
- The label `local-path-csi.domik.fi/storage=` on the storage nodes: the agent runs only there.
- For `Floating` volumes: the agent pods must reach each other on `transfer.port` (9754) over the
  pod network.

The driver never formats or mounts anything. A pool that is not the root of a mounted filesystem,
or is mounted without project quota, is refused with a reason in `kubectl get localpathnodes -o yaml`;
both checks can be switched off per pool to use a plain directory without limits. A pool whose
disk is mounted after the agent has started is picked up as the agent runs.

## Install

```sh
kubectl label node <node> local-path-csi.domik.fi/storage=
helm install local-path-csi oci://ghcr.io/lazovskiy/charts/local-path-csi --version <version> \
  --namespace local-path-csi --create-namespace -f values.yaml
```

```yaml
pools:
  - name: default
    path: /mnt/local-path-csi
```

The versions are the [releases](https://github.com/lazovskiy/local-path-csi/releases); the image
is `ghcr.io/lazovskiy/local-path-csi`, for linux/amd64 and linux/arm64, and the chart takes the
one of its own version. `./chart` in place of the `oci://` reference installs from the sources.

The chart declares the pool `default` without a path, so an install fails until the path is
given. The StorageClasses `local-path-pinned` and `local-path-floating` take their volumes from
that pool; neither is the default StorageClass. A `storageClasses` list in your values replaces
both, and a `pools` list replaces the pool.

The agent is privileged, so the namespace must allow privileged pods.

The CRDs are part of the release: `helm upgrade` updates them, and `helm uninstall` leaves them
and the `LocalPathVolume` objects in place (`helm.sh/resource-policy: keep`), because deleting a
`LocalPathVolume` removes its data.

### Names

The API group of the CRDs and the name of the driver are values: `apiGroup` and `driverName`.
Choose them at install and keep them. The volumes of a cluster are objects of the group it was
installed with, and every PersistentVolume names its driver, so a release that changes either no
longer sees the volumes it made.

The defaults were `local-csi.domik.fi` and `local.csi.domik.fi` before, with the resources
`LocalVolume` (`localvolumes`) and `LocalNode` (`localnodes`) and the node label
`local-csi.domik.fi/storage`. The chart, its objects and the binary were named `local-csi`, the
metrics `local_csi_*` and the alerts `LocalCsi*`. Nothing moves the objects of a cluster installed
that way to the new names, and dashboards and rules kept outside the chart need the new metric
names.

### Values

| Value | Meaning |
|---|---|
| `image.repository`, `image.tag` | Image of the driver; the tag defaults to the chart `appVersion` |
| `sidecars.*` | Images of the CSI sidecars (provisioner, attacher, resizer, registrar) |
| `kubeletDir` | Root directory of the kubelet, `/var/lib/kubelet` |
| `apiGroup` | API group of `LocalPathVolume` and `LocalPathNode` (default `local-path-csi.domik.fi`): the CRDs, the access rules and the driver follow it |
| `driverName` | Name of the CSI driver (default `local-path.csi.domik.fi`): the `CSIDriver`, the provisioner of the StorageClasses, the topology keys `<driverName>/node` and `<driverName>/storage` |
| `pools[].name`, `pools[].path` | A pool and its path on every storage node; the pool `default` is declared, its path must be given |
| `pools[].quota` | Limit volumes by project quota (default `true`); `false` makes a volume a plain directory whose size is only a reservation |
| `pools[].requireMountpoint` | Refuse a path that is not the root of a mounted filesystem (default `true`) |
| `pools[].overcommit` | Volume sizes may add up to the filesystem size times this (default `1.0`) |
| `projectIdBase` | Project ids of volumes start here (default `1000000`) |
| `storageClasses[]` | `name`, `pool`, `mobility` (`Pinned`), `default`, `reclaimPolicy` (`Delete`), `dirMode` (`0770`), `inodesPerGiB` (unlimited), `deleteGracePeriod` (`0`). Declared: `local-path-pinned` and `local-path-floating`, both of the pool `default`, neither the default StorageClass |
| `controller.replicas` | Replicas of the controller (default `2`); a pod with a volume cannot start while none is running |
| `controller.attachTimeout` | How long one attach may take (default `24h`): a `Floating` volume is attached when its data has arrived |
| `controller.attachWorkers` | Attaches handled at a time (default `100`); a volume on its way to another node holds one until it has arrived |
| `transfer.port` | Port the agents give volumes out on to each other (default `9754`) |
| `transfer.concurrency` | Volumes one node receives at a time (default `1`); the rest wait |
| `transfer.bandwidthLimitKiB` | KiB per second one copy may take, counted before compression (default `0`, no limit) |
| `transfer.compression` | How the data is compressed between the nodes: `zstd` (default) or `none` |
| `transfer.compressionLevel` | Level of zstd (default `1`): up to `19` is smaller and slower, `-1` to `-7` are faster and compress less |
| `transfer.keepSource` | How long the node a volume has left keeps its copy in the trash (default `24h`; `0` removes it at once) |
| `webhook.enabled` | The pod webhook: a new pod prefers the node that holds its `Floating` volumes and stays off the nodes they do not fit on (default `true`) |
| `webhook.failurePolicy`, `webhook.timeoutSeconds`, `webhook.excludeNamespaces` | `Ignore`: pods are created also when the webhook is down; namespaces whose pods it never sees (default `kube-system`, and always the namespace of the release) |
| `admissionPolicy.enabled` | ValidatingAdmissionPolicies that keep every agent to the `LocalPathNode` and the volumes of its own node, and from approving a volume it has brought back from a directory (default `true`) |
| `controller.agentTimeoutSeconds` | A node whose agent has not reported for this long takes no new volumes and no resizes (default `120`) |
| `agent.nodeSelector`, `agent.tolerations` | Where the agent runs |
| `metrics.port` | Port of `/metrics` of the controller and the agents (default `9753`); the pods carry `prometheus.io/scrape` annotations |
| `metrics.podMonitor.enabled`, `metrics.prometheusRule.enabled` | Create a PodMonitor and a PrometheusRule with the alerts; both need the prometheus-operator CRDs. `labels` adds labels your Prometheus selects by |
| `metrics.grafanaDashboard.enabled` | Create a ConfigMap with the dashboard for the Grafana dashboard sidecar (label `grafana_dashboard: "1"`) |

## How it works

```
Deployment local-path-csi-controller    local-path-csi controller + csi-provisioner, csi-attacher, csi-resizer
DaemonSet  local-path-csi-agent         local-path-csi agent + csi-node-driver-registrar, on storage nodes
```

The controller never touches a disk. For every volume it keeps a cluster-scoped `LocalPathVolume`
named after the PersistentVolume: the pool, the size, the node. The agent of that node makes it
true and reports back in the status; `LocalPathNode` objects carry the state and the size of the pools
of each node. `kubectl get localpathvolumes,localpathnodes` shows both.

How a volume is its agent says in `status.phase` (`Pending`, `Ready`, `Lost`, `Orphaned`) and, the
way the tools of Kubernetes read it, in the condition `Ready` of `status.conditions`:
`kubectl wait --for=condition=Ready localpathvolume/<pv>` waits for a volume. The `reason` of the
condition names in a word what `status.message` says in a sentence:

| `reason` | `status` | The volume |
|---|---|---|
| `Provisioned` | `True` | Its directory is there, with the limits of its spec |
| `PoolNotReady` | `False`, or `Unknown` for a volume that was there | Its pool is missing on the node or has failed its checks: the agent looks at nothing in it |
| `InvalidDirMode` | `False` | `dirMode` is not an octal mode |
| `CopyIncomplete` | `False` | It was handed over to this node, and its copy here was not completed |
| `DirectoryLost` | `False` | Its directory was there and is gone |
| `AwaitingApproval` | `False` | It was brought back from its directory and waits for an administrator: see [Volumes that come back](#volumes-that-come-back) |

`observedGeneration` of the condition is the `metadata.generation` of the volume the agent has
applied: after a resize the volume is ready again when the two are the same. A volume that was
just created, or handed over to another node, has no condition until the agent of its node has
written one.

A `LocalPathNode` is also the sign of life of its agent: the agent rewrites it every 30 seconds, and
the controller places nothing on a node whose report is older than `controller.agentTimeoutSeconds`.
Creating a volume there is refused (the scheduler picks another node), a resize is refused with the
reason, a deletion waits for the agent: only the agent can remove data. Pods that already run keep
their volumes. The controller measures the age of a report by its own clock, from the moment it
saw the report change: the clocks of the nodes need not agree. Only a node the controller sees for
the first time - after its own start - is as old as the time its agent wrote says. A
`LocalPathNode` is owned by its Node and disappears with it.

A volume on a node:

```
<pool>/volumes/<pv>/data         mounted into the pod; carries the project id and the limit
<pool>/volumes/<pv>/volume.json  passport: what the volume is, kept next to the data
<pool>/trash/<pv>.<expiry>/      a deleted volume, or the copy of one that moved away, kept until <expiry> (Unix seconds)
<pool>/incoming/<pv>/            the copy of a volume that is moving to this node
```

Only `ReadWriteOnce` and `ReadWriteOncePod` claims are accepted, and the StorageClass must use
`WaitForFirstConsumer`: the volume is created on the node the scheduler picked for its first pod.
A volume is a directory: a claim with `volumeMode: Block` is refused, and so is a StorageClass
with `mountOptions` - the filesystem of the pool is mounted by you, with the options you gave it.

### Volumes that follow their pod

A StorageClass says whether its volumes stay on their node:

| | `mobility: Pinned` (default) | `mobility: Floating` |
|---|---|---|
| The PersistentVolume is reachable from | its node, as with local-path-provisioner | every storage node |
| The pod is placed on another node | cannot happen: the pod waits for its node | the volume is copied there, then the pod starts |
| `kubectl drain` | copies nothing | evacuates the data with the pods |

A move happens inside the attach of the volume, the way it does for network disks: the old pod is
gone and its node has let the volume go, the scheduler has picked the new node, and only then the
controller sends the volume there (`spec.node` of the `LocalPathVolume`). The agent of the new node
fetches the data into `incoming/` with rsync (owners by number, hard links, ACLs, extended
attributes, sparse files), repeats the pass until one finds nothing left to transfer, flushes the
filesystem and reports the copy complete, for the generation of the `LocalPathVolume` it had read before
the last pass: a report about a move that was cancelled since does not count, and the copy is gone
over again. The controller then hands the volume over
(`status.node`), the agent makes the copy the volume and sets its limit, and the pod starts. The
node the volume left moves its copy to the trash for `transfer.keepSource`: a way back, and the
start of the next copy if the volume returns - then only what has changed travels.

The move does not depend on that attach being under way. Of the replicas of the controller one
leads (the Lease `local-path-csi-controller` in the namespace of the release), and it takes every
volume on its way further: it hands over a volume whose copy is complete and calls off a move
nobody waits for, also while the attacher waits between two attempts or the controller it had
called is gone. The call of the attacher does the same for its volume, and each of the two
writes on condition that nobody has changed the volume since it looked.

`ControllerPublishVolume` blocks for the whole copy. An error returned while the data is moving is
recorded in the VolumeAttachment, and the attach/detach controller backs off exponentially after
every failed attach. Measured on kind (Kubernetes 1.35) with a simulated move: with errors returned
every few seconds, a pod started 40 seconds after a 90-second move had finished; with a blocking
call, 2 seconds after a 5-minute move. That is why the attacher gets a long `--timeout`
(`controller.attachTimeout`), and why progress is reported through events and the volume status,
not through the attach error. A controller that is told to stop answers the calls that wait:
the attacher repeats them to the replica that is left.

- The pod is down for as long as the copy takes: a gigabit link carries about 100 MiB a second.
  Nothing is copied in advance.
- While its volume travels a pod shows `ContainerCreating` and, every two minutes, the kubelet's
  "timed out waiting for external-attacher". Progress is in the events of the pod, its claim and
  the `LocalPathVolume` (`VolumeMoving`), and in `status.transfer` of the `LocalPathVolume`.
- A pod that is deleted or placed elsewhere while its volume travels cancels the move: the volume
  stays where it was and the unfinished copy is removed.
- While the node that holds the data is down, a pod on another node waits; nothing is lost and
  nothing has to be cleaned up when the node returns.
- The agents talk to each other directly: a TLS connection between two agent pods, each side
  holding a key it made when it started and checking the other against the certificate in its
  `LocalPathNode`. A client whose certificate is not the one of some agent does not get past the
  handshake. The side with the data gives a volume only to the node the `LocalPathVolume` says it is
  sent to, and only while the volume is neither attached nor mounted.
- What the agents trust is therefore the `LocalPathNode` and `LocalPathVolume` objects. The agents share a
  service account, so RBAC alone would let any of them write those of every node; two
  ValidatingAdmissionPolicies (`admissionPolicy.enabled`) keep an agent to its own node by the
  node name the API server puts into the token of its pod. An agent writes the `LocalPathNode` of its
  node only, writes only to volumes that are on its node or on their way there, and never changes
  where a volume has to be (`spec`), where its data is (`status.node`) or which node uses it
  (`status.attachedNode`): an agent that was taken over can neither pass for another nor have the
  volumes of other nodes sent to itself. Nor does an agent approve a volume it brought back from
  a directory, or bring one back as anything but a volume that waits for that: see
  [Volumes that come back](#volumes-that-come-back).
- A node receives `transfer.concurrency` volumes at a time; `transfer.bandwidthLimitKiB` caps each.
- Between the agents the data is compressed with zstd, by the agent of the node that holds it:
  rsync itself sends files as they are. How much that saves shows in
  `local_path_csi_transfer_uncompressed_bytes_total` and `local_path_csi_transfer_compressed_bytes_total` of
  the receiving node. The default level is the fastest regular one; a node whose CPU is slower
  than its network copies faster with a negative `transfer.compressionLevel` or with
  `transfer.compression: none`.
- A volume on its way takes room in the pools of both nodes until it is handed over.

The scheduler knows nothing about where the data of a `Floating` volume is, so the chart installs
a mutating webhook for new pods. It adds to the affinity of a pod a preference for the node that
holds the data of its volumes (a restarted pod returns to it, and nothing is copied) and a ban on
the storage nodes where a volume would not fit. It is only advice, computed when the pod is
created: with the webhook down pods are created as usual, and a volume that does not fit on the
node of its pod is refused at the attach (`VolumeMoveFailed`). The controller makes the certificate
of the webhook itself, keeps it in the Secret `local-path-csi-webhook` and writes it into the webhook
configuration; to replace it, delete the Secret and restart the controller.

### Metrics, alerts and events

Agents read their values from the disk and the quota when they are scraped; the controller
answers from what its watches have seen of the custom resources.

| Metric | From | What |
|---|---|---|
| `local_path_csi_volume_size_bytes`, `local_path_csi_volume_used_bytes` | agent | Size and usage of a volume; labels `node`, `pool`, `persistentvolume`, `namespace`, `persistentvolumeclaim`. Usage is absent in a pool without quota |
| `local_path_csi_volume_inodes_used`, `local_path_csi_volume_inodes_limit` | agent | Files of a volume and their limit, when there is one |
| `local_path_csi_pool_ready` | agent | 1 for a pool that holds volumes; the label `state` names the reason otherwise |
| `local_path_csi_pool_capacity_bytes`, `_allocatable_bytes`, `_available_bytes` | agent | The filesystem of a pool: its size, what volume sizes may add up to, its free space |
| `local_path_csi_pool_trash_volumes`, `local_path_csi_pool_trash_bytes` | agent | The trash of a pool |
| `local_path_csi_agent_errors_total`, `local_path_csi_agent_volumes_total` | agent | Failures of the agent, and directories it removed, trashed, purged, brought back or received |
| `local_path_csi_agent_seconds_since_last_pass` | agent | Seconds since the agent went over the volumes of its node to the end; it does at least every 30 seconds. The node reports apart from that, so an agent that hangs in a pass looks alive everywhere else |
| `local_path_csi_transfers_running`, `local_path_csi_transfers_total` | agent | Volumes a node is receiving, and how many it has received (`result="completed"`) or failed to in an attempt (`result="failed"`) |
| `local_path_csi_transfer_seconds_total`, `local_path_csi_transfer_received_bytes_total` | agent | Time and size of the received volumes |
| `local_path_csi_transfer_uncompressed_bytes_total`, `local_path_csi_transfer_compressed_bytes_total` | agent | By `persistentvolume`, on the node that receives it: bytes rsync sent for the volume, and bytes that crossed the network for them. Kept while the volume is on the node or on its way to it |
| `local_path_csi_volume_moving`, `local_path_csi_volume_moving_received_bytes` | controller | A volume on its way: labels `persistentvolume`, `namespace`, `persistentvolumeclaim`, `source`, `target` and `state` (`Pending`, `Queued`, `Copying`, `Copied`, `Failed`) |
| `local_path_csi_volumes` | controller | Volumes by `node`, `pool`, `mobility` and `phase` |
| `local_path_csi_pool_allocated_bytes`, `local_path_csi_pool_free_bytes` | controller | Sizes of the volumes of a pool, and what the scheduler is told is left |
| `local_path_csi_node_agent_alive`, `local_path_csi_node_report_age_seconds` | controller | Whether the agent of a node reports |
| `local_path_csi_node_volumes_without_agent` | controller | Volumes on a node whose agent does not report: they cannot be created, resized, deleted or brought to a pod |

Every controller replica exports the same controller series: aggregate them with `max by (...)`.

The alerts are in `chart/files/alerts.yaml`, a plain Prometheus rule file: a controller that is
gone, volumes on a node without an agent, a lost or pending volume, a pool that is not ready,
almost allocated or low on space, a volume almost full (bytes or files), an agent that keeps
failing or does not come to the end of a pass, a volume that is not getting to the node of its pod.

Events are written on the `LocalPathVolume` and its claim (a move also on the pods that wait for the
volume), or on the `LocalPathNode`:

| Reason | When |
|---|---|
| `PoolNotReady`, `PoolReady` | The state of a pool, with what to do about it |
| `VolumeWaiting` | The agent cannot make the directory: the pool is not ready |
| `VolumeLost` | The directory of a ready volume is gone |
| `MovedToTrash` | A deleted volume is kept until the time in the event |
| `VolumeRestored`, `PersistentVolumeRestored` | A volume was brought back from its directory and waits for an administrator; approved, it got its PersistentVolume back |
| `DeletedWithoutNode` | A volume of a node that left the cluster was deleted, its data stayed on the disk |
| `VolumeMoving`, `VolumeMoved` | A volume travels to the node of its pod: the start, the progress every 30 seconds, the arrival |
| `VolumeMoveFailed` | A copy failed and is tried again, or the volume does not fit on the node |
| `VolumeMoveCancelled` | The pod is gone or was placed elsewhere: the volume stays where it was |
| `VolumeCopyKept` | A node has a copy of a volume that is not older than the one the cluster uses: it is left for a person to look at |

### Deleting a volume

- With `reclaimPolicy: Delete`, deleting the claim removes the data at once, or after
  `deleteGracePeriod` of the StorageClass (`30m`, `24h`, `7d`): until then the volume lies in the
  trash of its pool. The trash does not count as used capacity: when the filesystem has less free
  space than the volumes may still write, the oldest entries go early. A pool without quota does
  not know what its volumes have written: there the trash is kept until it expires.
- To take a volume back from the trash, move it on the node from `trash/<pv>.<expiry>` to
  `volumes/<pv>`.
- A volume whose node has left the cluster (the Node object is deleted) is deleted without waiting
  for its agent; the data stays on the disk of that node. Before it removes or trashes anything,
  an agent reads the volume from the API server again: a volume that was released while the node
  was away is not removed by what the agent remembers.

### Volumes that come back

A directory in `volumes/` that has a passport but no `LocalPathVolume` is an orphan: the node was away
when its volume was deleted, the directory was moved back from the trash, the object was removed
by force or lost with an etcd restore. The agent recreates the `LocalPathVolume`, in phase
`Orphaned`: it counts against its pool, its usage is measured, it can be deleted - and no pod is
given it.

Nothing is made out of nothing: without a directory no volume comes back, on any node. A claim
whose PersistentVolume is gone stays `Lost`, a pod whose `LocalPathVolume` is gone waits with
`FailedAttachVolume`, and a volume whose directory vanished is marked `Lost` and is not given an
empty one.

That a directory is the volume of its name, and whose volume it was, is the word of the node it
is on. Somebody who has taken a node over could put a directory of their own under the name of a
volume whose object was lost, and have the pods of its claim run on their data. So an orphan
waits for an administrator, whatever is left of it in the cluster - a claim that still names it,
a PersistentVolume that is still there:

```sh
kubectl get localpathvolume -l <group>/restore-pv          # the volumes that wait, and on which node
kubectl annotate localpathvolume <pv> <group>/restore-approved=true
```

(`<group>` is `apiGroup` of the chart; `status.message` of the volume says the same.) Look at
`spec.node` first: it is the node the directory was found on, and it should be the node the
volume was on. Approved, the volume is `Ready` again:

- its PersistentVolume is still there: the pods of its claim start, with the data they had;
- its PersistentVolume is gone: the controller recreates it, with `reclaimPolicy: Retain`,
  reserved for the name the volume has on record. A claim of that name and namespace binds to
  its old data - one that was there all along, or one that is created now, by hand or from the
  template of a StatefulSet. A volume with no claim on record is reserved for nobody.

An agent can neither write the annotation nor bring a volume back without the mark that makes
it wait: the policies of `admissionPolicy.enabled` see to that, and without them the approval is
no protection.

To remove such a volume for good, delete its PersistentVolume, if it has one, and then its
`LocalPathVolume`: deleting a `LocalPathVolume` is what removes the data (or moves it to the trash,
when the volume has a grace period).

### Limits of the limits

The quota guards against accidents, not against a hostile tenant: outside a user namespace the
owner of a file may move it to another project. A write past the limit fails with `ENOSPC`.

## Known issues

- **Two calls at the same moment may promise a pool more than it holds.** The controller checks
  the free space of a pool and records the volume under a lock of its own process, and
  csi-provisioner, csi-attacher and csi-resizer elect their leaders apart from each other. With
  `controller.replicas` above 1, a volume that is created and one that is resized or moved in may
  be handled by two replicas, and both pass the check. It takes two such calls for one pool within
  milliseconds and a pool that is nearly full; the pool is then promised more by the size of one
  volume. An agent does not check the sum. With `controller.replicas: 1` only a rollout is left,
  while the old pod and the new one run side by side.
- **A node whose clock is wrong is judged by that clock right after the controller starts.** The
  first report the controller sees of a node is as old as the time in it says. A node whose clock
  is more than `controller.agentTimeoutSeconds` behind is taken for dead until its agent reports
  again, 30 seconds at most, and no volume is created there meanwhile. A node whose clock is ahead
  and whose agent has died is taken for alive for that much longer.

## Development

| Command | What it checks |
|---|---|
| `cargo test` | Unit tests: the controller against an in-memory store, the decisions of the agent, the webhook, parsing; a volume carried between two agents by a real rsync (needs `rsync`, or its path in `LOCAL_PATH_CSI_RSYNC`); and the process the agent runs under, which collects what a copy that was given up leaves behind |
| `e2e/xfs-quota.sh` | Quota and volume directories on a real XFS (loop device in a privileged container) |
| `e2e/kind.sh` | The chart on a kind cluster with two storage nodes: provisioning, quota, fsGroup, kubelet metrics, expansion, capacity, deletion, the trash, a volume put back from it, a node that lost its agent, a `LocalPathVolume` lost while its PersistentVolume stays, a node that leaves the cluster and returns, metrics and events along the way; `Floating` volumes: a move after `cordon`, the way back, the webhook, a node without room, a cancelled move, an agent restarted and a node gone dark in the middle of a copy, the queue, a controller replica replaced |
| `e2e/alerts.sh` | The alerts with promtool: the rule file is valid and the alerts fire when they should |
| `API_GROUP=example.com DRIVER_NAME=path.csi.example.com e2e/kind.sh` | The same with the chart installed under other names than its defaults |
| `cargo run -- crds --out chart/files/crds` | Regenerates the CRDs from `src/api.rs`; `cargo test` fails while the chart carries stale ones |

The CSI bindings are generated at build time from `proto/csi.proto` (CSI spec v1.12.0).

### CI

GitHub Actions, `.github/workflows/`:

- **Every push to `main` and every pull request**: `cargo fmt`, `clippy -D warnings`, `cargo test`,
  `e2e/xfs-quota.sh`, `e2e/alerts.sh`, `helm lint`, `shellcheck`. `checks / lint` and
  `checks / unit` must pass for a pull request to be merged.
- **A pull request of this repository** also publishes an image `ghcr.io/lazovskiy/local-path-csi:prN`
  and a chart `oci://ghcr.io/lazovskiy/charts/local-path-csi` of version `X.Y.Z-prN`, whose image is
  that one; a comment in the pull request has the command to install it. Every push moves the tag
  `prN`: install with `image.pullPolicy: Always`. Closing the pull request deletes both.
- **The label `run-e2e`** on a pull request runs `e2e/kind.sh` on its image, with the default names
  and with other ones, on every push while the label is on.
- **A release**: set the version in `Cargo.toml` (and `Cargo.lock`, by `cargo check`) and in
  `version` and `appVersion` of `chart/Chart.yaml`, merge, and push a tag `vX.Y.Z` of a commit
  of `main` or of a `release-*` branch. The workflow refuses a tag that disagrees with those
  versions, runs the checks, pushes the image by its digest, runs e2e on it, and only then tags it
  `X.Y.Z` (and `latest` unless a higher version is out), publishes the chart and the GitHub release.
