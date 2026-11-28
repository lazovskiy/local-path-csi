# Architecture

This page describes the components of local-path-csi, its custom resources and how a volume is
stored on a node. For the values mentioned here, see the [chart README](../chart/README.md#values).

## Components

| Workload | Containers | Runs on |
|---|---|---|
| Deployment `local-path-csi-controller` | `local-path-csi` (the driver, started as `controller`), `csi-provisioner`, `csi-attacher`, `csi-resizer` | any node |
| DaemonSet `local-path-csi-agent` | `local-path-csi` (the driver, started as `agent`), `registrar` (csi-node-driver-registrar) | storage nodes |

The controller never touches a disk. It records what each volume should be, and the agent on the
node does the work.

## Custom resources

The driver uses two cluster-scoped custom resources:

- **`LocalPathVolume`**: one per volume, named after its PersistentVolume. The spec holds the
  pool, the size and the node. The agent of that node creates the volume and reports the result
  in the status.
- **`LocalPathNode`**: one per storage node. It holds the state and the size of each pool on the
  node.

To see both:

```sh
kubectl get localpathvolumes,localpathnodes
```

### Volume status

The agent reports the state of a volume in two places:

- `status.phase`: `Pending`, `Ready`, `Lost` or `Orphaned`.
- The `Ready` condition in `status.conditions`, in the form that Kubernetes tools understand.

To wait until a volume is ready:

```sh
kubectl wait --for=condition=Ready localpathvolume/<pv>
```

The `reason` of the condition is a one-word summary. `status.message` explains it in a sentence.

| `reason` | `status` | Meaning |
|---|---|---|
| `Provisioned` | `True` | The volume directory exists and has the limits from the spec. |
| `PoolNotReady` | `False` or `Unknown` | The pool is missing on the node or has failed its checks. The agent does not look inside it. |
| `InvalidDirMode` | `False` or `Unknown` | `dirMode` is not an octal mode. |
| `CopyIncomplete` | `False` or `Unknown` | The volume was handed over to this node, but its copy here is not complete. |
| `DirectoryLost` | `False` | The volume directory existed and is now gone. |
| `AwaitingApproval` | `False` | The volume was restored from its directory and waits for an administrator. See [Volumes that come back](volume-lifecycle.md#volumes-that-come-back). |

`PoolNotReady`, `InvalidDirMode` and `CopyIncomplete` mean that the agent could not act on the
volume this time. For a `Pending` volume the status is then `False`. For a volume in phase `Ready`,
`Orphaned` or `Lost`, the agent cannot tell whether that phase still holds, so the status is
`Unknown`.

`observedGeneration` of the condition is the `metadata.generation` that the agent saw when it
wrote the condition. The volume is ready for its current spec when the status is `True` and
`observedGeneration` equals `metadata.generation`. The generation changes, for example, after a
resize or when the volume is sent to another node.

A new volume, or a volume that was just handed over to another node, has no condition until the
agent of its node writes one.

### Agent heartbeat

A `LocalPathNode` also shows that its agent is alive. The agent rewrites it every 30 seconds. If
the report is older than `controller.agentTimeoutSeconds`, the controller treats the node as
having no agent:

- A new volume is refused, and the scheduler picks another node.
- A resize is refused with the reason.
- A deletion waits for the agent, because only the agent can remove data.
- Pods that already run keep their volumes.

The controller measures the age of a report with its own clock, from the moment it saw the report
change. The clocks of the nodes do not have to agree. There is one exception: when the controller
sees a node for the first time after it starts, it uses the time that the agent wrote into the
report. See [Known issues](known-issues.md).

A `LocalPathNode` is owned by its Node and is deleted together with it.

## Volumes on disk

Each pool has this layout on the node:

| Path | Content |
|---|---|
| `<pool>/volumes/<pv>/data` | The volume. It is mounted into the pod and carries the project ID and the limit. |
| `<pool>/volumes/<pv>/volume.json` | The passport: a description of the volume, kept next to the data. |
| `<pool>/trash/<pv>.<expiry>/` | A deleted volume, or the copy of a volume that moved to another node. It is kept until `<expiry>`, in Unix seconds. |
| `<pool>/incoming/<pv>/` | The copy of a volume that is moving to this node. |

## Supported claims and StorageClasses

- Only `ReadWriteOnce` and `ReadWriteOncePod` access modes are accepted.
- The StorageClass must use `volumeBindingMode: WaitForFirstConsumer`. The volume is created on
  the node that the scheduler picked for its first pod.
- A volume is a directory, so `volumeMode: Block` is refused.
- A claim of a StorageClass with `mountOptions` is refused. You mount the filesystem of the pool
  yourself, with your own options.

## Limits of the quota

- On XFS, a write past the limit fails with `ENOSPC`. ext4 reports `EDQUOT`.
- The quota protects against accidents, not against a hostile tenant. Outside a user namespace,
  the owner of a file can move it to another project.
