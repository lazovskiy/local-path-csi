# Floating volumes

A StorageClass sets whether its volumes stay on their node or follow their pod. This page
explains how a `Floating` volume moves between nodes and how the move is secured. For the values
mentioned here, see the [chart README](../chart/README.md#values).

## Pinned and Floating

| | `mobility: Pinned` (default) | `mobility: Floating` |
|---|---|---|
| The PersistentVolume is reachable from | its node, as with local-path-provisioner | every storage node |
| The pod is scheduled to another node | cannot happen: the pod waits for its node | the volume is copied there, then the pod starts |
| `kubectl drain` | copies nothing | moves the data together with the pods |

## What you see during a move

- The pod is down while the copy runs. A gigabit link copies about 100 MiB per second. Nothing is
  copied in advance.
- The pod shows `ContainerCreating`. Every two minutes the kubelet reports "timed out waiting for
  external-attacher". This is expected.
- Progress is reported in the `VolumeMoving` events of the pod, its claim and the
  `LocalPathVolume`, and in `status.transfer` of the `LocalPathVolume`.
- If you delete the pod, or it is scheduled elsewhere during the move, the move is cancelled. The
  volume stays where it was, and the unfinished copy is removed.
- If the node that holds the data is down, a pod on another node waits. No data is lost, and
  nothing needs to be cleaned up when the node returns.
- While a volume moves, it takes space in the pools of both nodes until it is handed over.

## How a move works

A move happens during the attach of the volume, the same way as for network disks:

1. The old pod is gone, and its node has released the volume.
2. The scheduler picks a new node for the pod.
3. The controller sends the volume to the new node: it sets `spec.node` of the `LocalPathVolume`.
4. The agent of the new node copies the data into `incoming/` with rsync. The copy keeps numeric
   owners, hard links, ACLs, extended attributes and sparse files.
5. The agent repeats rsync passes until a pass finds nothing left to transfer. Then it flushes the
   filesystem and reports the copy complete. If five passes in a row each find something new, the
   data keeps changing: the attempt fails and is retried after 15 seconds.
6. The controller hands the volume over: it sets `status.node`.
7. The agent turns the copy into the volume and sets its limit. The pod starts.
8. The old node moves its copy to the trash for `transfer.keepSource`. The copy is a way back. If
   the volume returns to this node, the next copy starts from it, so only the changed files
   travel. A changed file is sent whole.

The agent reads the generation of the `LocalPathVolume` before the first pass of an attempt. After
the flush it reads the volume again. If the generation has changed, for example because the move
was cancelled, the agent does not report the copy and goes over it again.

### Who drives the move

The move does not depend on a running attach call. One controller replica is the leader: it holds
the Lease `local-path-csi-controller` in the namespace of the release. The leader drives every
moving volume:

- it hands over a volume whose copy is complete;
- it cancels a move that no pod waits for.

It does this also while the attacher waits between two attempts, or when the controller replica
that the attacher called is gone. The attach call does the same for its own volume. Both write
only if nobody has changed the volume since they read it.

### Why the attach call blocks

`ControllerPublishVolume` blocks for the whole copy. The alternative is to return an error while
the data is moving, but each error is recorded in the VolumeAttachment, and the attach/detach
controller backs off exponentially after every failed attach. Measured on kind with Kubernetes
1.35 and a simulated move:

| Behavior | Result |
|---|---|
| Return an error every few seconds | the pod started 40 seconds after a 90-second move had finished |
| Block until the copy is done | the pod started 2 seconds after a 5-minute move had finished |

That is why the attacher gets a long `--timeout` (`controller.attachTimeout`), and why progress is
reported through events and the volume status, not through the attach error.

When a controller replica is told to stop, it answers the calls that wait. The attacher repeats
them to the replica that is left.

## Transfer settings

- A node receives `transfer.concurrency` volumes at a time. `transfer.bandwidthLimitKiB` limits
  the speed of each copy.
- The agent of the node that holds the data compresses it with zstd. rsync itself sends files
  uncompressed. To see how much compression saves, compare
  `local_path_csi_transfer_uncompressed_bytes_total` and
  `local_path_csi_transfer_compressed_bytes_total` on the receiving node.
- The default level is the fastest regular zstd level. If the CPU of a node is slower than its
  network, a negative `transfer.compressionLevel` or `transfer.compression: none` copies faster.

## Security

The agents talk to each other directly, over TLS between two agent pods:

- Each agent makes a key when it starts.
- Each side checks the other against the certificate in its `LocalPathNode`. A client whose
  certificate does not belong to an agent cannot complete the handshake.
- The agent with the data sends a volume only to the node that the `LocalPathVolume` names, and
  only while the volume is neither attached nor mounted.

So the agents trust the `LocalPathNode` and `LocalPathVolume` objects. All agents share one
service account, so RBAC alone would let any agent write the objects of every node. Two
ValidatingAdmissionPolicies (`admissionPolicy.enabled`) limit each agent to its own node. They
identify the node by the node name that the API server puts into the token of the pod. With the
policies, an agent:

- writes only the `LocalPathNode` of its own node;
- writes only volumes that are on its node or on their way to it;
- never changes where a volume must be (`spec`), where its data is (`status.node`) or which node
  uses it (`status.attachedNode`).

An agent that was taken over can neither pretend to be another agent nor have the volumes of
other nodes sent to itself. It also cannot approve a volume it restored from a directory, or
restore a volume without the mark that makes it wait for approval. See
[Volumes that come back](volume-lifecycle.md#volumes-that-come-back).

## Scheduling webhook

The scheduler does not know where the data of a `Floating` volume is. So the chart installs a
mutating webhook for new pods. It adds to the affinity of the pod:

- a preference for the node that holds the data of its volumes, so a restarted pod returns there
  and nothing is copied;
- an exclusion of the storage nodes that cannot take a volume: it does not fit, the pool is not
  ready, or the agent does not report.

This is only a hint, computed when the pod is created. If the webhook is down, pods are created as
usual. If the node of the pod cannot take a volume, the attach is refused with the
`VolumeMoveFailed` event.

The controller creates the webhook certificate itself, keeps it in the Secret
`local-path-csi-webhook` and writes it into the webhook configuration. Every replica rewrites the
configuration with its own certificate every 60 seconds. To replace the certificate, delete the
Secret and restart the controller. Until all old replicas have stopped, they can write the old
certificate back.
