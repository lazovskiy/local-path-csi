# Volume lifecycle

This page explains what happens when a volume is deleted, how to get it back from the trash, and
what the driver does with a volume directory that has lost its Kubernetes objects.

## Deleting a volume

With `reclaimPolicy: Delete`, deleting the claim removes the data:

- at once, if the StorageClass has no `deleteGracePeriod`;
- after `deleteGracePeriod` (for example `30m`, `24h` or `7d`). Until then the volume stays in the
  trash of its pool.

The trash does not count as used capacity. If the filesystem has less free space than the volumes
may still write, trash entries are removed early, starting with the ones that expire first. A
pool without quota cannot know how much its volumes have written, so there the trash is kept
until it expires.

Before an agent removes a volume or moves it to the trash, it reads the volume from the API server
again. So a volume that was released while the node was out of the cluster is not removed because
of an old state that the agent remembers.

### Recover a volume from the trash

On the node, move the directory back:

```sh
mv <pool>/trash/<pv>.<expiry> <pool>/volumes/<pv>
```

This works for a deleted volume, whose `LocalPathVolume` is gone. The agent then brings it back
as described in [Volumes that come back](#volumes-that-come-back).

The trash also holds copies that volumes left behind when they moved to another node. Such a
volume still has its `LocalPathVolume`, so a copy moved back is not brought back as a volume. If
the copy is older than the volume the cluster uses, the agent moves it to the trash again.
Otherwise the agent leaves it in place and writes the `VolumeCopyKept` event.

### Volumes of a deleted node

Normally a deletion waits for the agent of the node, because only the agent can remove data. If
the node has left the cluster (its Node object is deleted), there is no agent to wait for. When
you delete a claim of such a volume, the deletion completes without the agent, and the data
stays on the disk of that node. If the node comes back, its agent finds the directory and brings
the volume back.

## Volumes that come back

An orphan is a directory in `volumes/` that has a passport but no `LocalPathVolume`. This can
happen when:

- the node had left the cluster when its volume was deleted, and came back;
- the directory was moved back from the trash;
- the `LocalPathVolume` was removed by force;
- the `LocalPathVolume` was lost with an etcd restore.

The agent recreates the `LocalPathVolume` in phase `Orphaned`. An orphaned volume:

- counts against its pool;
- has its usage measured;
- can be deleted;
- is not given to any pod.

The driver never creates a volume from nothing. Without a directory, no volume comes back, on any
node:

- A claim whose PersistentVolume is gone stays `Lost`.
- A pod whose `LocalPathVolume` is gone waits with `FailedAttachVolume`.
- A volume whose directory has disappeared is marked `Lost`. It does not get a new empty
  directory.

### Why an orphan needs approval

Only the node says that a directory is a given volume and whose volume it was. Someone who has
taken over a node could put their own directory under the name of a volume whose object was lost.
The pods of that claim would then run on their data. So an orphan always waits for an
administrator, even if its claim or its PersistentVolume still exists.

### Approve a restored volume

`<group>` below is the `apiGroup` of the chart. `status.message` of the volume names the
annotation to set.

1. List the volumes that wait for approval, with the nodes their directories were found on:

   ```sh
   kubectl get localpathvolume -l <group>/restore-pv \
     -o custom-columns=NAME:.metadata.name,NODE:.spec.node
   ```

   Use `spec.node`: the `Node` column of the default output shows `status.node`, which is empty
   for these volumes.

2. Check that `spec.node` is the node the volume was on.

3. Approve the volume:

   ```sh
   kubectl annotate localpathvolume <pv> <group>/restore-approved=true
   ```

After approval the volume is `Ready` again:

- **If its PersistentVolume still exists**, the pods of its claim start with the data they had.
- **If its PersistentVolume is gone**, the controller recreates it with `reclaimPolicy: Retain`.
  The new PersistentVolume is reserved for the claim name and namespace that the volume has on
  record. It also has the StorageClass, the size and the access mode on record. A claim with that
  name and namespace binds to the old data if it matches them. It can be a claim that existed all
  along, or one that you create now, by hand or from the template of a StatefulSet.
  A volume without a claim on record is not reserved for anyone.

An agent cannot write the approval annotation, and cannot restore a volume without the mark that
makes it wait. The admission policies (`admissionPolicy.enabled`) enforce this. Without them, the
approval gives no protection.

### Remove an orphan for good

1. Delete its PersistentVolume, if it has one.
2. Delete its `LocalPathVolume`.

Deleting a `LocalPathVolume` removes the data. If the volume has a grace period, the data goes to
the trash instead.
