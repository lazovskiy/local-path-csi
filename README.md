# local-path-csi

local-path-csi is a CSI driver for Kubernetes that stores each volume in a directory on a node,
like [local-path-provisioner](https://github.com/rancher/local-path-provisioner). It adds the
features that local-path-provisioner lacks:

- **Real size limits.** The size of a claim is enforced by an XFS project quota. The kernel
  applies the limit to every user, including root. `df` in the pod shows the size of the claim,
  not the size of the disk.
- **Usage metrics.** The kubelet reports `kubelet_volume_stats_*` for every volume. The driver
  also exports its own metrics for every volume and pool. It reads usage from the quota in one
  system call instead of walking the directory tree. The chart ships alerts and a Grafana
  dashboard. See [Monitoring](docs/monitoring.md).
- **Online expansion.** When you grow a claim, the driver raises the limit of the mounted volume.
- **Capacity-aware scheduling.** A new volume is placed only on a node whose pool has room.
- **fsGroup support.** The kubelet changes the group of the files when a pod sets a different
  `fsGroup`.
- **Volumes that follow their pod.** With `mobility: Floating`, a pod can start on another storage
  node. The driver copies the volume there first. `kubectl drain` moves the data together with
  the pods. See [Floating volumes](docs/floating-volumes.md).
- **Data is kept when in doubt.** A deleted volume can wait in a trash for a while. A volume
  directory that has lost its Kubernetes objects comes back as a volume instead of being removed.
  See [Volume lifecycle](docs/volume-lifecycle.md).

## Requirements

- **Kubernetes:** tested on 1.35. The target is the three latest minor versions.
- **Linux kernel:** 5.14 or newer on the storage nodes (the driver uses `quotactl_fd`).
- **A pool on every storage node.** A pool is a filesystem that you format and mount yourself.
  It must be mounted at the same path on every storage node.
  - For size limits, the filesystem must be XFS mounted with the `prjquota` option. You cannot add
    this option with a remount: unmount the filesystem first.
  - ext4 is not supported. Its quota does not limit a process with `CAP_SYS_RESOURCE`, such as
    root in a privileged container. The driver does not refuse an ext4 pool mounted with
    `prjquota`, but its limits do not hold for such processes.
  - The pool directory must exist when the agent pod starts.
- **A node label.** Add the label `local-path-csi.domik.fi/storage=` to the storage nodes. The
  agent runs only on these nodes.
- **Pod network access for Floating volumes.** The agent pods must reach each other on
  `transfer.port` (default `9754`).

The driver never formats a disk or mounts a pool. It checks each pool and refuses one that:

- is not the root of a mounted filesystem, or
- is mounted without project quota.

The reason is shown in `kubectl get localpathnodes -o yaml`. You can turn off both checks for a
pool to use a plain directory without size limits. If you mount the disk of a pool after the
agent has started, the agent picks it up without a restart.

## Install

1. Label the storage nodes:

   ```sh
   kubectl label node <node> local-path-csi.domik.fi/storage=
   ```

2. Create a `values.yaml` file with the path of the pool:

   ```yaml
   pools:
     - name: default
       path: /mnt/local-path-csi
   ```

3. Install the chart:

   ```sh
   helm install local-path-csi oci://ghcr.io/lazovskiy/charts/local-path-csi --version <version> \
     --namespace local-path-csi --create-namespace -f values.yaml
   ```

The available versions are listed on the
[releases](https://github.com/lazovskiy/local-path-csi/releases) page. The image is
`ghcr.io/lazovskiy/local-path-csi`, built for `linux/amd64` and `linux/arm64`. Each chart version
uses the image of the same version. To install from the sources, use `./chart` instead of the
`oci://` reference.

The agent runs as a privileged pod, so the namespace must allow privileged pods.

The chart creates two StorageClasses, `local-path-pinned` and `local-path-floating`. For all
values, the CRDs and the names you must choose at install, see the [chart README](chart/README.md).

## Documentation

- [Chart](chart/README.md): what the chart installs, the names you choose at install, all values.
- [Architecture](docs/architecture.md): components, custom resources, volume status, the layout
  on disk.
- [Floating volumes](docs/floating-volumes.md): how volumes move between nodes, and how the move
  is secured.
- [Volume lifecycle](docs/volume-lifecycle.md): deletion, the trash, and volumes that come back
  from a directory.
- [Monitoring](docs/monitoring.md): metrics, alerts and events.
- [Known issues](docs/known-issues.md)
- [Development](docs/development.md): tests, CI and releases.
