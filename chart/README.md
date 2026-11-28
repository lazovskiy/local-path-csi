# local-path-csi Helm chart

This chart installs local-path-csi, a CSI driver that stores each volume in a directory on a
node. For what the driver does and what the nodes need, see the
[repository README](https://github.com/lazovskiy/local-path-csi#readme).

## Install

```sh
kubectl label node <node> local-path-csi.domik.fi/storage=
helm install local-path-csi oci://ghcr.io/lazovskiy/charts/local-path-csi --version <version> \
  --namespace local-path-csi --create-namespace -f values.yaml
```

The smallest `values.yaml` sets the path of the pool:

```yaml
pools:
  - name: default
    path: /mnt/local-path-csi
```

The agent runs as a privileged pod, so the namespace must allow privileged pods.

## What the chart creates by default

- **A pool named `default`, without a path.** The install fails until you set the path.
- **Two StorageClasses:** `local-path-pinned` and `local-path-floating`. Both take their volumes
  from the pool `default`. Neither is the default StorageClass of the cluster.

Helm replaces a list as a whole. If you set `pools` in your values, your list replaces the pool
`default`. If you set `storageClasses`, your list replaces both StorageClasses. This also applies
to `--set`: with `--set pools[0].path=...`, set `pools[0].name` as well.

## CRDs

The CRDs are part of the release:

- `helm upgrade` updates them.
- `helm uninstall` leaves the CRDs in place: they carry the annotation
  `helm.sh/resource-policy: keep`. So the `LocalPathVolume` objects stay too. This matters
  because deleting a `LocalPathVolume` deletes its data.

## Names

Two values set the names that the driver writes into the cluster:

- `apiGroup`: the API group of the CRDs.
- `driverName`: the name of the CSI driver.

Choose both at install and do not change them later. The volumes of a cluster are objects of
the API group the chart was installed with, and every PersistentVolume names its driver. A
release that changes either value no longer sees the volumes it created.

### Old names

Earlier versions used other default names:

| What | Old name | Current name |
|---|---|---|
| API group | `local-csi.domik.fi` | `local-path-csi.domik.fi` |
| Driver name | `local.csi.domik.fi` | `local-path.csi.domik.fi` |
| Resources | `LocalVolume` (`localvolumes`), `LocalNode` (`localnodes`) | `LocalPathVolume` (`localpathvolumes`), `LocalPathNode` (`localpathnodes`) |
| Node label | `local-csi.domik.fi/storage` | `local-path-csi.domik.fi/storage` |
| Chart, objects, binary | `local-csi` | `local-path-csi` |
| Metrics | `local_csi_*` | `local_path_csi_*` |
| Alerts | `LocalCsi*` | `LocalPathCsi*` |

Nothing moves the objects of a cluster installed with the old names to the new ones. Dashboards
and rules that you keep outside the chart need the new metric names.

## Values

### Image

| Value | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/lazovskiy/local-path-csi` | Image of the driver. |
| `image.tag` | `""` | Tag of the image. Empty means the `appVersion` of the chart. |
| `image.pullPolicy` | `IfNotPresent` | Pull policy of the driver image. |
| `imagePullSecrets` | `[]` | Pull secrets for the pods of the chart. |
| `sidecars.provisioner`, `sidecars.attacher`, `sidecars.resizer`, `sidecars.registrar` | images from `registry.k8s.io/sig-storage` | Images of the CSI sidecars. |

### Cluster

| Value | Default | Description |
|---|---|---|
| `namespaceOverride` | `""` | Namespace of everything the chart creates. Empty means the namespace of the release. Set it when the chart is a dependency of another chart and cannot have a namespace of its own. The chart does not create the namespace. |
| `kubeletDir` | `/var/lib/kubelet` | Root directory of the kubelet on the nodes. |
| `apiGroup` | `local-path-csi.domik.fi` | API group of `LocalPathVolume` and `LocalPathNode`. The CRDs, the access rules and the driver use it. See [Names](#names). |
| `driverName` | `local-path.csi.domik.fi` | Name of the CSI driver. It is used for the `CSIDriver` object, the provisioner of the StorageClasses and the topology keys `<driverName>/node` and `<driverName>/storage`. See [Names](#names). |

### Pools

| Value | Default | Description |
|---|---|---|
| `pools[].name` | required | Name of the pool. The pool declared by the chart is named `default`. |
| `pools[].path` | none, required | Path of the pool. It must be the same on every storage node. |
| `pools[].quota` | `true` | Limit volumes with a project quota. The filesystem must be mounted with `prjquota` (or `pquota`). With `false`, a volume is a plain directory, and its size is only a reservation. |
| `pools[].requireMountpoint` | `true` | Refuse a path that is not the root of a mounted filesystem. |
| `pools[].overcommit` | `1.0` | The sizes of all volumes in the pool may add up to the size of the filesystem times this number. |
| `projectIdBase` | `1000000` | First project ID for volumes. |

### StorageClasses

Each item of `storageClasses` creates one StorageClass. The chart declares `local-path-pinned`
(`mobility: Pinned`) and `local-path-floating` (`mobility: Floating`), both on the pool `default`.

| Field | Default | Description |
|---|---|---|
| `name` | required | Name of the StorageClass. |
| `pool` | required | Pool that the volumes are created in. |
| `mobility` | `Pinned` | `Pinned`: the volume stays on its node. `Floating`: the volume follows its pod. See [Floating volumes](https://github.com/lazovskiy/local-path-csi/blob/main/docs/floating-volumes.md). |
| `default` | `false` | Make this the default StorageClass of the cluster. |
| `reclaimPolicy` | `Delete` | Reclaim policy of the PersistentVolumes. |
| `dirMode` | `0770` | Permissions of the volume directory. |
| `inodesPerGiB` | `0` (unlimited) | Files and directories allowed per GiB of volume size. |
| `deleteGracePeriod` | `0` | How long a deleted volume stays in the trash of its pool, for example `30m`, `24h` or `7d`. See [Volume lifecycle](https://github.com/lazovskiy/local-path-csi/blob/main/docs/volume-lifecycle.md). |

### Controller

| Value | Default | Description |
|---|---|---|
| `controller.replicas` | `2` | Replicas of the controller. A pod with a volume cannot start while no replica is running. |
| `controller.attachTimeout` | `24h` | How long one attach may take. A `Floating` volume is attached only after its data has arrived on the node. |
| `controller.attachWorkers` | `100` | Attaches handled at the same time. A volume that is moving to another node holds one worker until it arrives. |
| `controller.agentTimeoutSeconds` | `120` | If the agent of a node has not reported for this long, the node gets no new volumes and no resizes. |
| `controller.resources`, `controller.nodeSelector`, `controller.tolerations` | empty | Resources and placement of the controller pods. |
| `controller.priorityClassName` | `system-cluster-critical` | Priority class of the controller pods. |

### Agent

| Value | Default | Description |
|---|---|---|
| `agent.nodeSelector` | `local-path-csi.domik.fi/storage: ""` | Nodes the agent runs on. Every one of them must have all the pools. |
| `agent.tolerations` | `[{operator: Exists}]` | Tolerations of the agent pods. |
| `agent.resources` | empty | Resources of the agent pods. |
| `agent.priorityClassName` | `system-node-critical` | Priority class of the agent pods. |

### Transfer of Floating volumes

| Value | Default | Description |
|---|---|---|
| `transfer.port` | `9754` | Port on which the agents send volumes to each other. The agent pods must reach each other on it. |
| `transfer.concurrency` | `1` | Volumes that one node receives at the same time. The rest wait. |
| `transfer.bandwidthLimitKiB` | `0` (no limit) | KiB per second that one copy may use, counted before compression. |
| `transfer.compression` | `zstd` | Compression of the data between the nodes: `zstd` or `none`. |
| `transfer.compressionLevel` | `1` | zstd level. Up to `19` compresses more and runs slower. `-1` to `-7` run faster and compress less. |
| `transfer.keepSource` | `24h` | How long the node that a volume has left keeps its copy in the trash. `0` removes the copy at once. |

### Webhook and admission policies

| Value | Default | Description |
|---|---|---|
| `webhook.enabled` | `true` | Mutating webhook for new pods. A pod prefers the node that holds its `Floating` volumes and avoids nodes where they do not fit. See [Floating volumes](https://github.com/lazovskiy/local-path-csi/blob/main/docs/floating-volumes.md#scheduling-webhook). |
| `webhook.port` | `9443` | Port of the webhook server in the controller. |
| `webhook.failurePolicy` | `Ignore` | With `Ignore`, pods are created even when the webhook is down. |
| `webhook.timeoutSeconds` | `5` | Timeout of a webhook call. |
| `webhook.excludeNamespaces` | `[kube-system]` | Namespaces whose pods the webhook never sees. The namespace of the driver is always excluded. |
| `admissionPolicy.enabled` | `true` | ValidatingAdmissionPolicies that limit each agent to its own `LocalPathNode` and to the volumes of its node, and stop it from approving a volume it has restored from a directory. Requires Kubernetes 1.32 or newer. See [Floating volumes](https://github.com/lazovskiy/local-path-csi/blob/main/docs/floating-volumes.md#security). |

### Metrics

| Value | Default | Description |
|---|---|---|
| `metrics.port` | `9753` | Port of `/metrics` on the controller and the agents. The pods carry `prometheus.io/scrape` annotations. |
| `metrics.podMonitor.enabled`, `metrics.podMonitor.labels` | `false`, `{}` | Create a PodMonitor. Requires the prometheus-operator CRDs: without them the chart skips the PodMonitor and the install still succeeds. Use `labels` for the labels your Prometheus selects by. |
| `metrics.prometheusRule.enabled`, `metrics.prometheusRule.labels` | `false`, `{}` | Create a PrometheusRule with the alerts. Requires the prometheus-operator CRDs: without them the chart skips the PrometheusRule and the install still succeeds. |
| `metrics.grafanaDashboard.enabled`, `metrics.grafanaDashboard.labels` | `false`, `grafana_dashboard: "1"` | Create a ConfigMap with the dashboard for the Grafana dashboard sidecar. |

For the metrics, alerts and dashboard, see [Monitoring](https://github.com/lazovskiy/local-path-csi/blob/main/docs/monitoring.md).
