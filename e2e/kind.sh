#!/usr/bin/env bash
# End-to-end test of the driver on a kind cluster: two workers, each with an XFS pool with project
# quota on a loop device, the chart installed with plain helm.
#
#   e2e/kind.sh             # build the image, create the cluster, test, delete the cluster
#   KEEP=1 e2e/kind.sh      # leave the cluster for a look around (kubeconfig path is printed)
#   DEBUG=1 e2e/kind.sh     # print objects, logs and events when a check fails
#
# Env: BUILDER (buildx builder for the image), PREBUILT (test this image instead of building one: a local
# image or a reference to pull), KIND_IMAGE (node image, selects the Kubernetes version), API_GROUP and
# DRIVER_NAME (install the chart with these instead of its defaults).
set -Eeuo pipefail

cd "$(dirname "$0")/.."
CLUSTER=local-path-csi-e2e
IMAGE=local-path-csi:e2e
WORK="$(mktemp -d -t local-path-csi-e2e.XXXXXX)"
export KUBECONFIG="$WORK/kubeconfig"
WORKERS=("$CLUSTER-worker" "$CLUSTER-worker2")
FAILED=0
# Given: passed to the chart. Not given: the chart is installed as it is, and has to come up with these.
NAMES=()
[ -z "${API_GROUP:-}" ] || NAMES+=(--set "apiGroup=$API_GROUP")
[ -z "${DRIVER_NAME:-}" ] || NAMES+=(--set "driverName=$DRIVER_NAME")
API_GROUP="${API_GROUP:-local-path-csi.domik.fi}"
DRIVER_NAME="${DRIVER_NAME:-local-path.csi.domik.fi}"

cleanup() {
  if [ "${KEEP:-0}" = 1 ]; then
    echo "cluster kept: export KUBECONFIG=$KUBECONFIG; delete with: kind delete cluster --name $CLUSTER"
    return
  fi
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; FAILED=1; }
# check <description> <command...>: the command must succeed.
check() { local what="$1"; shift; if "$@" >/dev/null 2>&1; then pass "$what"; else fail "$what"; fi; }
# eventually <seconds> <command...>: retries the command until it succeeds.
eventually() { local n="$1"; shift; for _ in $(seq "$n"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
node_sh() { docker exec "$1" sh -c "$2"; }
pod_sh() { kubectl exec "$1" -- sh -c "$2"; }
# jp <jsonpath> <kubectl get arguments...>
jp() { local path="$1"; shift; kubectl get "$@" -o "jsonpath=$path"; }
# is <operator> <expected> <command...>: compares the output of the command, run anew on every
# retry of `eventually`.
is() { local operator="$1" expected="$2"; shift 2; test "$("$@" 2>/dev/null)" "$operator" "$expected"; }
count() { "$@" | wc -l; }
# gone <kubectl get arguments...>: the object does not exist.
gone() { ! kubectl get "$@" >/dev/null 2>&1; }
# metric <controller|agent> <node of the agent, or ""> <series>: the value of a series, read from
# the metrics page of the pod through the API server.
metric() {
  local pod
  pod="$(kubectl -n local-path-csi get pods -l "app.kubernetes.io/component=$1" ${2:+--field-selector "spec.nodeName=$2"} -o jsonpath='{.items[0].metadata.name}')"
  kubectl get --raw "/api/v1/namespaces/local-path-csi/pods/$pod:9753/proxy/metrics" | awk -v series="$3" 'index($0, series " ") == 1 {print $2}'
}
# event <reason> <text>: an event with the reason mentions the text.
event() { kubectl get events -A --field-selector "reason=$1" -o jsonpath='{.items[*].message}' | grep -q -- "$2"; }
# readiness <volume>: the condition Ready of a LocalPathVolume, as status/reason.
readiness() { jp '{.status.conditions[?(@.type=="Ready")].status}/{.status.conditions[?(@.type=="Ready")].reason}' localpathvolume "$1"; }
# applied <volume>: the condition Ready is about the volume as its spec is now.
applied() { test "$(jp '{.metadata.generation}' localpathvolume "$1")" = "$(jp '{.status.conditions[?(@.type=="Ready")].observedGeneration}' localpathvolume "$1")"; }
# provisioned <volume>: the agent says that the volume is what its spec says now.
provisioned() { test "$(readiness "$1")" = True/Provisioned && applied "$1"; }

echo "== image"
if [ -n "${PREBUILT:-}" ]; then
  docker image inspect "$PREBUILT" >/dev/null 2>&1 || docker pull -q "$PREBUILT" >/dev/null
  docker tag "$PREBUILT" "$IMAGE"
else
  docker buildx build ${BUILDER:+--builder "$BUILDER"} --provenance=false --sbom=false -t "$IMAGE" --load . >"$WORK/build.log" 2>&1 \
    || { tail -30 "$WORK/build.log"; exit 1; }
fi

echo "== cluster"
mkdir "$WORK/disks"
# The filesystems are made outside the nodes: the node image has no mkfs.xfs.
docker run --rm -v "$WORK/disks:/disks" docker.io/library/alpine:3.22 sh -ec '
  apk add -q --no-progress xfsprogs >/dev/null
  for n in 1 2; do truncate -s 1G /disks/$n.img; mkfs.xfs -q /disks/$n.img; done'
cat >"$WORK/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
    extraMounts: [{hostPath: $WORK/disks/1.img, containerPath: /disk.img}]
  - role: worker
    extraMounts: [{hostPath: $WORK/disks/2.img, containerPath: /disk.img}]
YAML
kind create cluster --name "$CLUSTER" --config "$WORK/kind.yaml" ${KIND_IMAGE:+--image "$KIND_IMAGE"} --wait 120s >"$WORK/kind.log" 2>&1 \
  || { cat "$WORK/kind.log"; exit 1; }
kind load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null 2>&1
for node in "${WORKERS[@]}"; do
  # "-o loop" detaches the loop device when the mount goes away with the node container.
  node_sh "$node" '
    for i in $(seq 0 63); do [ -e /dev/loop$i ] || mknod /dev/loop$i b 7 $i; done
    mkdir -p /mnt/local-path-csi /var/lib/local-path-csi-plain
    mount -o loop,prjquota /disk.img /mnt/local-path-csi'
  kubectl label node "$node" local-path-csi.domik.fi/storage= >/dev/null
done
echo "kubernetes $(kubectl version -o json | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])'), kernel $(uname -r)"

echo "== install"
cat >"$WORK/values.yaml" <<YAML
image: {repository: local-path-csi, tag: e2e, pullPolicy: Never}
pools:
  - {name: default, path: /mnt/local-path-csi}
  - {name: plain, path: /var/lib/local-path-csi-plain, quota: false, requireMountpoint: false}
  - {name: absent, path: /mnt/local-path-csi/absent}
storageClasses:
  - {name: local, pool: default}
  - {name: local-plain, pool: plain, dirMode: "0777"}
  - {name: local-kept, pool: default, deleteGracePeriod: 1h}
  - {name: local-brief, pool: default, deleteGracePeriod: 5s}
  - {name: local-floating, pool: default, mobility: Floating}
# Slow enough for a copy to be caught in the middle: 10 MiB a second.
transfer:
  bandwidthLimitKiB: 10240
# On the control plane: a worker leaves the cluster in one of the scenarios.
controller:
  nodeSelector: {node-role.kubernetes.io/control-plane: ""}
  tolerations: [{operator: Exists}]
  # Short, so that the scenario of a node without an agent does not take minutes.
  agentTimeoutSeconds: 75
YAML
# The pool "absent" is a directory inside the first pool: present, but not a mountpoint.
for node in "${WORKERS[@]}"; do node_sh "$node" 'mkdir /mnt/local-path-csi/absent'; done
helm install local-path-csi chart --namespace local-path-csi --create-namespace -f "$WORK/values.yaml" "${NAMES[@]}" >/dev/null
check "the definitions are of the group of the install" kubectl get "crd/localpathvolumes.$API_GROUP" "crd/localpathnodes.$API_GROUP"
check "the driver is registered under its name" kubectl get "csidriver/$DRIVER_NAME"
kubectl -n local-path-csi rollout status deployment/local-path-csi-controller --timeout=240s >/dev/null
kubectl -n local-path-csi rollout status daemonset/local-path-csi-agent --timeout=240s >/dev/null

echo "== pools"
pool_state() { jp "{.status.pools[?(@.name==\"$2\")].state}" localpathnode "$1"; }
for node in "${WORKERS[@]}"; do
  check "$node: pool on XFS with project quota is Ready" eventually 30 is = Ready pool_state "$node" default
  check "$node: pool without quota on the root filesystem is Ready when both checks are off" is = Ready pool_state "$node" plain
  check "$node: directory that is not a mountpoint is refused" is = NotMountpoint pool_state "$node" absent
done
for node in "${WORKERS[@]}"; do
  check "$node: the agent exports a ready pool as 1" is = 1 metric agent "$node" "local_path_csi_pool_ready{node=\"$node\",pool=\"default\",state=\"Ready\"}"
  check "$node: and a refused one as 0, with the reason" is = "0" metric agent "$node" "local_path_csi_pool_ready{node=\"$node\",pool=\"absent\",state=\"NotMountpoint\"}"
  check "$node: a refused pool has nothing to allocate" is = 0 metric agent "$node" "local_path_csi_pool_allocatable_bytes{node=\"$node\",pool=\"absent\"}"
done
check "a refused pool is explained in an event" eventually 30 event PoolNotReady "requireMountpoint: false"
check "control plane runs no agent" is = 2 count kubectl -n local-path-csi get pods -l app.kubernetes.io/component=agent -o name
check "capacity of every node is published for the scheduler" eventually 90 is -ge 4 count kubectl -n local-path-csi get csistoragecapacities -o name

echo "== volume with quota"
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: data}
spec: {accessModes: [ReadWriteOnce], storageClassName: local, resources: {requests: {storage: 100Mi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: app}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsUser: 1000, runAsGroup: 1000, fsGroup: 2000}
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: data}}]
YAML
check "pod with a volume starts" kubectl wait pod/app --for=condition=Ready --timeout=180s
PV="$(jp '{.spec.volumeName}' pvc data)"
NODE="$(jp '{.spec.nodeName}' pod app)"
check "PersistentVolume is bound to the node of the volume" \
  test "$(jp '{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0]}' pv "$PV")" = "$NODE"
check "LocalPathVolume is Ready on that node" test "$(jp '{.status.phase}/{.status.node}/{.status.attachedNode}' localpathvolume "$PV")" = "Ready/$NODE/$NODE"
check "kubectl wait takes its condition for it" kubectl wait --for=condition=Ready "localpathvolume/$PV" --timeout=30s
check "the condition names its reason, and the spec it is about" provisioned "$PV"
check "LocalPathVolume records its claim" test "$(jp '{.spec.claim.namespace}/{.spec.claim.name}' localpathvolume "$PV")" = default/data
check "the pod sees the size of its claim, not of the pool" test "$(pod_sh app 'df -k /data | tail -1' | awk '{print $2}')" = 102400
check "a non-root pod writes through fsGroup" pod_sh app 'echo payload > /data/file && test "$(stat -c %g /data/file)" = 2000'
check "the volume directory is closed to others (0770 + setgid from the kubelet)" test "$(pod_sh app 'stat -c %a /data')" = 2770
check "a write past the size fails" sh -c '! kubectl exec app -- dd if=/dev/zero of=/data/big bs=1M count=200'
USED="$(pod_sh app 'du -sk /data' | cut -f1)"
check "and stops at the size (${USED}K of 102400K)" test "$USED" -le 102400 -a "$USED" -ge 100000
volume_stat() {
  kubectl get --raw "/api/v1/nodes/$NODE/proxy/stats/summary" | python3 -I -c '
import json, sys
for pod in json.load(sys.stdin)["pods"]:
    for volume in pod.get("volume", []):
        if volume.get("pvcRef", {}).get("name") == "data":
            print(volume[sys.argv[1]])' "$1"
}
check "the kubelet reports the capacity of the volume" eventually 120 is = 104857600 volume_stat capacityBytes
check "the kubelet reports its usage" eventually 120 is -ge 100000000 volume_stat usedBytes
check "the LocalPathVolume shows the usage" eventually 60 is -ge 100000000 jp '{.status.usedBytes}' localpathvolume "$PV"
SERIES="{node=\"$NODE\",pool=\"default\",persistentvolume=\"$PV\",namespace=\"default\",persistentvolumeclaim=\"data\"}"
check "the agent exports the size of the volume" is = 104857600 metric agent "$NODE" "local_path_csi_volume_size_bytes$SERIES"
check "and its usage, as it is at the scrape" is -ge 100000000 metric agent "$NODE" "local_path_csi_volume_used_bytes$SERIES"
check "the controller counts the volume" eventually 30 is = 1 metric controller "" "local_path_csi_volumes{node=\"$NODE\",pool=\"default\",mobility=\"Pinned\",phase=\"Ready\"}"
check "and what it takes of the pool" is = 104857600 metric controller "" "local_path_csi_pool_allocated_bytes{node=\"$NODE\",pool=\"default\"}"
check "the directory is where the layout says" node_sh "$NODE" "test -f /mnt/local-path-csi/volumes/$PV/data/file -a -f /mnt/local-path-csi/volumes/$PV/volume.json"

echo "== agent restart"
kubectl -n local-path-csi delete pod -l app.kubernetes.io/component=agent --wait=true >/dev/null
kubectl -n local-path-csi rollout status daemonset/local-path-csi-agent --timeout=120s >/dev/null
check "the mounted volume survives a restart of the agent" pod_sh app 'test "$(cat /data/file)" = payload && rm /data/big && echo more > /data/file2'
# The agent is not the first process of its container: the signal that stops the container has
# to be passed on to it.
STOPPED="$(kubectl -n local-path-csi get pods -l app.kubernetes.io/component=agent --field-selector "spec.nodeName=$NODE" -o jsonpath='{.items[0].metadata.name}')"
kubectl -n local-path-csi logs -f "$STOPPED" -c local-path-csi >"$WORK/stopped.log" 2>&1 &
kubectl -n local-path-csi delete pod "$STOPPED" --wait=true >/dev/null
wait "$!" || true
check "an agent that is told to stop hears it, and stops in its own way" grep -q "shutting down" "$WORK/stopped.log"
kubectl -n local-path-csi rollout status daemonset/local-path-csi-agent --timeout=120s >/dev/null

echo "== expansion"
kubectl patch pvc data --type=merge -p '{"spec":{"resources":{"requests":{"storage":"300Mi"}}}}' >/dev/null
check "the claim grows" eventually 120 is = 300Mi jp '{.status.capacity.storage}' pvc data
check "the mounted volume grows in place" eventually 30 is = 307200 pod_sh app "df -k /data | tail -1 | awk '{print \$2}'"
check "the condition of the volume is about its new size" eventually 30 applied "$PV"
check "and takes more data" pod_sh app 'dd if=/dev/zero of=/data/big bs=1M count=200'

echo "== claim for one pod"
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: single}
spec: {accessModes: [ReadWriteOncePod], storageClassName: local, resources: {requests: {storage: 10Mi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: single}
spec:
  terminationGracePeriodSeconds: 1
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: single}}]
YAML
check "pod with a ReadWriteOncePod claim starts" kubectl wait pod/single --for=condition=Ready --timeout=120s
check "its LocalPathVolume records the access mode of the claim" is = ReadWriteOncePod jp '{.spec.accessMode}' localpathvolume "$(jp '{.spec.volumeName}' pvc single)"
check "and the one of a ReadWriteOnce claim records that" is = ReadWriteOnce jp '{.spec.accessMode}' localpathvolume "$PV"
kubectl delete pod single --wait=false >/dev/null; kubectl delete pvc single --wait=false >/dev/null

echo "== what does not fit and what is not supported"
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: huge}
spec: {accessModes: [ReadWriteOnce], storageClassName: local, resources: {requests: {storage: 10Gi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: huge}
spec:
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: huge}}]
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: shared}
spec: {accessModes: [ReadWriteMany], storageClassName: local, resources: {requests: {storage: 10Mi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: shared}
spec:
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: shared}}]
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: raw}
spec: {accessModes: [ReadWriteOnce], volumeMode: Block, storageClassName: local, resources: {requests: {storage: 10Mi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: raw}
spec:
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeDevices: [{name: data, devicePath: /dev/data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: raw}}]
YAML
check "a volume larger than every pool keeps its pod unscheduled" eventually 60 sh -c "kubectl get events --field-selector involvedObject.name=huge -o jsonpath='{.items[*].message}' | grep -q 'enough free storage'"
check "and creates nothing" test "$(jp '{.status.phase}' pvc huge)" = Pending
check "ReadWriteMany is refused with a reason" eventually 60 sh -c "kubectl get events --field-selector involvedObject.name=shared -o jsonpath='{.items[*].message}' | grep -q 'not supported'"
check "a block volume is refused with a reason" eventually 60 sh -c "kubectl get events --field-selector involvedObject.name=raw -o jsonpath='{.items[*].message}' | grep -q 'volumeMode Block is not supported'"
check "and creates nothing" test "$(jp '{.status.phase}' pvc raw)" = Pending
kubectl delete pod huge shared raw --wait=false >/dev/null; kubectl delete pvc huge shared raw --wait=false >/dev/null

echo "== pool without quota"
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: plain}
spec: {accessModes: [ReadWriteOnce], storageClassName: local-plain, resources: {requests: {storage: 50Mi}}}
---
apiVersion: v1
kind: Pod
metadata: {name: plain}
spec:
  terminationGracePeriodSeconds: 1
  securityContext: {runAsUser: 1000, runAsGroup: 1000}
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: plain}}]
YAML
check "pod with a volume without quota starts" kubectl wait pod/plain --for=condition=Ready --timeout=120s
check "dirMode 0777 lets a pod without fsGroup write" pod_sh plain 'echo x > /data/file && test "$(stat -c %a /data)" = 777'
check "the volume is not limited: it shows the whole filesystem" test "$(pod_sh plain 'df -k /data | tail -1' | awk '{print $2}')" -gt 1048576

echo "== deletion"
PLAIN_PV="$(jp '{.spec.volumeName}' pvc plain)"; PLAIN_NODE="$(jp '{.spec.nodeName}' pod plain)"
kubectl delete pod app plain --wait=true >/dev/null
kubectl delete pvc data plain --wait=true >/dev/null
check "the PersistentVolume of the volume with quota is gone" eventually 120 gone pv "$PV"
check "and of the volume without quota" eventually 120 gone pv "$PLAIN_PV"
# A LocalPathVolume goes only after its agent has removed the data: once it is gone, so is the directory.
check "the LocalPathVolume of the volume with quota is gone" eventually 30 gone localpathvolume "$PV"
check "with its data" node_sh "$NODE" "test ! -e /mnt/local-path-csi/volumes/$PV"
check "the LocalPathVolume of the volume without quota is gone" eventually 30 gone localpathvolume "$PLAIN_PV"
check "with its data" node_sh "$PLAIN_NODE" "test ! -e /var/lib/local-path-csi-plain/volumes/$PLAIN_PV"
check "the node gets its capacity back" eventually 150 sh -c "kubectl -n local-path-csi get csistoragecapacities -o jsonpath='{range .items[*]}{.nodeTopology.matchLabels}{\" \"}{.storageClassName}{\" \"}{.capacity}{\"\\n\"}{end}' | grep '$NODE' | grep ' local ' | grep -vq '^.* 0\$'"

# claim_and_pod <name> <storage class> [node] [size]: a claim (100Mi unless told) and a pod that
# mounts it at /data, on the node or wherever the scheduler puts it.
# claim_and_pod <name> <class> [node of the pod] [size] [volume the claim asks for by name]
claim_and_pod() {
  local selector=""
  [ -z "${3:-}" ] || selector="nodeSelector: {kubernetes.io/hostname: $3}"
  kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1}
spec: {accessModes: [ReadWriteOnce], storageClassName: $2, ${5:+volumeName: $5, }resources: {requests: {storage: ${4:-100Mi}}}}
---
apiVersion: v1
kind: Pod
metadata: {name: $1}
spec:
  terminationGracePeriodSeconds: 1
  $selector
  securityContext: {runAsUser: 1000, runAsGroup: 1000, fsGroup: 2000}
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: $1}}]
YAML
}

echo "== deleted volume with a grace period"
claim_and_pod kept local-kept
check "pod with a volume of the kept class starts" kubectl wait pod/kept --for=condition=Ready --timeout=180s
pod_sh kept 'echo precious > /data/file'
PV="$(jp '{.spec.volumeName}' pvc kept)"; NODE="$(jp '{.spec.nodeName}' pod kept)"
kubectl delete pod kept --wait=true >/dev/null; kubectl delete pvc kept --wait=true >/dev/null
check "the PersistentVolume and the LocalPathVolume are gone" eventually 120 gone pv "$PV"
check "and so is the LocalPathVolume" eventually 30 gone localpathvolume "$PV"
check "the data waits in the trash of the pool" node_sh "$NODE" "test ! -e /mnt/local-path-csi/volumes/$PV && grep -q precious /mnt/local-path-csi/trash/$PV.*/data/file"
check "the trash is counted" eventually 30 is = 1 metric agent "$NODE" "local_path_csi_pool_trash_volumes{node=\"$NODE\",pool=\"default\"}"
check "an event tells until when the data is kept" eventually 30 event MovedToTrash "is kept in the trash of pool default on node $NODE until"

echo "== volume put back from the trash"
node_sh "$NODE" "mv /mnt/local-path-csi/trash/$PV.* /mnt/local-path-csi/volumes/$PV"
check "the agent brings the LocalPathVolume back, as an orphan" eventually 90 is = "Orphaned/$NODE" jp '{.status.phase}/{.spec.node}' localpathvolume "$PV"
check "its condition says that it is not ready, and what for" eventually 30 is = False/AwaitingApproval readiness "$PV"
check "which says what it waits for, and whose it says it was" eventually 30 sh -c "kubectl get localpathvolume $PV -o jsonpath='{.status.message}' | grep 'the volume of claim default/kept' | grep -q 'annotating the volume $API_GROUP/restore-approved=true'"
# Long enough for the controller to have gone over the volumes that were brought back twice.
check "what its directory says of its claim gets it no PersistentVolume" sh -c "sleep 25; ! kubectl get pv $PV"
kubectl annotate localpathvolume "$PV" "$API_GROUP/restore-approved=true" >/dev/null
check "approved by an administrator, it gets its PersistentVolume back, keeping its data on release" eventually 60 is = Retain jp '{.spec.persistentVolumeReclaimPolicy}' pv "$PV"
check "it is reserved for the claim it had" is = default/kept/local-kept jp '{.spec.claimRef.namespace}/{.spec.claimRef.name}/{.spec.storageClassName}' pv "$PV"
check "both are told in events" eventually 30 sh -c "kubectl get events -A --field-selector reason=VolumeRestored -o name | grep -q . && kubectl get events -A --field-selector reason=PersistentVolumeRestored -o name | grep -q ."
check "the restore mark is cleared" eventually 30 is = "" jp "{.metadata.labels.${API_GROUP//./\\.}/restore-pv}" localpathvolume "$PV"
check "and the volume is an orphan no more" eventually 60 is = Ready jp '{.status.phase}' localpathvolume "$PV"
check "nor by its condition" eventually 30 is = True/Provisioned readiness "$PV"
claim_and_pod kept local-kept
check "a claim of the same name gets the volume" eventually 60 is = "$PV" jp '{.spec.volumeName}' pvc kept
check "and its pod finds the data" sh -c "kubectl wait pod/kept --for=condition=Ready --timeout=180s && test \"\$(kubectl exec kept -- cat /data/file)\" = precious"
kubectl delete pod kept --wait=true >/dev/null; kubectl delete pvc kept --wait=true >/dev/null
check "the released PersistentVolume keeps the volume" sh -c "sleep 5; test \"\$(kubectl get localpathvolume $PV -o jsonpath='{.status.phase}')\" = Ready"
kubectl delete pv "$PV" --wait=true >/dev/null
kubectl delete localpathvolume "$PV" --wait=false >/dev/null
check "deleting the LocalPathVolume itself is the way to remove it (to the trash again: its class says so)" eventually 60 gone localpathvolume "$PV"

echo "== expired trash"
claim_and_pod brief local-brief
kubectl wait pod/brief --for=condition=Ready --timeout=180s >/dev/null
PV="$(jp '{.spec.volumeName}' pvc brief)"; NODE="$(jp '{.spec.nodeName}' pod brief)"
kubectl delete pod brief --wait=true >/dev/null; kubectl delete pvc brief --wait=true >/dev/null
check "a volume with a short grace period is deleted" eventually 120 gone pv "$PV"
check "and leaves the trash when the period is over" eventually 90 node_sh "$NODE" "! ls -d /mnt/local-path-csi/trash/$PV.* 2>/dev/null | grep -q ."

echo "== volume that follows its pod"
A="${WORKERS[0]}"; B="${WORKERS[1]}"
# fingerprint <pod>: names, modes, owners, times, link counts and contents of everything in /data.
fingerprint() {
  kubectl exec "$1" -- sh -c 'cd /data && { find . | sort | while read -r f; do stat -c "%n %a %u %g %Y %h %F" "$f"; done
    find . -type f | sort | while read -r f; do stat -c "%s" "$f"; md5sum "$f"; done; }' | md5sum | cut -d" " -f1
}
lv() { jp "$1" localpathvolume "$2"; }
agent_of() { kubectl -n local-path-csi get pods -l app.kubernetes.io/component=agent --field-selector "spec.nodeName=$1" -o jsonpath='{.items[0].metadata.name}'; }
claim_and_pod roam local-floating "$A"
check "pod with a floating volume starts" kubectl wait pod/roam --for=condition=Ready --timeout=180s
PV="$(jp '{.spec.volumeName}' pvc roam)"
check "its PersistentVolume is reachable from every storage node, not bound to one" \
  is = "$DRIVER_NAME/storage" jp '{.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].key}' pv "$PV"
# The modes are ones the kubelet leaves alone: with fsGroup it gives the group read and write on
# every file (and setgid and search on every directory) each time a pod mounts the volume.
pod_sh roam 'umask 002 && cd /data && echo payload > file && mkdir -p dir/sub && dd if=/dev/urandom of=dir/blob bs=1M count=60 2>/dev/null &&
  ln -s ../file dir/link && ln file dir/hard && chmod 2771 dir/sub && chmod 661 file && touch -d "2020-01-02 03:04:05" old'
BEFORE="$(fingerprint roam)"
kubectl delete pod roam --wait=true >/dev/null

claim_and_pod roam local-floating
check "the webhook makes a new pod prefer the node that holds the data" \
  is = "In/$A" jp '{.spec.affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].preference.matchFields[0].operator}/{.spec.affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution[0].preference.matchFields[0].values[0]}' pod roam
check "so the pod comes back to that node" sh -c "kubectl wait pod/roam --for=condition=Ready --timeout=120s && test \"\$(kubectl get pod roam -o jsonpath='{.spec.nodeName}')\" = $A"
check "and nothing travels" is = "$A/" lv '{.status.node}/{.status.transfer.state}' "$PV"
check "the data is as it was left" is = "$BEFORE" fingerprint roam
kubectl delete pod roam --wait=true >/dev/null

kubectl cordon "$A" >/dev/null
claim_and_pod roam local-floating
check "with its node closed the pod goes to another node and starts there" kubectl wait pod/roam --for=condition=Ready --timeout=300s
check "on the other node" is = "$B" jp '{.spec.nodeName}' pod roam
check "the volume has followed it: the data is there, after one move" is = "$B/$B/$B/1/Ready" lv '{.spec.node}/{.status.node}/{.status.attachedNode}/{.status.moves}/{.status.phase}' "$PV"
check "the node that has it now says that it is ready" provisioned "$PV"
check "and it is that node that says so" is = "$B" lv '{.status.reportedBy}' "$PV"
check "every file arrived as it was: contents, owners, modes, times, links" is = "$BEFORE" fingerprint roam
check "the pod sees the size of its claim on the new node" is = 102400 pod_sh roam "df -k /data | tail -1 | awk '{print \$2}'"
check "and the limit holds there" sh -c '! kubectl exec roam -- dd if=/dev/zero of=/data/big bs=1M count=200'
pod_sh roam 'rm -f /data/big'
check "the node the volume left keeps its copy in the trash" node_sh "$A" "test ! -e /mnt/local-path-csi/volumes/$PV && grep -q payload /mnt/local-path-csi/trash/$PV.*/data/file"
check "the node that received it keeps nothing half-done" node_sh "$B" "test -d /mnt/local-path-csi/volumes/$PV/data && test -z \"\$(ls /mnt/local-path-csi/incoming)\""
check "the move is told in events" sh -c "kubectl get events --field-selector reason=VolumeMoving -o name | grep -q . && kubectl get events --field-selector reason=VolumeMoved -o name | grep -q ."
check "kubectl describe shows them on the pod that waited" sh -c "kubectl describe pod roam | grep -q VolumeMoving"
check "and on the claim" sh -c "kubectl describe pvc roam | grep -q VolumeMoved"
check "the old copy is told as well" eventually 30 event MovedToTrash "the copy it left in pool default on node $A is kept in the trash until"
check "the receiving agent counts the volume" eventually 30 is = 1 metric agent "$B" "local_path_csi_transfers_total{node=\"$B\",result=\"completed\"}"
check "the controller shows it on its new node" eventually 30 is = 1 metric controller "" "local_path_csi_volumes{node=\"$B\",pool=\"default\",mobility=\"Floating\",phase=\"Ready\"}"
kubectl uncordon "$A" >/dev/null

echo "== volume that goes back"
pod_sh roam 'umask 002 && cd /data && echo more > added && rm dir/blob && echo changed > file'
CHANGED="$(fingerprint roam)"
kubectl delete pod roam --wait=true >/dev/null
claim_and_pod roam local-floating "$A"
check "a pod on the former node gets the volume back" kubectl wait pod/roam --for=condition=Ready --timeout=300s
check "with what was changed meanwhile" is = "$CHANGED" fingerprint roam
check "after the second move" is = "$A/2" lv '{.status.node}/{.status.moves}' "$PV"
check "the copy the node had kept became the volume, not a second copy" node_sh "$A" "! ls -d /mnt/local-path-csi/trash/$PV.* 2>/dev/null | grep -q ."
check "and the other node keeps its copy now" eventually 60 node_sh "$B" "test ! -e /mnt/local-path-csi/volumes/$PV && test -f /mnt/local-path-csi/trash/$PV.*/data/added"

echo "== data is compressed on its way"
claim_and_pod squeeze local-floating "$A"
kubectl wait pod/squeeze --for=condition=Ready --timeout=180s >/dev/null
# Zeros: what rsync sends for them is at least their size, and nothing compresses better.
pod_sh squeeze 'umask 002 && dd if=/dev/zero of=/data/zeros bs=1M count=50 2>/dev/null'
ZEROS="$(fingerprint squeeze)"; SQUEEZE_PV="$(jp '{.spec.volumeName}' pvc squeeze)"
traffic() { metric agent "$B" "local_path_csi_transfer_$1_bytes_total{node=\"$B\",persistentvolume=\"$SQUEEZE_PV\"}"; }
kubectl delete pod squeeze --wait=true >/dev/null
claim_and_pod squeeze local-floating "$B"
check "a volume of zeros follows its pod to the other node" kubectl wait pod/squeeze --for=condition=Ready --timeout=300s
check "and arrives as it was" is = "$ZEROS" fingerprint squeeze
check "the receiving agent counts what rsync sent for the volume: its 50Mi at least" is -ge "$((50 << 20))" traffic uncompressed
check "a small part of that crossed the network" is -lt "$((5 << 20))" traffic compressed
check "which is still something" is -gt 0 traffic compressed
kubectl delete pod squeeze --wait=true >/dev/null; kubectl delete pvc squeeze --wait=true >/dev/null
check "the volume is gone" eventually 120 gone localpathvolume "$SQUEEZE_PV"
check "and its traffic leaves the metrics of the agent" eventually 60 is = "" traffic compressed

echo "== agent that was taken over"
# All the agents share a service account; what tells them apart is the node in their tokens.
SERVER="$(kubectl config view --raw -o jsonpath='{.clusters[0].cluster.server}')"
kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d >"$WORK/ca.crt"
# as_agent <node> <path of the object> <merge patch>: a write with a token of the agent of the
# node; prints the answer of the API server.
as_agent() {
  local token
  token="$(kubectl -n local-path-csi create token local-path-csi-agent --bound-object-kind Pod --bound-object-name "$(agent_of "$1")")"
  curl -s --cacert "$WORK/ca.crt" -H "Authorization: Bearer $token" -H 'Content-Type: application/merge-patch+json' \
    -X PATCH "$SERVER/apis/$API_GROUP/v1alpha1/$2" -d "$3"
}
# answers <text> <as_agent arguments...>
answers() { local text="$1"; shift; as_agent "$@" | grep -q -- "$text"; }
ADDRESS="$(jp '{.status.transferAddress}' localpathnode "$B")"
check "it cannot pass for the agent of another node: the LocalPathNode of that node is closed to it" \
  answers "writes the LocalPathNode of its own node only" "$A" "localpathnodes/$B/status" '{"status":{"transferAddress":"10.0.0.1:1","transferCertificate":"mine"}}'
check "that LocalPathNode says what it said" is = "$ADDRESS" jp '{.status.transferAddress}' localpathnode "$B"
check "it cannot have a volume of another node sent to itself" \
  answers "nor on its way there" "$B" "localpathvolumes/$PV" "{\"spec\":{\"node\":\"$B\"}}"
check "nor say that the data is with it" \
  answers "nor on its way there" "$B" "localpathvolumes/$PV/status" "{\"status\":{\"node\":\"$B\"}}"
check "the agent of the node of the volume does not send it away either: that is for the controller" \
  answers "an agent does not change the spec of a volume" "$A" "localpathvolumes/$PV" "{\"spec\":{\"node\":\"$B\"}}"
check "and does not say where the data is" \
  answers "an agent changes neither status.node nor status.attachedNode" "$A" "localpathvolumes/$PV/status" "{\"status\":{\"node\":\"$B\"}}"
check "nor does it approve the restore of a volume: that is for the claim or an administrator" \
  answers "an agent does not approve the restore of a volume" "$A" "localpathvolumes/$PV" "{\"metadata\":{\"annotations\":{\"$API_GROUP/restore-approved\":\"true\"}}}"
check "nor does it mark a volume as one that was brought back, or take the mark off one" \
  answers "an agent neither takes the mark off a volume that was brought back nor puts it on one" "$A" "localpathvolumes/$PV" "{\"metadata\":{\"labels\":{\"$API_GROUP/restore-pv\":\"true\"}}}"
# creates_as_agent <node> <name>: an agent makes a volume of its own node - a copy of $PV under
# another name - without the mark of a volume that was brought back; prints the answer.
creates_as_agent() {
  local token
  token="$(kubectl -n local-path-csi create token local-path-csi-agent --bound-object-kind Pod --bound-object-name "$(agent_of "$1")")"
  kubectl get localpathvolume "$PV" -o json | python3 -I -c '
import json, sys
volume = json.load(sys.stdin)
print(json.dumps({"apiVersion": volume["apiVersion"], "kind": volume["kind"], "metadata": {"name": sys.argv[1]}, "spec": volume["spec"]}))' "$2" |
    curl -s --cacert "$WORK/ca.crt" -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
      -X POST "$SERVER/apis/$API_GROUP/v1alpha1/localpathvolumes" -d @-
}
# refused <text> <creates_as_agent arguments...>
refused() { local text="$1"; shift; creates_as_agent "$@" | grep -q -- "$text"; }
check "nor make a volume that passes for one the controller has made: what an agent brings back waits for an administrator" \
  refused "only as one it has brought back from a directory" "$A" pvc-of-a-stranger
check "no such volume is there" gone localpathvolume pvc-of-a-stranger
check "the volume is where it was" is = "$A/$A" lv '{.spec.node}/{.status.node}' "$PV"
check "what belongs to its node, an agent writes" \
  answers '"kind": *"LocalPathVolume"' "$A" "localpathvolumes/$PV/status" "{\"status\":{\"reportedBy\":\"$A\"}}"

echo "== API server"
check "it refuses a write based on an outdated view of a volume: the controller relies on that" \
  sh -c "! kubectl patch localpathvolume $PV --subresource=status --type=merge -p '{\"metadata\":{\"resourceVersion\":\"1\"},\"status\":{\"message\":\"stale\"}}'"
check "and takes one based on the current view" \
  sh -c "kubectl patch localpathvolume $PV --subresource=status --type=merge -p \"{\\\"metadata\\\":{\\\"resourceVersion\\\":\\\"\$(kubectl get localpathvolume $PV -o jsonpath='{.metadata.resourceVersion}')\\\"},\\\"status\\\":{\\\"usedBytes\\\":1}}\""

echo "== node without room"
kubectl delete pod roam --wait=true >/dev/null
# Leaves node B 50Mi: the volume of 100Mi does not fit there.
ROOM="$(jp '{.status.pools[?(@.name=="default")].allocatableBytes}' localpathnode "$B")"
claim_and_pod filler local "$B" "$((ROOM - 50 * 1024 * 1024))"
check "a volume fills the pool of the other node" kubectl wait pod/filler --for=condition=Ready --timeout=180s
FILLER_PV="$(jp '{.spec.volumeName}' pvc filler)"
claim_and_pod roam local-floating
NOT_IN='{.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0]'
check "the webhook keeps a new pod off the node its volume does not fit on" is = "NotIn/$B" jp "$NOT_IN.operator}/$NOT_IN.values[0]}" pod roam
kubectl delete pod roam --wait=true >/dev/null
# A pod that names its node passes by the scheduler and the webhook.
kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata: {name: forced}
spec:
  nodeName: $B
  terminationGracePeriodSeconds: 1
  containers:
    - {name: app, image: "busybox:1.37", command: [sleep, infinity], volumeMounts: [{name: data, mountPath: /data}]}
  volumes: [{name: data, persistentVolumeClaim: {claimName: roam}}]
YAML
check "a volume that does not fit is refused when it is attached" eventually 120 event VolumeMoveFailed "cannot move to node $B"
check "and stays where it is" is = "$A/$A" lv '{.spec.node}/{.status.node}' "$PV"
kubectl delete pod forced filler --wait=true >/dev/null; kubectl delete pvc filler --wait=true >/dev/null
check "the room comes back when the other volume is gone" eventually 60 gone localpathvolume "$FILLER_PV"

echo "== move that nobody waits for"
claim_and_pod heavy local-floating "$A" 400Mi
kubectl wait pod/heavy --for=condition=Ready --timeout=180s >/dev/null
pod_sh heavy 'umask 002 && dd if=/dev/urandom of=/data/blob bs=1M count=250 2>/dev/null'
HEAVY="$(fingerprint heavy)"; HEAVY_PV="$(jp '{.spec.volumeName}' pvc heavy)"
# shellcheck disable=SC2283 # "is" is a function, and "=" is its first argument, not an assignment.
copying() { is = "Copying/$1" lv '{.status.transfer.state}/{.status.transfer.target}' "$HEAVY_PV"; }
kubectl delete pod heavy --wait=true >/dev/null
claim_and_pod heavy local-floating "$B" 400Mi
check "a copy in progress is reported in the LocalPathVolume" eventually 90 copying "$B"
kubectl delete pod heavy --wait=true >/dev/null
check "when the pod is gone the move is cancelled: the volume stays" eventually 90 is = "$A/$A/" lv '{.spec.node}/{.status.node}/{.status.attachedNode}' "$HEAVY_PV"
check "the unfinished copy is removed" eventually 90 node_sh "$B" "test ! -e /mnt/local-path-csi/incoming/$HEAVY_PV"
# The rsync of the copy is killed, and what it had started outlives it: somebody has to wait for
# those, or they stay on the node, dead, for as long as the agent runs.
nothing_of_a_copy() { ! node_sh "$1" "ps -eo stat=,args= | grep -E '[r]sync|[l]ocal-csi rsh|[l]ocal-csi\\] <defunct>'"; }
check "and nothing of it stays on the node, running or dead" eventually 30 nothing_of_a_copy "$B"
check "an event tells that" eventually 30 event VolumeMoveCancelled "no pod waits for the volume"
claim_and_pod heavy local-floating "$A" 400Mi
check "a pod on the node of the volume starts with nothing to wait for" kubectl wait pod/heavy --for=condition=Ready --timeout=120s
check "and finds its data" is = "$HEAVY" fingerprint heavy

echo "== agent restarted in the middle of a copy"
kubectl delete pod heavy --wait=true >/dev/null
claim_and_pod heavy local-floating "$B" 400Mi
check "the copy is under way when the agent goes" eventually 90 copying "$B"
kubectl -n local-path-csi delete pod "$(agent_of "$B")" --wait=true >/dev/null
check "the copy is taken up again and the pod starts" kubectl wait pod/heavy --for=condition=Ready --timeout=400s
check "on the node it was sent to" is = "$B/$B" lv '{.status.node}/{.status.attachedNode}' "$HEAVY_PV"
check "with every byte" is = "$HEAVY" fingerprint heavy

echo "== node with the data goes dark in the middle of a copy"
# New contents: the copy node A kept of the volume must not make this move a short one.
pod_sh heavy 'dd if=/dev/urandom of=/data/blob bs=1M count=250 2>/dev/null'
HEAVY="$(fingerprint heavy)"
kubectl delete pod heavy --wait=true >/dev/null
claim_and_pod heavy local-floating "$A" 400Mi
check "the copy is under way when the node goes dark" eventually 90 copying "$A"
docker pause "$B" >/dev/null
check "the volume takes room on the node it goes to" eventually 30 is -ge 419430400 metric controller "" "local_path_csi_pool_allocated_bytes{node=\"$A\",pool=\"default\"}"
check "the controller shows the volume as moving" eventually 30 is = 1 metric controller "" "local_path_csi_volume_moving{persistentvolume=\"$HEAVY_PV\",namespace=\"default\",persistentvolumeclaim=\"heavy\",source=\"$B\",target=\"$A\",state=\"Copying\"}"
sleep 10; docker unpause "$B" >/dev/null
check "the move waits for it and goes on" kubectl wait pod/heavy --for=condition=Ready --timeout=400s
check "with every byte" is = "$HEAVY" fingerprint heavy

echo "== two volumes to one node"
# Both with new contents, so that both have something to copy.
claim_and_pod roam local-floating "$A"
kubectl wait pod/roam --for=condition=Ready --timeout=180s >/dev/null
pod_sh roam 'umask 002 && dd if=/dev/urandom of=/data/dir/blob bs=1M count=60 2>/dev/null'
pod_sh heavy 'dd if=/dev/urandom of=/data/blob bs=1M count=250 2>/dev/null'
CHANGED="$(fingerprint roam)"; HEAVY="$(fingerprint heavy)"
kubectl delete pod heavy roam --wait=true >/dev/null
claim_and_pod heavy local-floating "$B" 400Mi
claim_and_pod roam local-floating "$B"
check "a node receives one volume at a time, the other waits for its turn" \
  eventually 90 sh -c "kubectl get localpathvolumes -o jsonpath='{.items[*].status.transfer.state}' | grep -q Queued"
check "both arrive" kubectl wait pod/heavy pod/roam --for=condition=Ready --timeout=400s
check "the first whole" is = "$HEAVY" fingerprint heavy
check "the second whole" is = "$CHANGED" fingerprint roam

echo "== controller stops while a volume is on its way"
pod_sh heavy 'dd if=/dev/urandom of=/data/blob bs=1M count=250 2>/dev/null'
HEAVY="$(fingerprint heavy)"
kubectl delete pod heavy --wait=true >/dev/null
claim_and_pod heavy local-floating "$A" 400Mi
check "the copy is under way when the controller is told to stop" eventually 90 copying "$A"
# The replica the attacher calls: its call waits for the volume to arrive.
CALLED="$(kubectl -n local-path-csi get "$(kubectl -n local-path-csi get leases -o name | grep attacher)" -o jsonpath='{.spec.holderIdentity}')"
check "a controller with a call that waits for a volume stops without being killed" kubectl -n local-path-csi delete pod "$CALLED" --wait=true --timeout=20s
check "the volume goes on with the replica that is left, and the pod starts" kubectl wait pod/heavy --for=condition=Ready --timeout=400s
check "with every byte" is = "$HEAVY" fingerprint heavy
kubectl -n local-path-csi rollout status deployment/local-path-csi-controller --timeout=240s >/dev/null

echo "== controller replica is replaced"
LEASE="$(kubectl -n local-path-csi get leases -o name | grep attacher)"
LEADER="$(kubectl -n local-path-csi get "$LEASE" -o jsonpath='{.spec.holderIdentity}')"
kubectl delete pod roam --wait=true >/dev/null
kubectl -n local-path-csi delete pod "$LEADER" --wait=false >/dev/null
claim_and_pod roam local-floating "$A"
check "a pod with a volume starts while the leading replica is gone, its volume moved by the other" kubectl wait pod/roam --for=condition=Ready --timeout=400s
check "the data is whole" is = "$CHANGED" fingerprint roam
kubectl -n local-path-csi rollout status deployment/local-path-csi-controller --timeout=240s >/dev/null
kubectl delete pod roam heavy --wait=true >/dev/null; kubectl delete pvc roam heavy --wait=true >/dev/null
check "deleted floating volumes leave nothing in volumes and incoming" eventually 120 sh -c "
  for node in $A $B; do test -z \"\$(docker exec \$node sh -c 'ls -A /mnt/local-path-csi/volumes; ls -A /mnt/local-path-csi/incoming')\" || exit 1; done"

echo "== controller that leads"
holder() { jp '{.spec.holderIdentity}' -n local-path-csi lease local-path-csi-controller; }
HOLDER="$(holder)"
check "one replica of the controller holds the Lease" kubectl -n local-path-csi get pod "$HOLDER"
kubectl -n local-path-csi delete pod "$HOLDER" --wait=false >/dev/null
# A leader that is told to stop gives the Lease up. One that just went silent would keep it for
# the fifteen seconds of its lease.
another_leads() { local now; now="$(holder)"; test -n "$now" && test "$now" != "$HOLDER" && kubectl -n local-path-csi get pod "$now"; }
check "when it is deleted another replica has the Lease at once" eventually 6 another_leads
kubectl -n local-path-csi rollout status deployment/local-path-csi-controller --timeout=240s >/dev/null

echo "== pinned volume and a closed node"
claim_and_pod stay local "$A"
kubectl wait pod/stay --for=condition=Ready --timeout=180s >/dev/null
STAY_PV="$(jp '{.spec.volumeName}' pvc stay)"
kubectl delete pod stay --wait=true >/dev/null
kubectl cordon "$A" >/dev/null
claim_and_pod stay local
# The wording of the scheduler differs between Kubernetes versions; it always names the node affinity of the volume.
check "a pod of a pinned volume waits for the node of the volume" eventually 90 sh -c "kubectl get pod stay -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].reason} {.status.conditions[?(@.type==\"PodScheduled\")].message}' | grep -q 'Unschedulable .*node affinity'"
check "the webhook has nothing to add to it" is = "" jp '{.spec.affinity}' pod stay
check "and nothing is copied anywhere" is = "$A/$A/" lv '{.spec.node}/{.status.node}/{.status.transfer.state}' "$STAY_PV"
kubectl uncordon "$A" >/dev/null
check "when the node opens the pod starts there" kubectl wait pod/stay --for=condition=Ready --timeout=180s
kubectl delete pod stay --wait=true >/dev/null; kubectl delete pvc stay --wait=true >/dev/null
check "the volume is removed when its claim is" eventually 120 gone localpathvolume "$STAY_PV"

echo "== events where kubectl describe shows them"
check "pool events on the LocalPathNode" sh -c "kubectl describe localpathnode $A | grep -q PoolReady"

echo "== node whose agent is gone"
AGENTLESS="${WORKERS[0]}"
check "the Node owns its LocalPathNode" is = "Node/$(jp '{.metadata.uid}' node "$AGENTLESS")" jp '{.metadata.ownerReferences[0].kind}/{.metadata.ownerReferences[0].uid}' localpathnode "$AGENTLESS"
claim_and_pod lonely local "$AGENTLESS"
kubectl wait pod/lonely --for=condition=Ready --timeout=180s >/dev/null
PV="$(jp '{.spec.volumeName}' pvc lonely)"
kubectl label node "$AGENTLESS" local-path-csi.domik.fi/storage- >/dev/null
agents_on() { kubectl -n local-path-csi get pods -l app.kubernetes.io/component=agent --field-selector "spec.nodeName=$1" -o name | wc -l; }
check "without the label the agent leaves the node" eventually 90 is = 0 agents_on "$AGENTLESS"
check "a running pod keeps its volume" pod_sh lonely 'echo still-here > /data/file'
sleep 80
kubectl patch pvc lonely --type=merge -p '{"spec":{"resources":{"requests":{"storage":"200Mi"}}}}' >/dev/null
check "a resize is refused with the reason" eventually 120 sh -c "kubectl get events --field-selector involvedObject.name=lonely -o jsonpath='{.items[*].message}' | grep -q 'has not reported'"
check "the controller reports the agent as gone" eventually 60 is = 0 metric controller "" "local_path_csi_node_agent_alive{node=\"$AGENTLESS\"}"
check "and its volume as stranded" is = 1 metric controller "" "local_path_csi_node_volumes_without_agent{node=\"$AGENTLESS\"}"
check "and the size of the volume is left alone" is = 104857600 jp '{.spec.sizeBytes}' localpathvolume "$PV"
kubectl label node "$AGENTLESS" local-path-csi.domik.fi/storage= >/dev/null
check "with the agent back the resize goes through" eventually 400 is = 200Mi jp '{.status.capacity.storage}' pvc lonely
check "no volume is stranded any more" eventually 60 is = 0 metric controller "" "local_path_csi_node_volumes_without_agent{node=\"$AGENTLESS\"}"
check "and the pod sees it" eventually 30 is = 204800 pod_sh lonely "df -k /data | tail -1 | awk '{print \$2}'"
kubectl delete pod lonely --wait=true >/dev/null; kubectl delete pvc lonely --wait=true >/dev/null

echo "== LocalPathVolume that is lost while its PersistentVolume stays"
BEREFT="${WORKERS[0]}"
claim_and_pod bereft local "$BEREFT"
kubectl wait pod/bereft --for=condition=Ready --timeout=180s >/dev/null
pod_sh bereft 'echo mine > /data/file'
PV="$(jp '{.spec.volumeName}' pvc bereft)"; PV_UID="$(jp '{.metadata.uid}' pv "$PV")"
kubectl delete pod bereft --wait=true >/dev/null
eventually 60 is = "" jp '{.status.attachedNode}' localpathvolume "$PV"
eventually 60 is = "" jp "{.items[?(@.spec.source.persistentVolumeName==\"$PV\")].metadata.name}" volumeattachments
# Removed by force, while its agent is stopped: an agent that runs would put its finalizer back,
# or take the deletion for one that is meant and remove the data.
docker pause "$BEREFT" >/dev/null
kubectl patch localpathvolume "$PV" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null
kubectl delete localpathvolume "$PV" --wait=true >/dev/null
docker unpause "$BEREFT" >/dev/null
check "the agent brings the LocalPathVolume back, as an orphan, though its PersistentVolume is there" eventually 180 is = "Orphaned/$BEREFT" jp '{.status.phase}/{.spec.node}' localpathvolume "$PV"
check "the agent of its node cannot take the mark off it" \
  answers "an agent neither takes the mark off a volume that was brought back nor puts it on one" "$BEREFT" "localpathvolumes/$PV" "{\"metadata\":{\"labels\":{\"$API_GROUP/restore-pv\":null}}}"
claim_and_pod bereft local "$BEREFT"
check "the pod of its claim is not given it: that the directory is the volume, only its node has said" eventually 120 event FailedAttachVolume "annotated the LocalPathVolume $API_GROUP/restore-approved=true"
check "and the volume stays an orphan, whatever is left of it in the cluster" is = "Orphaned/" jp '{.status.phase}/{.status.attachedNode}' localpathvolume "$PV"
kubectl annotate localpathvolume "$PV" "$API_GROUP/restore-approved=true" >/dev/null
check "approved by an administrator, it is a volume like any other again" eventually 60 is = "Ready/" jp "{.status.phase}/{.metadata.labels.${API_GROUP//./\\.}/restore-pv}" localpathvolume "$PV"
check "with the PersistentVolume it had" is = "$PV_UID" jp '{.metadata.uid}' pv "$PV"
check "and the pod of its claim finds its data" sh -c "kubectl wait pod/bereft --for=condition=Ready --timeout=300s && test \"\$(kubectl exec bereft -- cat /data/file)\" = mine"
kubectl delete pod bereft --wait=true >/dev/null; kubectl delete pvc bereft --wait=false >/dev/null
check "deleted with its claim, as any volume is" eventually 120 gone localpathvolume "$PV"

echo "== node that leaves the cluster and comes back"
LEAVING="${WORKERS[1]}"
claim_and_pod stranded local "$LEAVING"
kubectl wait pod/stranded --for=condition=Ready --timeout=180s >/dev/null
pod_sh stranded 'echo survivor > /data/file'
PV="$(jp '{.spec.volumeName}' pvc stranded)"
kubectl delete pod stranded --wait=true >/dev/null
eventually 60 is = "" jp '{.status.attachedNode}' localpathvolume "$PV"
# The node goes dark: kubelet and agent stop at once, as on a power cut.
docker pause "$LEAVING" >/dev/null
kubectl delete node "$LEAVING" --wait=true >/dev/null
kubectl delete pvc stranded --wait=false >/dev/null
check "a volume of a node that is gone is deleted without its agent" eventually 180 gone pv "$PV"
check "its LocalPathVolume is released" eventually 30 gone localpathvolume "$PV"
check "an event says that the data was left on the node" eventually 30 event DeletedWithoutNode "its data stays on the disk"
docker unpause "$LEAVING" >/dev/null
check "the data stays on the node" node_sh "$LEAVING" "grep -q survivor /mnt/local-path-csi/volumes/$PV/data/file"
check "the LocalPathNode of the deleted Node is gone with it" eventually 60 gone localpathnode "$LEAVING"
# The node boots again: a kubelet registers its Node only when it starts.
node_sh "$LEAVING" 'systemctl restart kubelet'
eventually 120 kubectl get node "$LEAVING"
kubectl label node "$LEAVING" local-path-csi.domik.fi/storage= >/dev/null
check "when the node returns, it reports its pools again, owned by the new Node" eventually 240 is = "Ready/$(jp '{.metadata.uid}' node "$LEAVING")" jp '{.status.pools[0].state}/{.metadata.ownerReferences[0].uid}' localpathnode "$LEAVING"
check "when the node returns, the volume comes back, as an orphan" eventually 240 is = "Orphaned/$LEAVING" jp '{.status.phase}/{.spec.node}' localpathvolume "$PV"
# The claim of the same name asks for the volume by its name. That does not bring it back: which
# directory is the volume of that name is the word of the node.
claim_and_pod stranded local "$LEAVING" 100Mi "$PV"
# Long enough for the controller to have gone over the volumes that were brought back twice.
check "a claim that names the volume does not get it by that" sh -c "sleep 25; ! kubectl get pv $PV"
kubectl annotate localpathvolume "$PV" "$API_GROUP/restore-approved=true" >/dev/null
check "approved by an administrator, the volume gets a PersistentVolume reserved for the claim it had" eventually 60 is = "default/stranded/Retain" jp '{.spec.claimRef.namespace}/{.spec.claimRef.name}/{.spec.persistentVolumeReclaimPolicy}' pv "$PV"
check "and the claim of the same name finds its data" sh -c "kubectl wait pod/stranded --for=condition=Ready --timeout=240s && test \"\$(kubectl exec stranded -- cat /data/file)\" = survivor"
check "the volume is an orphan no more" eventually 60 is = Ready jp '{.status.phase}' localpathvolume "$PV"

echo "== filesystem mounted at the path of a pool after the agent has started"
# The pool "absent" is a plain directory so far. A filesystem mounted there now - without project
# quota, so the pool gets another reason to be refused - has to reach the agent as it runs.
node_sh "${WORKERS[0]}" 'mount -t tmpfs tmpfs /mnt/local-path-csi/absent'
check "the agent sees it without a restart" eventually 60 is = NoQuota pool_state "${WORKERS[0]}" absent
node_sh "${WORKERS[0]}" 'umount /mnt/local-path-csi/absent'
check "and sees it go" eventually 60 is = NotMountpoint pool_state "${WORKERS[0]}" absent

if [ "$FAILED" != 0 ] && [ "${DEBUG:-0}" = 1 ]; then
  echo "== diagnostics"
  kubectl get localpathnodes,localpathvolumes,pv,pvc,pods -A -o wide 2>&1 | tail -40
  kubectl -n local-path-csi logs deployment/local-path-csi-controller -c local-path-csi --tail=30 2>&1
  kubectl -n local-path-csi logs daemonset/local-path-csi-agent -c local-path-csi --tail=30 2>&1
  kubectl get events -A --sort-by=.lastTimestamp 2>&1 | tail -25
fi
exit "$FAILED"
