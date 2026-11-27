#!/usr/bin/env bash
# Runs tests/xfs_quota.rs against a real XFS with project quota: a loop device inside a
# privileged container, so nothing on the host is formatted or remounted.
#
#   e2e/xfs-quota.sh            # XFS
#   FS=ext4 e2e/xfs-quota.sh    # ext4 with the project feature
#
# ext4 does not hold a process with CAP_SYS_RESOURCE to a limit, which is why the driver asks for
# XFS. There the tests run without that capability, as a pod that is not privileged does: the
# rest of the quota code is the same for both filesystems.
#
# Env: IMAGE (must have the same libc as the build host), CARGO_TARGET_DIR.
set -Eeuo pipefail

cd "$(dirname "$0")/.."
FS="${FS:-xfs}"
IMAGE="${IMAGE:-docker.io/library/ubuntu:26.04}"

binary="$(cargo test --locked --no-run --test xfs_quota --message-format=json \
  | python3 -I -c 'import json,sys
for line in sys.stdin:
    try: m = json.loads(line)
    except ValueError: continue
    if m.get("reason") == "compiler-artifact" and m.get("executable") and m["target"]["name"] == "xfs_quota":
        print(m["executable"])')"
[ -x "$binary" ] || { echo "test binary was not built" >&2; exit 1; }

docker run --rm --privileged -e FS="$FS" -v "$binary:/xfs_quota:ro" "$IMAGE" bash -Eeuo pipefail -c '
  apt-get -qq update >/dev/null && apt-get -qq install -y --no-install-recommends xfsprogs e2fsprogs >/dev/null
  truncate -s 2G /pool.img
  loop="$(losetup -f)"
  # A loop device allocated after the container started has no node in its /dev.
  [ -b "$loop" ] || mknod "$loop" b 7 "${loop#/dev/loop}"
  losetup "$loop" /pool.img
  trap "umount /pool 2>/dev/null || true; losetup -d $loop" EXIT
  if [ "$FS" = xfs ]; then mkfs.xfs -q "$loop"; else mkfs.ext4 -q -O project,quota -E quotatype=prjquota "$loop"; fi
  mkdir /pool && mount -o prjquota "$loop" /pool
  echo "kernel $(uname -r), $FS on $loop"
  as=()
  [ "$FS" = xfs ] || as=(setpriv --bounding-set -sys_resource --inh-caps -sys_resource)
  LOCAL_PATH_CSI_TEST_POOL=/pool "${as[@]}" /xfs_quota --ignored --test-threads=1 --nocapture
'
