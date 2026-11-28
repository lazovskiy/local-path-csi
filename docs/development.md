# Development

## Tests

| Command | What it checks |
|---|---|
| `cargo test` | Unit tests: the controller against an in-memory store, the decisions of the agent, the webhook, parsing. Integration tests in `tests/`: leader election and the store of the controller against a mock API server; custom names reaching everything built from them; a volume copied between two agents with a real rsync (needs `rsync` in `PATH`, or its path in `LOCAL_PATH_CSI_RSYNC`); the process the agent runs under, which cleans up after an abandoned copy. |
| `e2e/xfs-quota.sh` | Quota and volume directories on a real XFS filesystem, on a loop device in a privileged container. |
| `e2e/kind.sh` | The chart on a kind cluster with two storage nodes. See [End-to-end tests](#end-to-end-tests). |
| `API_GROUP=example.com DRIVER_NAME=path.csi.example.com e2e/kind.sh` | The same, with the chart installed under names other than the defaults. |
| `e2e/alerts.sh` | The alerts, with promtool: the rule file is valid, and the alerts fire when they should. |

### End-to-end tests

`e2e/kind.sh` covers:

- provisioning, quota, fsGroup, kubelet volume stats, expansion and capacity;
- deletion, the trash, and a volume put back from the trash;
- a node that lost its agent;
- a `LocalPathVolume` lost while its PersistentVolume stays;
- a node that leaves the cluster and returns;
- metrics and events along the way;
- `Floating` volumes: a move after `cordon`, the way back, the webhook, a node without room, a
  cancelled move, an agent restarted and a node gone dark in the middle of a copy, the queue, and
  a controller replica replaced.

## Generated code

- The CRDs in `chart/files/crds` are generated from `src/api.rs`:

  ```sh
  cargo run -- crds --out chart/files/crds
  ```

  `cargo test` fails while the chart has stale CRDs.

- The CSI bindings are generated at build time from `proto/csi.proto` (CSI spec v1.12.0).

## CI

CI runs on GitHub Actions; the workflows are in `.github/workflows/`.

- **Every push to `main` and every pull request** runs `cargo fmt`, `clippy -D warnings`,
  `cargo test`, `e2e/xfs-quota.sh`, `e2e/alerts.sh`, `helm lint` and `shellcheck`. A pull request
  can be merged only when `checks / lint` and `checks / unit` pass.
- **A pull request from this repository** also publishes:
  - the image `ghcr.io/lazovskiy/local-path-csi:prN`;
  - the chart `oci://ghcr.io/lazovskiy/charts/local-path-csi` of version `X.Y.Z-prN`, which uses
    that image.

  A comment in the pull request shows the install command. Every push moves the tag `prN`, so
  install with `image.pullPolicy: Always`. Closing the pull request deletes the image and the
  chart.
- **The label `run-e2e`** on a pull request runs `e2e/kind.sh` on its image, with the default
  names and with other ones. It runs on every push while the label is on.
- **The layer cache** of image builds is `ghcr.io/lazovskiy/local-path-csi:buildcache`. Pull
  requests and releases share it. Only the files that `.dockerignore` lets in reach the build. A
  change anywhere else reuses the image as it is, and a change of the code rebuilds the crate but
  not its dependencies.

## Release

1. Set the version in:
   - `Cargo.toml`, and in `Cargo.lock` by running `cargo check`;
   - `version` and `appVersion` of `chart/Chart.yaml`.
2. Merge the change.
3. Push a tag `vX.Y.Z` on a commit of `main` or of a `release-*` branch.

The release workflow then:

1. refuses a tag that does not match the versions in those files;
2. runs the checks;
3. pushes the image by its digest and runs the e2e tests on it;
4. tags the image `X.Y.Z`, and `latest` unless a higher version is already out;
5. publishes the chart and the GitHub release.
