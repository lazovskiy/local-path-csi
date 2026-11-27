#!/usr/bin/env bash
# Checks the alerts of the chart with promtool: the rule file is valid, and the alerts fire when
# they should (e2e/alerts_test.yaml).
#
# Env: PROMETHEUS_IMAGE.
set -Eeuo pipefail

cd "$(dirname "$0")/.."
IMAGE="${PROMETHEUS_IMAGE:-docker.io/prom/prometheus:v3.16.0}"

docker run --rm --entrypoint promtool \
  -v "$PWD/chart/files/alerts.yaml:/rules/alerts.yaml:ro" -v "$PWD/e2e/alerts_test.yaml:/tests/alerts_test.yaml:ro" \
  "$IMAGE" check rules /rules/alerts.yaml
docker run --rm --entrypoint promtool \
  -v "$PWD/chart/files/alerts.yaml:/rules/alerts.yaml:ro" -v "$PWD/e2e/alerts_test.yaml:/tests/alerts_test.yaml:ro" \
  "$IMAGE" test rules /tests/alerts_test.yaml
