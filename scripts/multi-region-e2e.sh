#!/usr/bin/env bash
set -euo pipefail

# Runs the automated multi-region sync + dual-daemon E2E suite against
# docker-compose.multi-region.yml (zero real AWS resources). Supports both
# `docker compose` (Linux CI) and standalone `docker-compose` (macOS).

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f docker-compose.multi-region.yml)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -f docker-compose.multi-region.yml)
else
  echo "error: neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 1
fi

ALL_SERVICES=(
  floci-region-a
  floci-region-b
  floci-init-a
  floci-init-b
  valkey-east
  valkey-west
  floci-test-s3-sync-a-to-b
  floci-test-s3-sync-b-to-a
  floci-test-ddb-sync-a-to-b
  floci-test-ddb-sync-b-to-a
  stravin-ctrl-a
  stravin-node-a
  stravin-edge-a
  stravin-ctrl-b
  stravin-node-b
  stravin-edge-b
)

cleanup() {
  local exit_code=$?
  if [ "${exit_code}" -ne 0 ]; then
    echo "--- Multi-region E2E failed (exit=${exit_code}); dumping service logs ---" >&2
    "${COMPOSE[@]}" logs --tail=80 "${ALL_SERVICES[@]}" || true
  fi
  if [ "${STRAVIN_E2E_KEEP_STACK:-0}" != "1" ]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

export STRAVIN_AUTH_TOKENS="${STRAVIN_AUTH_TOKENS:-local:dev:stravin-local-dev-token}"
export STRAVIN_E2E_TOKEN="stravin-local-dev-token"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"

echo "[multi-region-e2e] Starting dual-Floci + sync + regional daemon stack..."
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build

wait_http() {
  local name="$1"
  local url="$2"
  local i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[multi-region-e2e] ${name} healthy (${url})"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "[multi-region-e2e] timed out waiting for ${name} at ${url}" >&2
  return 1
}

wait_http "stravin-ctrl-a" "http://127.0.0.1:8083/healthz"
wait_http "stravin-node-a" "http://127.0.0.1:8091/healthz"
wait_http "stravin-edge-a" "http://127.0.0.1:8080/healthz"
wait_http "stravin-ctrl-b" "http://127.0.0.1:8085/healthz"
wait_http "stravin-node-b" "http://127.0.0.1:8092/healthz"
wait_http "stravin-edge-b" "http://127.0.0.1:8081/healthz"

for svc in floci-test-s3-sync-a-to-b floci-test-s3-sync-b-to-a floci-test-ddb-sync-a-to-b floci-test-ddb-sync-b-to-a; do
  cid="$("${COMPOSE[@]}" ps -q "${svc}")"
  if [ -z "${cid}" ] || [ "$(docker inspect -f '{{.State.Running}}' "${cid}")" != "true" ]; then
    echo "[multi-region-e2e] sync container ${svc} is not running" >&2
    exit 1
  fi
  echo "[multi-region-e2e] ${svc} running"
done

if command -v go >/dev/null 2>&1; then
  GO_CMD=(go)
elif command -v asdf >/dev/null 2>&1 && asdf which go >/dev/null 2>&1; then
  GO_CMD=(asdf exec go)
else
  GO_CMD=(./scripts/go.sh)
fi

echo "[multi-region-e2e] Running multi-region E2E Go suite (-race -tags=multiregion)..."
"${GO_CMD[@]}" test -race -v -tags=multiregion -count=1 -run "${STRAVIN_MR_E2E_RUN:-.}" ./internal/e2e/...
