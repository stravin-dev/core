#!/usr/bin/env bash
set -euo pipefail

# Runs the full 6/6 Stravin Core E2E suite against Docker Compose + Floci
# (zero real AWS resources). Supports both `docker compose` (Linux CI) and
# standalone `docker-compose` (macOS).

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "error: neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 1
fi

KEY_DIR="$(mktemp -d)"
cleanup() {
  local exit_code=$?
  if [ "${exit_code}" -ne 0 ]; then
    echo "--- E2E failed (exit=${exit_code}); dumping service logs ---" >&2
    "${COMPOSE[@]}" logs --tail=80 floci floci-init stravin-ctrl stravin-node stravin-edge stravin-pack stravin-hook || true
  fi
  if [ "${STRAVIN_E2E_KEEP_STACK:-0}" != "1" ]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "${KEY_DIR}"
  exit "${exit_code}"
}
trap cleanup EXIT

ssh-keygen -q -t ed25519 -N "" -C "local:e2e-ssh-user" -f "${KEY_DIR}/id_ed25519"
chmod 600 "${KEY_DIR}/id_ed25519"

export STRAVIN_SSH_AUTHORIZED_KEYS="$(cat "${KEY_DIR}/id_ed25519.pub")"
export STRAVIN_AUTH_TOKENS="${STRAVIN_AUTH_TOKENS:-local:dev:stravin-local-dev-token}"
export STRAVIN_E2E_SSH_KEY="${KEY_DIR}/id_ed25519"
export STRAVIN_E2E_TOKEN="stravin-local-dev-token"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://127.0.0.1:4566}"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_REGION="us-east-1"
export AWS_DEFAULT_REGION="us-east-1"

echo "[e2e] Starting full Docker Compose + Floci stack..."
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build floci floci-init stravin-ctrl stravin-node stravin-edge stravin-pack stravin-hook

wait_http() {
  local name="$1"
  local url="$2"
  local i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[e2e] ${name} healthy (${url})"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "[e2e] timed out waiting for ${name} at ${url}" >&2
  return 1
}

wait_http "stravin-ctrl" "http://127.0.0.1:8083/healthz"
wait_http "stravin-node" "http://127.0.0.1:8081/healthz"
wait_http "stravin-edge" "http://127.0.0.1:8080/healthz"
wait_http "stravin-pack" "http://127.0.0.1:8082/healthz"
wait_http "stravin-hook" "http://127.0.0.1:8084/healthz"

echo "[e2e] Running 6/6 Go E2E suite (-race -tags=e2e)..."
./scripts/go.sh test -race -v -tags=e2e -count=1 -run "${STRAVIN_E2E_RUN:-.}" ./internal/e2e/...
