#!/usr/bin/env bash
set -euo pipefail

# Runs the local Stravin Core Load + Chaos E2E verification suite:
#   1. Boots docker-compose.chaos.yml (Floci + Toxiproxy + all 5 Stravin daemons)
#      with an ephemeral Ed25519 SSH keypair.
#   2. Seeds test repositories via stravin-admin.
#   3. Runs stravin-chaos injecting real Toxiproxy faults alongside stravin-loadgen
#      driving concurrent dev + agent Git workloads across HTTP (:18080) & SSH (:12222).
#   4. Sends SIGINT (Ctrl+C) to stravin-chaos and stravin-loadgen, verifying:
#      - stravin-chaos exits cleanly and removes all active toxics (0 remaining)
#      - stravin-loadgen exits cleanly within flush timeout with exit code 0
#      - total_ops > 0, unexpected_errors == 0, fsck_failures == 0.
#
# Supports both `docker compose` (Docker CLI v2) and standalone `docker-compose`.

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "error: neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 1
fi

COMPOSE_FILE="docker-compose.chaos.yml"
COMPOSE_ARGS=(-f "${COMPOSE_FILE}")
if [ "${TELEMETRY:-0}" = "1" ]; then
  COMPOSE_ARGS+=(--profile telemetry)
fi

WORK_DIR="$(mktemp -d)"
CHAOS_PID=""
LOADGEN_PID=""

cleanup() {
  local exit_code=$?

  if [ -n "${CHAOS_PID}" ] && kill -0 "${CHAOS_PID}" 2>/dev/null; then
    kill -INT "${CHAOS_PID}" 2>/dev/null || true
    wait "${CHAOS_PID}" 2>/dev/null || true
  fi
  if [ -n "${LOADGEN_PID}" ] && kill -0 "${LOADGEN_PID}" 2>/dev/null; then
    kill -INT "${LOADGEN_PID}" 2>/dev/null || true
    wait "${LOADGEN_PID}" 2>/dev/null || true
  fi

  if [ "${exit_code}" -ne 0 ]; then
    echo "--- chaos-e2e failed (exit=${exit_code}); dumping diagnostics ---" >&2
    if [ -f "${WORK_DIR}/chaos.log" ]; then
      echo "=== stravin-chaos.log (tail) ===" >&2
      tail -n 15 "${WORK_DIR}/chaos.log" >&2 || true
    fi
    if [ -f "${WORK_DIR}/loadgen.log" ]; then
      echo "=== stravin-loadgen.log (tail) ===" >&2
      tail -n 20 "${WORK_DIR}/loadgen.log" >&2 || true
    fi
    if [ -f "${WORK_DIR}/loadgen-summary.json" ] && [ -s "${WORK_DIR}/loadgen-summary.json" ]; then
      echo "=== loadgen failure breakdown ===" >&2
      python3 - "${WORK_DIR}/loadgen-summary.json" <<'PY' >&2 || true
import json, re, sys
from collections import Counter
data = json.load(open(sys.argv[1]))
print("oracle:", json.dumps(data.get("oracle", {})))
c = Counter()
for f in data.get("failures") or []:
    d = re.sub(r'/[^\s\']+stravin-loadgen-[^\s\']+', '<tmp>', f.get("detail", ""))
    c[(f.get("scenario"), f.get("op"), d)] += 1
for (sc, op, d), cnt in c.most_common(15):
    print(f"  [{cnt}x] {sc} ({op}): {d}")
PY
    else
      "${COMPOSE[@]}" -f "${COMPOSE_FILE}" logs --tail=20 stravin-ctrl stravin-node stravin-edge || true
    fi
  fi

  if [ "${STRAVIN_CHAOS_KEEP_STACK:-0}" != "1" ]; then
    "${COMPOSE[@]}" -f "${COMPOSE_FILE}" --profile telemetry down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK_DIR}"
  exit "${exit_code}"
}
trap cleanup EXIT

ssh-keygen -q -t ed25519 -N "" -C "local:chaos-e2e-ssh-user" -f "${WORK_DIR}/id_ed25519"
chmod 600 "${WORK_DIR}/id_ed25519"

export STRAVIN_SSH_AUTHORIZED_KEYS="$(cat "${WORK_DIR}/id_ed25519.pub")"
export STRAVIN_AUTH_TOKENS="${STRAVIN_AUTH_TOKENS:-local:dev:stravin-local-dev-token}"
export STRAVIN_CTRL_ADMIN_TOKEN="${STRAVIN_CTRL_ADMIN_TOKEN:-stravin-local-ctrl-admin-token}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://127.0.0.1:4566}"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_REGION="us-east-1"
export AWS_DEFAULT_REGION="us-east-1"

CHAOS_RUN_SECONDS="${CHAOS_RUN_SECONDS:-25}"
DEVS="${DEVS:-4}"
AGENTS="${AGENTS:-6}"
REPOS="${REPOS:-3}"
SEED="${SEED:-42}"
FLUSH_TIMEOUT="${FLUSH_TIMEOUT:-10s}"
OTEL_ENDPOINT="${OTEL_ENDPOINT:-}"
if [ "${TELEMETRY:-0}" = "1" ] && [ -z "${OTEL_ENDPOINT}" ]; then
  OTEL_ENDPOINT="http://127.0.0.1:4317"
fi

HOST_GOOS="$(uname -s | tr '[:upper:]' '[:lower:]')"
HOST_GOARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')"

echo "[chaos-e2e] Building stravin-loadgen, stravin-chaos, and stravin-admin (${HOST_GOOS}/${HOST_GOARCH})..."
mkdir -p bin
CGO_ENABLED=0 GOOS="${HOST_GOOS}" GOARCH="${HOST_GOARCH}" ./scripts/go.sh build -o bin/stravin-loadgen ./cmd/stravin-loadgen
CGO_ENABLED=0 GOOS="${HOST_GOOS}" GOARCH="${HOST_GOARCH}" ./scripts/go.sh build -o bin/stravin-chaos ./cmd/stravin-chaos
CGO_ENABLED=0 GOOS="${HOST_GOOS}" GOARCH="${HOST_GOARCH}" ./scripts/go.sh build -o bin/stravin-admin ./cmd/stravin-admin

echo "[chaos-e2e] Starting ${COMPOSE_FILE} stack..."
"${COMPOSE[@]}" -f "${COMPOSE_FILE}" --profile telemetry down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" "${COMPOSE_ARGS[@]}" up -d --build

wait_http() {
  local name="$1"
  local url="$2"
  local i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[chaos-e2e] ${name} healthy (${url})"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "[chaos-e2e] timed out waiting for ${name} at ${url}" >&2
  return 1
}

wait_http "toxiproxy" "http://127.0.0.1:8474/proxies"
wait_http "stravin-ctrl" "http://127.0.0.1:8083/healthz"
wait_http "stravin-node" "http://127.0.0.1:8081/healthz"
wait_http "stravin-edge" "http://127.0.0.1:8080/healthz"
wait_http "stravin-edge (via toxiproxy)" "http://127.0.0.1:18080/healthz"
wait_http "stravin-pack" "http://127.0.0.1:8082/healthz"
wait_http "stravin-hook" "http://127.0.0.1:8084/healthz"

echo "[chaos-e2e] Seeding ${REPOS} test repositories (repo-1..repo-${REPOS}) via stravin-admin..."
for i in $(seq 1 "${REPOS}"); do
  ./bin/stravin-admin repo create --tenant local --repo "repo-${i}" --idempotent >/dev/null
done

echo "[chaos-e2e] Launching stravin-chaos against http://127.0.0.1:8474 (seed=${SEED})..."
./bin/stravin-chaos \
  --toxiproxy-url "http://127.0.0.1:8474" \
  --seed "${SEED}" \
  --interval 3s \
  --fault-duration 2s \
  --duration 0 \
  --otel-endpoint "${OTEL_ENDPOINT}" \
  >"${WORK_DIR}/chaos.log" 2>&1 &
CHAOS_PID=$!

echo "[chaos-e2e] Launching stravin-loadgen (devs=${DEVS}, agents=${AGENTS}, repos=${REPOS}, seed=${SEED})..."
./bin/stravin-loadgen \
  --devs "${DEVS}" \
  --agents "${AGENTS}" \
  --duration 0 \
  --http-url "http://127.0.0.1:18080" \
  --ssh-addr "127.0.0.1:12222" \
  --ssh-key "${WORK_DIR}/id_ed25519" \
  --tenant "local" \
  --repos "${REPOS}" \
  --auth-token "dev:stravin-local-dev-token" \
  --admin-url "http://127.0.0.1:8083" \
  --admin-token "${STRAVIN_CTRL_ADMIN_TOKEN}" \
  --otel-endpoint "${OTEL_ENDPOINT}" \
  --flush-timeout "${FLUSH_TIMEOUT}" \
  --seed "${SEED}" \
  >"${WORK_DIR}/loadgen-summary.json" 2>"${WORK_DIR}/loadgen.log" &
LOADGEN_PID=$!

echo "[chaos-e2e] Running under active Toxiproxy chaos for ${CHAOS_RUN_SECONDS}s..."
elapsed=0
while [ "${elapsed}" -lt "${CHAOS_RUN_SECONDS}" ]; do
  if ! kill -0 "${CHAOS_PID}" 2>/dev/null; then
    echo "[chaos-e2e] stravin-chaos exited prematurely" >&2
    exit 1
  fi
  if ! kill -0 "${LOADGEN_PID}" 2>/dev/null; then
    echo "[chaos-e2e] stravin-loadgen exited prematurely" >&2
    exit 1
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

echo "[chaos-e2e] Sending SIGINT (Ctrl+C) to stravin-chaos..."
kill -INT "${CHAOS_PID}"
wait "${CHAOS_PID}"
CHAOS_PID=""

echo "[chaos-e2e] Verifying Toxiproxy has 0 active toxics after SIGINT..."
proxies_json="$(curl -fsS http://127.0.0.1:8474/proxies)"
active_toxics="$(python3 -c '
import json, sys
data = json.loads(sys.stdin.read())
total = sum(len(p.get("toxics", [])) for p in data.values())
print(total)
' <<< "${proxies_json}")"
if [ "${active_toxics}" -ne 0 ]; then
  echo "[chaos-e2e] expected 0 active toxics after SIGINT, found ${active_toxics}" >&2
  echo "${proxies_json}" >&2
  exit 1
fi
echo "[chaos-e2e] Verified 0 active toxics remaining in Toxiproxy."

echo "[chaos-e2e] Sending SIGINT (Ctrl+C) to stravin-loadgen..."
shutdown_start="$(date +%s)"
kill -INT "${LOADGEN_PID}"
loadgen_rc=0
wait "${LOADGEN_PID}" || loadgen_rc=$?
LOADGEN_PID=""
shutdown_end="$(date +%s)"
shutdown_secs=$((shutdown_end - shutdown_start))

echo "[chaos-e2e] stravin-loadgen exited with code ${loadgen_rc} in ${shutdown_secs}s"
if [ "${loadgen_rc}" -ne 0 ]; then
  echo "[chaos-e2e] stravin-loadgen exited non-zero (${loadgen_rc})" >&2
  exit 1
fi

if [ ! -s "${WORK_DIR}/loadgen-summary.json" ]; then
  echo "[chaos-e2e] loadgen-summary.json is empty" >&2
  exit 1
fi

python3 - "${WORK_DIR}/loadgen-summary.json" "${shutdown_secs}" <<'PY'
import json
import sys

summary_path = sys.argv[1]
shutdown_secs = int(sys.argv[2])

with open(summary_path, "r", encoding="utf-8") as f:
    data = json.load(f)

oracle = data.get("oracle", {})
total_ops = int(oracle.get("total_ops", 0))
retries = int(oracle.get("retries", 0))
unexpected_errors = int(oracle.get("unexpected_errors", 0))
fsck_failures = int(oracle.get("fsck_failures", 0))
duration_ms = int(data.get("duration_ns", 0)) // 1_000_000

print(
    f"[chaos-e2e] Summary: total_ops={total_ops} retries={retries} "
    f"unexpected_errors={unexpected_errors} fsck_failures={fsck_failures} "
    f"duration_ms={duration_ms} shutdown_s={shutdown_secs}"
)

failures = data.get("failures") or []
if failures:
    print("[chaos-e2e] Recorded failures:", json.dumps(failures, indent=2), file=sys.stderr)

if total_ops <= 0:
    print("[chaos-e2e] FAIL: expected total_ops > 0", file=sys.stderr)
    sys.exit(1)
if unexpected_errors != 0:
    print(f"[chaos-e2e] FAIL: expected unexpected_errors == 0, got {unexpected_errors}", file=sys.stderr)
    sys.exit(1)
if fsck_failures != 0:
    print(f"[chaos-e2e] FAIL: expected fsck_failures == 0, got {fsck_failures}", file=sys.stderr)
    sys.exit(1)

print("[chaos-e2e] PASS: all load + chaos + SIGINT assertions succeeded.")
PY
