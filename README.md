# Stravin Core

Stateless, multi-region Git storage engine in Go backed by S3 and DynamoDB, licensed under Apache 2.0.

## Quickstart (100% Local via Docker Compose + Floci)

Everything runs locally against [Floci](https://github.com/floci-dev/floci) (`AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`) with zero real AWS resources.

### 1. Boot the full stack

```bash
docker compose up -d --build
```

Services started:
- **Floci** (`:4566`): Local S3, DynamoDB, DynamoDB Streams, and KMS emulator (`floci-init` creates `git-objects-us-east-1`, `stravin-core`, `alias/stravin-local-dev`, and seeds `local/demo` and `local/repo-1`..`local/repo-3`).
- **NATS JetStream** (`:4222` client, `:8222` monitoring): Local async event bus with persistent JetStream storage (`nats-init` idempotently creates the `STRAVIN_CORE` stream for `stravin.core.>`).
- **`stravin-edge`** (`:8080` Smart HTTP, `:2222` SSH): Stateless Git ingress proxy. Default local HTTP token: `stravin-local-dev-token` (tenant `local`).
- **`stravin-node`** (`:8081` health/metrics, `:9090` gRPC): Git read/write engine with NVMe pack cache and Tier 1 Wasm hook runtime.
- **`stravin-pack`** (`:8082`): Sharded pack compaction (`.midx` + `commit-graph`) and delayed GC daemon.
- **`stravin-ctrl`** (`:8083`): Regional write-lease manager, `failover_epoch` fencing, and S3 integrity scrub/rewind API.
- **`stravin-hook`** (`:8084`): DynamoDB Streams consumer dispatching KMS-encrypted, HMAC-SHA256-signed Tier 2 webhooks with retry and DLQ.

### 2. Push and clone over Smart HTTP

```bash
git init /tmp/demo && cd /tmp/demo
git commit --allow-empty -m "hello stravin"
git push http://dev:stravin-local-dev-token@127.0.0.1:8080/local/demo.git HEAD:refs/heads/main
git clone http://dev:stravin-local-dev-token@127.0.0.1:8080/local/demo.git /tmp/demo-clone
```

### 3. Admin & Seed CLI (`stravin-admin`)

Build and use `stravin-admin` to register repositories, deploy Tier 1 `.wasm` hooks to S3 + DynamoDB, and configure Tier 2 KMS-encrypted webhooks:

```bash
make build

export AWS_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Register a tenant/repo META record
./bin/stravin-admin repo create --tenant local --repo my-repo

# Upload a Tier 1 .wasm hook to S3 (hooks/{sha256}.wasm) and bind it in DynamoDB
./bin/stravin-admin hook put --tenant local --repo my-repo --hook-id policy --wasm ./policy.wasm

# Configure a Tier 2 webhook with a KMS-encrypted HMAC secret
./bin/stravin-admin webhook put --tenant local --repo my-repo --webhook-id ci \
  --url http://host.docker.internal:9000/webhook --secret local-dev-secret
```

### 4. Run the Automated 6/6 E2E Suite

```bash
make e2e
```

`make e2e` builds and boots the full Docker Compose + Floci stack with an ephemeral SSH keypair, waits for all 5 daemons to report healthy, runs all 6 end-to-end scenarios with `go test -race -tags=e2e ./internal/e2e/...`, and tears down cleanly:
1. Smart HTTP (`:8080`) + SSH (`:2222`) Git lifecycle (initial + incremental thin-pack push/clone/fetch).
2. Concurrent Ref-CAS conflict resolution (exactly one winner, zero lost commits).
3. Tier 1 Wasm hook `ALLOW` and `REJECT` (S3 upload cancelled, zero DDB writes, Git side-band stderr).
4. Tier 2 `stravin-hook` async outbox (DynamoDB Streams -> HMAC-SHA256 webhook delivery + retry/DLQ).
5. `stravin-pack` sharded lease compaction (`.midx` + `commit-graph` + 2-phase swap) and delayed GC.
6. `stravin-ctrl` `failover_epoch` fencing and fault-injected S3 pack deletion `HeadObject` scrub + deterministic `Ref History` rewind + audit trail.

### 5. Local Telemetry Stack (Grafana + Prometheus + Loki + Tempo)

Enable the optional OpenTelemetry observability profile:

```bash
docker compose --profile telemetry up -d
```

Open Grafana at [http://localhost:3000](http://localhost:3000) (anonymous Admin access enabled for local dev) with pre-provisioned Prometheus, Loki, and Tempo datasources and cross-linked dashboards:

- **Stravin Core** (`stravin-core`): Edge Git HTTP/SSH RPS & latency, gRPC throughput, Node pack cache hit ratio, S3/DynamoDB latency, and Wasm hook evaluation metrics with Prometheus exemplars -> Tempo traces -> Loki logs links.
- **Stravin Load Tester** (`stravin-loadgen`): Active dev/agent threads, scenario throughput & latency, retryable conflict/transient counts, unexpected errors, and `git fsck` failures with direct trace-ID cross-links to Tempo and Loki.

## Load & Chaos Testing (Local)

Stravin Core includes a standalone multi-persona Git load generator (`stravin-loadgen`), a Toxiproxy fault injector (`stravin-chaos`), and a local chaos Compose stack (`docker-compose.chaos.yml`) to prove correctness under concurrent load and network faults. Load and chaos verification is local-only (not run in GitHub Actions).

### Quick Commands

```bash
# Build the standalone load generator and chaos nemesis binaries
make build-loadgen
make build-chaos

# Boot the Toxiproxy + Floci + Stravin Core chaos stack (add TELEMETRY=1 for Grafana/OTel)
make chaos-up
make chaos-up TELEMETRY=1

# Run stravin-loadgen against the local stack (configurable DEVS, AGENTS, REPOS, DURATION, HTTP_URL, AUTH_TOKEN, etc.)
make load-test DEVS=4 AGENTS=6 DURATION=60s HTTP_URL=http://127.0.0.1:18080

# Tear down the chaos stack and telemetry containers/volumes
make chaos-down

# Run the automated local Load + Chaos E2E suite (with SIGINT Ctrl+C cleanup verification)
make chaos-e2e
```

Both `docker-compose.yml` and `docker-compose.chaos.yml` raise `stravin-edge` local rate limits by default (`STRAVIN_RATE_LIMIT_RPS=2000`, `STRAVIN_MAX_CONCURRENT_CLONES=256`, vs. production defaults `100` / `32`) so stress tests exercise backend daemons. Override either variable before starting Compose (e.g., `STRAVIN_RATE_LIMIT_RPS=100 STRAVIN_MAX_CONCURRENT_CLONES=32 make chaos-up`).

### `stravin-loadgen` CLI & 22 Dev/Agent Scenarios

`stravin-loadgen` simulates parallel developer (`--devs`) and AI agent (`--agents`) Git workers using deterministic seeded entropy (`--seed`), client-side `git fsck --full --strict` and SHA verification, and guaranteed OpenTelemetry flush on `SIGINT` / Ctrl+C (`--flush-timeout`, default `10s`):

```bash
./bin/stravin-loadgen \
  --devs 4 --agents 6 \
  --duration 0 \
  --seed 42 \
  --http-url http://127.0.0.1:18080 \
  --ssh-addr 127.0.0.1:12222 \
  --ssh-key /path/to/id_ed25519 \
  --tenant local --repos 3 \
  --auth-token dev:stravin-local-dev-token \
  --scenarios ""
```

Key flags: `--devs`, `--agents`, `--repos` (number of target repositories `repo-1..repo-N`, default `3`), `--duration` (`0` = continuous until Ctrl+C), `--seed`, `--scenarios` (comma-separated filter; empty runs all 22), `--http-url`, `--ssh-addr`, `--ssh-key`, `--auth-token`, `--otel-endpoint`, `--flush-timeout`, `--max-retries`, `--fail-fast`.

Registered scenarios (22 total):
- **Developer (11)**: `dev_clone_and_read`, `dev_feature_branch_pr`, `dev_incremental_thin_pack`, `dev_rebase_and_force_push`, `dev_concurrent_main_push_retry`, `dev_tag_and_release`, `dev_large_binary_asset`, `dev_branch_cleanup_cycle`, `dev_shallow_clone_unshallow`, `dev_ssh_full_lifecycle`, `dev_noop_empty_push_and_fetch`.
- **AI Agent (11)**: `agent_rapid_checkpoint_burst`, `agent_parallel_branch_fanout`, `agent_shared_branch_race`, `agent_deep_tree_refactor`, `agent_orphan_branch_artifact`, `agent_merge_commit_diamond`, `agent_cherry_pick_backport`, `agent_wasm_hook_allow_and_reject`, `agent_high_churn_compaction_trigger`, `agent_protocol_switch_http_ssh`, `agent_corrupt_ref_negative_test`.

### `stravin-chaos` & `docker-compose.chaos.yml`

`docker-compose.chaos.yml` routes Git and backend traffic through [Toxiproxy](https://github.com/Shopify/toxiproxy):
- `127.0.0.1:8474`: Toxiproxy HTTP admin API
- `127.0.0.1:18080` -> `stravin-edge:8080` (`edge_http` Smart HTTP proxy)
- `127.0.0.1:12222` -> `stravin-edge:2222` (`edge_ssh` Git SSH proxy)
- Internal proxies: `edge_to_node_grpc` (`:19090`) and `node_to_floci` (`:14566`)

`stravin-chaos` injects seeded `latency`, `bandwidth`, `reset_peer`, `timeout`, and `slow_close` toxics (plus optional `--enable-restarts` on Stravin daemons, never `floci`) and automatically clears all active toxics on `SIGINT`/`SIGTERM` exit:

```bash
./bin/stravin-chaos --toxiproxy-url http://127.0.0.1:8474 --seed 42 --interval 5s --fault-duration 3s
```

## Multi-Region Local Testing (Dual Floci)

A standalone multi-region stack (`docker-compose.multi-region.yml`) runs two regional Floci emulators (`us-east-1` on `:4566` and `us-west-2` on `:4567`), two regional Stravin Core daemon sets (`stravin-edge-a/b`, `stravin-node-a/b`, `stravin-ctrl-a/b`), and four containerized directional sync workers built from `Dockerfile.floci-test`:

- `floci-test-s3-sync-a-to-b` / `floci-test-s3-sync-b-to-a`: S3->SQS event-driven cross-region replication with `x-amz-meta-floci-replicated-from` loop guards.
- `floci-test-ddb-sync-a-to-b` / `floci-test-ddb-sync-b-to-a`: DynamoDB Streams MREC replication with `_floci_rep_origin` / `_floci_rep_ts` LWW and delete bounce suppression.

Production daemon images (`Dockerfile`) remain completely isolated from `cmd/floci-test-*`, `internal/flocitest/*`, and `service/sqs`.

```bash
# Build the standalone floci-test-s3-sync and floci-test-ddb-sync Docker images
make build-floci-test-tools

# Boot the dual-Floci multi-region stack (Region A :8080/:2222, Region B :8081/:2223)
make multi-region-up

# Tear down the multi-region stack and volumes
make multi-region-down

# Run the automated multi-region sync + push-A/fetch-B E2E suite
make multi-region-e2e
```

## Local Development Setup

Tool versions are pinned in `.tool-versions` for [asdf](https://asdf-vm.com/):

```bash
asdf install
```

### Pre-commit Hooks & Tests

```bash
make install-hooks
make pre-commit
make test
```

Hooks run Go formatting (`gofmt`, `goimports`), static analysis (`go vet`, `golangci-lint`), `go mod tidy -diff`, and repository hygiene checks. If `go` or `golangci-lint` are not installed on the host (or when `STRAVIN_FORCE_DOCKER_GO=1`), the hooks transparently fall back to Docker (`golang:1.24-bookworm` via `./scripts/go.sh` and `golangci/golangci-lint:v2.15.0`).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
