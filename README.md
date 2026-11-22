# Stravin Core

Stateless, multi-region Git storage engine in Go backed by S3 and DynamoDB, licensed under Apache 2.0.

## Quickstart (100% Local via Docker Compose + Floci)

Everything runs locally against [Floci](https://github.com/floci-dev/floci) (`AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`) with zero real AWS resources.

### 1. Boot the full stack

```bash
docker compose up -d --build
```

Services started:
- **Floci** (`:4566`): Local S3, DynamoDB, DynamoDB Streams, and KMS emulator (`floci-init` creates `git-objects-us-east-1`, `stravin-core`, `alias/stravin-local-dev`, and seeds `local/demo`).
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

Open Grafana at [http://localhost:3000](http://localhost:3000) (anonymous Admin access enabled for local dev) with pre-provisioned Prometheus, Loki, and Tempo datasources.

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
