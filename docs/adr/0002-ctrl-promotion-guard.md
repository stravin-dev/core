# ADR 0002: Guard `stravin-ctrl` Failover Promotion with Witness Quorum and Operator Gate

- **Status:** Accepted
- **Date:** 2026-11-21
- **Tracking Issue:** [stravin-dev/core#57](https://github.com/stravin-dev/core/issues/57) (split from multi-region data-loss umbrella [#39](https://github.com/stravin-dev/core/issues/39))

## Context

In Stravin Core, each repository has a single authoritative `home_region` stored in DynamoDB `META` alongside a monotonically increasing `failover_epoch` and `write_state`. Normal Git pushes execute strictly inside `home_region` with zero cross-region synchronous coordination.

In a multi-region deployment backed by DynamoDB Global Tables (Multi-Region Eventual Consistency, MREC), an **asymmetric network split** creates a silent data-loss hazard if a secondary region relies solely on local health probes to promote itself:

1. `home_region` (`us-east-1`) remains healthy and continues accepting pushes at `failover_epoch = E`.
2. A WAN partition isolates the secondary region (`us-west-2`) from `us-east-1` while both regions remain reachable by different subsets of clients.
3. If `stravin-ctrl` in `us-west-2` infers from local probe timeouts that `us-east-1` is dead and unilaterally advances `failover_epoch` to `E + 1` and transitions to `ACTIVE`, both regions accept concurrent pushes to the same refs.
4. When the WAN split heals, DynamoDB Global Tables MREC resolves conflicting `REF` and `META` writes via **Last-Writer-Wins (LWW)**, silently clobbering committed ref tips from one side of the split.

We must prevent false failovers while preserving two core invariants:

1. **Zero latency impact on normal home-region pushes** — pushes never contact witnesses or wait on cross-region quorums.
2. **Zero regression to single-region v1.0.0 deployments and local Docker + Floci E2E tests** — when multi-region promotion protection is not enabled, `POST /v1/epoch/advance` and `POST /v1/lease/fence` behave identically to v1.0.0.

## Decision

### 1. Opt-In Multi-Region `PromotionGuard` (Strategy + Composite)

`internal/ctrl/promotion_guard.go` introduces a pluggable `PromotionGuard` interface (GoF Strategy) evaluated inside `LeaseManager.FenceRegion` **before** advancing `failover_epoch` or transitioning a region into `ACTIVE`:

```go
type PromotionGuard interface {
    AuthorizePromotion(ctx context.Context, req PromotionRequest) (PromotionDecision, error)
}
```

Three concrete guards are provided:

1. **`OperatorPromotionGuard`** — requires a non-empty `operator_id` and a constant-time (`crypto/subtle.ConstantTimeCompare`) match against `STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN` (supplied in the JSON body as `approval_token` or via `X-Stravin-Approval-Token`). Fails closed (`403 Forbidden`) when the configured token is empty or the supplied credentials are missing or invalid.
2. **`WitnessQuorumGuard`** — concurrently queries independent witness HTTP endpoints (`STRAVIN_CTRL_WITNESS_ENDPOINTS`) using a strict timeout (`STRAVIN_CTRL_WITNESS_TIMEOUT`, default `3s`) and `http.Transport{Proxy: nil}` so environment proxies cannot intercept or misroute witness checks.
   - Each witness is queried via `GET <endpoint>?region=<home_region>&target_region=<region>&epoch=<current_epoch>` and returns JSON:
     ```json
     {"primary_fenced": true, "primary_healthy": false, "state": "fenced"}
     ```
   - Promotion proceeds only when at least `STRAVIN_CTRL_WITNESS_QUORUM` (default strict majority `floor(N/2)+1`) witnesses return HTTP 200 with `primary_fenced: true` and not healthy.
   - If any witness reports the primary healthy and quorum is not reached, promotion fails closed with `ErrPrimaryHealthy` (`409 Conflict`).
   - If witnesses are unreachable, time out, or return non-200/invalid JSON and fenced votes are below quorum, promotion fails closed with `ErrPromotionDenied` (`403 Forbidden`).
3. **`CompositePromotionGuard`** (GoF Composite) — combines child guards under a configurable policy (`STRAVIN_CTRL_PROMOTION_GUARD_POLICY`):
   - `require_all` (**default**, AND): every configured guard must authorize the promotion.
   - `witness_or_operator` (OR): either a valid witness quorum OR a valid operator approval authorizes the promotion (allowing an incident commander to override a degraded witness fleet with a recorded audit trail).

### 2. Configuration and Fail-Closed Defaults

| Variable | Default | Purpose |
| --- | --- | --- |
| `STRAVIN_CTRL_REQUIRE_PROMOTION_GUARD` | `false` | Enables `PromotionGuard` on `stravin-ctrl` epoch advance / promotion. When `false`, single-region v1.0.0 behavior is 100% unchanged. |
| `STRAVIN_CTRL_PROMOTION_GUARD_POLICY` | `require_all` | Composite guard policy: `require_all` (AND) or `witness_or_operator` (OR). |
| `STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN` | `""` | Shared secret token required by `OperatorPromotionGuard`. |
| `STRAVIN_CTRL_WITNESS_ENDPOINTS` | `""` | Comma-separated HTTP(S) URLs of independent third-region/global witness endpoints. |
| `STRAVIN_CTRL_WITNESS_QUORUM` | `0` (`floor(N/2)+1`) | Minimum number of witnesses that must confirm `home_region` is fenced. |
| `STRAVIN_CTRL_WITNESS_TIMEOUT` | `3s` | Per-witness HTTP timeout. |

When `STRAVIN_CTRL_REQUIRE_PROMOTION_GUARD=true`, `stravin-ctrl` refuses to start unless at least one of `STRAVIN_CTRL_WITNESS_ENDPOINTS` or `STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN` is configured (fail-closed startup).

### 3. Immutable Audit Trail and Telemetry

Every `PromotionGuard` evaluation (both `allowed` and `denied`) is persisted to `DDBAuditStore` via `RecordPromotionAudit`:

- Repo-scoped promotion: `PK=T#{tenant}#REPO#{repo}#AUDIT`, `SK=PROMOTION#{ulid}`
- Region-scoped promotion: `PK=REGION#{region}#AUDIT`, `SK=PROMOTION#{ulid}`
- Records `guard_type`, `allowed`, `decision_reason`, `operator_id`, `witness_votes`, `quorum_needed`, `quorum_reached`, `home_region`, `current_epoch`, and `target_epoch`.
- `PromotionRequest.ApprovalToken` is tagged `json:"-"` and is **never** written to DynamoDB audit records, logs, or HTTP responses.

An OpenTelemetry counter is emitted on every evaluation:

- `stravin_ctrl_promotion_guard_total{region, outcome}` where `outcome` is `allowed` or `denied`.

### 4. Operational Failover Procedure

1. **Detect suspected primary outage** via external monitoring across regions and third-region witnesses.
2. **Verify primary fencing**: ensure `home_region` ingress is drained/fenced (or confirmed unreachable by the witness fleet).
3. **Invoke promotion** against the target secondary `stravin-ctrl` with admin Bearer auth plus operator identity and approval token:
   ```bash
   curl -X POST https://ctrl-west.internal/v1/epoch/advance \
     -H "Authorization: Bearer ${STRAVIN_CTRL_ADMIN_TOKEN}" \
     -H "Content-Type: application/json" \
     -d '{
       "tenant_id": "acme",
       "repo_id": "core",
       "home_region": "us-east-1",
       "mode": "ACTIVE",
       "operator_id": "oncall@stravin.dev",
       "approval_token": "'"${STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN}"'",
       "reason": "us-east-1 confirmed fenced by witnesses"
     }'
   ```
4. **Run post-failover scrub** (`POST /v1/scrub`) on affected repositories to verify S3 pack integrity and deterministically rewind any refs missing un-replicated packs.

## Consequences

- **Split-brain prevention:** Asymmetric WAN splits where `home_region` is still healthy are blocked with `409 Conflict` / `403 Forbidden` before `failover_epoch` advances or `ACTIVE` writes open.
- **Push latency:** Unchanged (`0 ms` added to home-region push path).
- **Single-region compatibility:** Unchanged when `STRAVIN_CTRL_REQUIRE_PROMOTION_GUARD=false`.
- **Operational requirement:** Multi-region deployments must provision independent witness endpoints (e.g., in a third region or neutral control plane) and manage `STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN` via secret rotation.
