# ADR 0001: Gate `stravin-pack` Delayed GC Deletion on S3 CRR `ReplicationStatus` and Lag Circuit Breaker

- **Status:** Accepted
- **Date:** 2026-11-21
- **Tracking Issue:** [stravin-dev/core#56](https://github.com/stravin-dev/core/issues/56) (split from multi-region data-loss umbrella [#39](https://github.com/stravin-dev/core/issues/39))

## Context

In Stravin Core, `stravin-pack` compacts small push packfiles in a repository's `home_region` using a two-phase DynamoDB manifest swap:

1. **Publish:** Upload the consolidated `.pack` and `.idx` to `home_region` S3 and insert an `ACTIVE` manifest entry.
2. **Supersede:** Transition the source pack manifest entries to `COMPACTED` and stamp `compacted_at`, `gc_after` (default +24h), and `replaced_by_pack_id`.
3. **Delayed GC (`GCSweeper`):** After `gc_after` elapses, delete the superseded `.pack` and `.idx` objects from S3 and remove their `COMPACTED` manifest entries.

In a multi-region deployment relying on asynchronous S3 Cross-Region Replication (CRR), a fixed grace window is insufficient during prolonged WAN partitions, S3 CRR stalls, or misconfigured replication rules. If `stravin-pack` deletes superseded packs in `home_region` before the replacement compacted `.pack` and `.idx` finish replicating to the secondary region's S3 bucket:

- Secondary-region readers lose origin-fallback copies of the superseded packs while the replacement pack is not yet replicated.
- A regional failover during the stall can result in permanent commit/object loss.

At the same time, we must preserve two core invariants:

1. **Zero latency impact on the live home-region push path** — pushes must never wait on cross-region S3 replication.
2. **Zero regression to single-region v1.0.0 behavior and local Docker + Floci deployments** — no `if localMode` forks and no extra S3 API calls when single-region mode is active.

## Decision

### 1. Opt-In Multi-Region Replication Gate

Delayed GC deletion in `stravin-pack` is gated by an opt-in `ReplicationGate` configured via environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `STRAVIN_PACK_REQUIRE_CRR_REPLICATION` | `false` | Enables the S3 CRR `HeadObject` replication gate and circuit breaker in `GCSweeper`. When `false`, `GCSweeper` makes zero `HeadObject` calls and single-region behavior is unchanged. |
| `STRAVIN_PACK_CRR_BREAKER_THRESHOLD` | `5` | Consecutive non-completed/failed replication checks before the CRR-lag circuit breaker trips. |
| `STRAVIN_PACK_CRR_BREAKER_COOLDOWN` | `15m` | Cooldown window during which all GC deletions are paused after the breaker trips. |

### 2. AWS S3 `HeadObject` `ReplicationStatus` Semantics

`internal/storage/s3` exposes `ObjectMeta.ReplicationStatus` directly from the standard AWS SDK v2 `HeadObjectOutput.ReplicationStatus` (`x-amz-replication-status` HTTP response header).

AWS S3 defines five enum values (in `github.com/aws/aws-sdk-go-v2/service/s3/types.ReplicationStatus`), plus the absent/empty header:

| `ReplicationStatus` | AWS S3 Meaning | Gate Decision (when enabled) |
| --- | --- | --- |
| `COMPLETED` | Multi-destination / S3 Batch / newer replication rules: object has been replicated to all configured destinations. | **Allow** |
| `COMPLETE` | Single-destination S3 CRR rules: object has been replicated to the destination bucket. | **Allow** |
| `PENDING` | Replication is still queued or in progress. | **Skip (fail-closed)** + increment breaker |
| `FAILED` | Replication failed (permissions, KMS key policy, bucket mismatch, etc.). | **Skip (fail-closed)** + increment breaker |
| `REPLICA` | Object is a destination replica written *by* CRR, not a source object confirmed replicated outward. | **Skip (fail-closed)** + increment breaker |
| `""` (empty / header absent) | CRR is not enabled on the bucket, the object did not match a replication rule, or the bucket is a single-region / local Floci bucket. | **Skip (fail-closed)** + increment breaker |
| Any `HeadObject` error (404, 5xx, timeout) | Cannot verify replication durability. | **Skip (fail-closed)** + increment breaker |

Both `COMPLETED` and `COMPLETE` are accepted because AWS S3 returns `COMPLETE` for standard single-destination CRR rules and `COMPLETED` for multi-destination/Batch CRR rules. Every other status or error fails closed so GC can never delete a superseded pack on an ambiguous signal.

#### Floci Compatibility

Local Docker + Floci emulators do not populate `x-amz-replication-status` on `HeadObject` responses. Because `STRAVIN_PACK_REQUIRE_CRR_REPLICATION` defaults to `false`, single-region local Docker + Floci runs require no `if localMode` forks and incur zero `HeadObject` overhead. Multi-region Floci tests that enable the gate either see fail-closed protection or use a real/simulated CRR status header.

### 3. Replacement Pack Verification (`.pack` AND `.idx`)

During `Engine.supersedePhase`, `DDBManifestGCAdapter` stamps `replaced_by_pack_id` alongside `compacted_at` and `gc_after` on each superseded `COMPACTED` manifest entry.

When `GCSweeper` evaluates a `COMPACTED` entry past `gc_after` with `ReplicationGate` enabled:

1. It resolves `replaced_by_pack_id` from the manifest entry. (For legacy `COMPACTED` entries created before this field existed, it falls back to verifying all currently `ACTIVE` packs in the repository; if no `ACTIVE` packs exist, it fails closed.)
2. It issues `HeadObject` for **both** the replacement `.pack` and `.idx` keys. Because S3 CRR replicates objects independently, verifying both ensures a secondary-region reader never sees a `.pack` without its `.idx` (or vice versa).
3. Within a single `SweepRepo` pass, replacement packs confirmed `COMPLETED`/`COMPLETE` are cached in memory so many superseded packs sharing one replacement do not trigger redundant `HeadObject` calls.

### 4. CRR-Lag Circuit Breaker and Observability

During a prolonged WAN partition or CRR misconfiguration, individual `HeadObject` checks would repeatedly observe `PENDING`, `FAILED`, or timeouts across every repository and pack.

`CRRCircuitBreaker` prevents S3 API thrashing and halts all GC deletions when replication is unhealthy:

- Each non-completed status or `HeadObject` error increments a consecutive failure counter; any successful `COMPLETED`/`COMPLETE` verification resets the counter to `0`.
- When consecutive failures reach `STRAVIN_PACK_CRR_BREAKER_THRESHOLD` (default `5`), the breaker **opens** for `STRAVIN_PACK_CRR_BREAKER_COOLDOWN` (default `15m`).
- While open, `GCSweeper` pauses **all** GC deletions (both `COMPACTED` packs in Pass 1 and orphaned packs in Pass 2) without issuing further `HeadObject` calls, and logs structured warnings.
- Once the cooldown elapses, the breaker automatically closes and resets the failure streak so the next sweep probes S3 CRR again.

#### OpenTelemetry Metrics

- `stravin_pack_gc_crr_blocked_total` (`Int64Counter`, attribute `reason` in `{pending, failed, replica, missing_status, unknown_status, head_error, no_replacement, breaker_open}`): increments whenever a GC deletion is blocked.
- `stravin_pack_gc_crr_breaker_open` (`Int64Gauge`, `1` when open, `0` when closed): alerts operators when sustained CRR lag has paused GC.

### 5. S3 CRR Delete-Marker Replication Must Never Be Enabled

S3 bucket replication rules for Stravin object buckets **must keep `DeleteMarkerReplication` set to `Disabled`** (AWS S3's default for V2 replication rules):

- Packfiles are immutable and content-addressed (`{prefix}/{tenant}/{repo}/packs/{sha256}.{pack,idx}`).
- When `home_region` GC deletes a superseded pack after verifying its replacement is `COMPLETED` in the peer region, that deletion is a local storage-reclamation action. Replicating delete markers would create a race window where a delete marker could hide or remove objects needed by in-flight secondary-region reads or during an asymmetric failover.
- Each region's `stravin-pack` GC reaps superseded objects locally once its own manifest and replication checks pass.

## Consequences

- **Data-loss protection:** Eliminates the compaction-GC data-loss window during WAN partitions and S3 CRR stalls by refusing to delete superseded packs until both `.pack` and `.idx` of the replacement pack are confirmed replicated (`COMPLETED`/`COMPLETE`).
- **Push latency:** Unchanged (0 ms added). `ReplicationGate` runs strictly inside the background delayed-GC loop.
- **Single-region performance:** Unchanged when `STRAVIN_PACK_REQUIRE_CRR_REPLICATION=false` (default).
- **Storage trade-off during prolonged CRR outages:** Superseded packs remain in S3 while CRR is degraded or the circuit breaker is open, trading temporary S3 storage cost for zero commit loss. Operators monitor `stravin_pack_gc_crr_breaker_open` and `stravin_pack_gc_crr_blocked_total` to remediate CRR stalls.
