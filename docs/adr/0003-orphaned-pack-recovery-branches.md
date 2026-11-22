# ADR 0003: Automated Orphaned-Pack Recovery Branches (`refs/stravin-recovery/*`)

- **Status:** Accepted
- **Date:** 2026-11-21
- **Tracking Issue:** [stravin-dev/core#58](https://github.com/stravin-dev/core/issues/58) (split from multi-region data-loss umbrella [#39](https://github.com/stravin-dev/core/issues/39))

## Context

In Stravin Core, normal Git pushes execute strictly inside each repository's `home_region` with zero cross-region synchronous coordination. During an **ungraceful disaster failover**, commits pushed inside the bounded RPO window (before S3 CRR completes) are missing from the newly promoted secondary region's S3 bucket. When `stravin-ctrl` runs its post-failover S3 integrity scrub (`POST /v1/scrub`), those refs are deterministically rewound via `Ref History` to the last intact replicated tip.

When the old primary region recovers:

1. Its S3 bucket still contains the un-replicated `.pack` and `.idx` objects pushed inside the RPO window.
2. DynamoDB `Ref History` (`RH#{s}`, 7-day TTL) still records the rewound/diverged commit OIDs and their backing `pack_id`.
3. Without automated reconciliation, those commits remain unreachable from `Ref State` (`RS#{s}`) and their packfiles are eventually reaped by delayed GC, turning a recoverable RPO window into silent data loss.

We must surface recoverable RPO commits automatically while preserving three invariants:

1. **Non-destructive:** Active branches (`refs/heads/*`, `refs/tags/*`) are **never** mutated or force-updated.
2. **Idempotent:** Repeated reconciliation runs never fail or overwrite existing recovery refs.
3. **Zero push-path latency:** Recovery reconciliation runs strictly out-of-band in `stravin-ctrl` (on `DRAINING`/`READ-ONLY` recovery boot or via authenticated admin API).

## Decision

### 1. Recovery Ref Naming (`refs/stravin-recovery/*`)

Recoverable diverged tips are published under the reserved namespace:

```text
refs/stravin-recovery/<sanitized-ref>-<short-oid>
```

- `<sanitized-ref>` strips a leading `refs/` prefix, collapses `..` and any characters outside `[A-Za-z0-9._-]` into `-`, collapses consecutive dashes, and trims leading/trailing `-` and `.` (falling back to `ref` if empty).
- `<short-oid>` is the lowercase first 12 hexadecimal characters of the commit OID.
- Example: a rewound commit `abcdef012345...` on `refs/heads/main` is published as `refs/stravin-recovery/heads-main-abcdef012345`.

### 2. Divergence Detection and S3 Verification (`RecoveryReconciler`)

`internal/ctrl/recovery.go` implements `RecoveryReconciler` (GoF Command pattern via `RecoveryCommand`) over segregated `RecoveryDDBStore`, `RecoveryS3Store`, and `RecoveryAuditStore` ports:

1. **Enumerate source refs:** List current `Ref State` entries (plus optional `target_refs` for branches deleted during rewind), ignoring any existing `refs/stravin-recovery/*` refs.
2. **Trace current tip lineage:** For each source ref, sort `Ref History` by ascending ULID and record each commit OID's **first-seen** `PrevOID`. Walking backwards from the current `Ref State.OID` along first-seen parents yields the active lineage — ensuring rewind entries (which re-point to an earlier OID) never falsely mask rewound commits as ancestors, while normal linear push history produces zero false positives.
3. **Select diverged tips:** Any history entry with a non-empty `pack_id` whose OID is outside the current lineage — and is not itself the first-seen parent of another diverged entry — is selected as a candidate recovery tip.
4. **Verify S3 pack integrity:** Bounded parallel `HeadObject` checks verify **both** `.pack` and `.idx` objects exist in S3. Missing packs (`s3.ErrNotFound`) are skipped cleanly without creating dangling refs; transient S3 errors fail closed and propagate.

### 3. Idempotent Publication and Delayed GC Protection

Each verified candidate is published via `ddb.CommitPush` with `OldOID: ddb.ZeroOID`, which enforces DynamoDB's `ConditionExpression: attribute_not_exists(oid)`:

- If the recovery ref already exists (either observed during `ListRefs` or raced by a concurrent reconciler returning `ddb.ErrRefConflict`), the reconciler skips it cleanly without modifying the existing ref or failing the run.
- **Dual-layer delayed GC protection:**
  1. **Manifest preservation:** `CommitPush` atomically writes an `ACTIVE` `PackManifest` entry for the verified `pack_id` (using the `SizeBytes` returned by `HeadObject`) and sets `RefState.PackID` on the recovery ref.
  2. **Sweeper defense-in-depth:** `internal/compaction/gc.go` (`GCSweeper`) accepts an optional `Refs RefLister` + `RefShards`. Before evaluating `COMPACTED` or orphaned S3 packs, `SweepRepo` adds every `pack_id` referenced by an active `refs/stravin-recovery/*` ref to `activePackIDs`, preventing delayed GC from reaping recovery packs even if their manifest entry was previously superseded.

### 4. Admin API, Recovery Boot, Audit Trail, and Telemetry

- **Authenticated HTTP endpoint:** `POST /v1/recovery/reconcile` in `internal/ctrl/http.go`, protected by the existing constant-time Bearer `requireAdminAuth` middleware (`401 Unauthorized` without valid token).
- **Optional recovery-boot wiring:** `cmd/stravin-ctrl/main.go` supports `STRAVIN_CTRL_RECOVERY_ON_BOOT=true` and `STRAVIN_CTRL_RECOVERY_REPOS=tenant/repo,...`, running reconciliation automatically on boot only when the regional write lease is in `DRAINING` or `READ_ONLY` mode.
- **Immutable audit trail:** `DDBAuditStore.RecordRecoveryAudit` persists `PK=T#{tenant}#REPO#{repo}#AUDIT`, `SK=RECOVERY#{ulid}` (`attribute_not_exists(PK)`) for every published recovery ref.
- **OpenTelemetry counters:**
  - `stravin_ctrl_recovery_refs_published_total{region}`
  - `stravin_ctrl_recovery_runs_total{region, result}`

### 5. Operational Recovery Procedure

1. When the former primary region returns in `DRAINING` or `READ_ONLY` mode, `stravin-ctrl` reconciles configured repos on boot or on demand:
   ```bash
   curl -X POST https://ctrl-east.internal/v1/recovery/reconcile \
     -H "Authorization: Bearer ${STRAVIN_CTRL_ADMIN_TOKEN}" \
     -H "Content-Type: application/json" \
     -d '{"tenant_id": "acme", "repo_id": "core"}'
   ```
2. Developers inspect recovered commits via `git fetch origin 'refs/stravin-recovery/*:refs/remotes/origin/stravin-recovery/*'` and cherry-pick or merge as needed.
3. Once recovered commits are integrated or discarded, operators delete the `refs/stravin-recovery/*` ref so delayed GC can reclaim the backing packfile after its grace window.

## Consequences

- **RPO data preservation:** Commits stranded in the old primary's S3 bucket during ungraceful failover are automatically surfaced and protected from GC.
- **Non-destructive & idempotent:** Active branches are never touched; repeated runs are safe no-ops.
- **Zero push-path overhead:** Normal home-region pushes are completely unaffected.
