package compaction

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
)

func TestCRRCircuitBreaker_TripCooldownResetAndConcurrency(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 16, 0, 0, 0, time.UTC))
	breaker := NewCRRCircuitBreaker(CRRCircuitBreakerConfig{
		Threshold: 3,
		Cooldown:  10 * time.Minute,
		Clock:     clk,
	})

	if breaker.IsOpen() {
		t.Fatal("breaker must start closed")
	}
	if breaker.RecordFailure() {
		t.Fatal("breaker must not trip on failure 1/3")
	}
	if breaker.RecordFailure() {
		t.Fatal("breaker must not trip on failure 2/3")
	}
	// Success before threshold resets failure streak.
	breaker.RecordSuccess()
	if got := breaker.ConsecutiveFailures(); got != 0 {
		t.Fatalf("ConsecutiveFailures after success = %d, want 0", got)
	}

	// 3 consecutive failures trip the breaker.
	for i := 1; i <= 2; i++ {
		if breaker.RecordFailure() {
			t.Fatalf("breaker tripped early at failure %d/3", i)
		}
	}
	if !breaker.RecordFailure() {
		t.Fatal("expected breaker to trip on failure 3/3")
	}
	if !breaker.IsOpen() {
		t.Fatal("expected IsOpen() == true immediately after trip")
	}

	// Still open before cooldown expires.
	clk.Advance(9 * time.Minute)
	if !breaker.IsOpen() {
		t.Fatal("expected breaker to remain open before cooldown expires")
	}

	// Automatically resets once cooldown elapses.
	clk.Advance(1 * time.Minute)
	if breaker.IsOpen() {
		t.Fatal("expected breaker to close after cooldown elapses")
	}
	if got := breaker.ConsecutiveFailures(); got != 0 {
		t.Fatalf("ConsecutiveFailures after cooldown reset = %d, want 0", got)
	}

	// Exercise concurrent access under -race.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if (id+j)%3 == 0 {
					breaker.RecordSuccess()
				} else {
					breaker.RecordFailure()
				}
				_ = breaker.IsOpen()
				_ = breaker.ConsecutiveFailures()
			}
		}(i)
	}
	wg.Wait()
}

func TestGCSweeper_CRRReplicationGate_StatusMatrix(t *testing.T) {
	cases := []struct {
		name              string
		packStatus        string
		idxStatus         string
		headErr           error
		wantReaped        int
		wantSkippedRep    int
		wantExists        bool
		wantBlockedReason string
	}{
		{
			name:           "completed_allows_delete",
			packStatus:     s3.ReplicationStatusCompleted,
			idxStatus:      s3.ReplicationStatusCompleted,
			wantReaped:     1,
			wantSkippedRep: 0,
			wantExists:     false,
		},
		{
			name:           "complete_allows_delete",
			packStatus:     s3.ReplicationStatusComplete,
			idxStatus:      s3.ReplicationStatusComplete,
			wantReaped:     1,
			wantSkippedRep: 0,
			wantExists:     false,
		},
		{
			name:              "pending_pack_skips_delete",
			packStatus:        s3.ReplicationStatusPending,
			idxStatus:         s3.ReplicationStatusCompleted,
			wantReaped:        0,
			wantSkippedRep:    1,
			wantExists:        true,
			wantBlockedReason: "pending",
		},
		{
			name:              "pending_idx_skips_delete",
			packStatus:        s3.ReplicationStatusCompleted,
			idxStatus:         s3.ReplicationStatusPending,
			wantReaped:        0,
			wantSkippedRep:    1,
			wantExists:        true,
			wantBlockedReason: "pending",
		},
		{
			name:              "failed_skips_delete",
			packStatus:        s3.ReplicationStatusFailed,
			idxStatus:         s3.ReplicationStatusFailed,
			wantReaped:        0,
			wantSkippedRep:    1,
			wantExists:        true,
			wantBlockedReason: "failed",
		},
		{
			name:              "replica_fail_closed_skips_delete",
			packStatus:        s3.ReplicationStatusReplica,
			idxStatus:         s3.ReplicationStatusReplica,
			wantReaped:        0,
			wantSkippedRep:    1,
			wantExists:        true,
			wantBlockedReason: "replica",
		},
		{
			name:              "empty_status_fail_closed_skips_delete",
			packStatus:        "",
			idxStatus:         "",
			wantReaped:        0,
			wantSkippedRep:    1,
			wantExists:        true,
			wantBlockedReason: "missing_status",
		},
		{
			name:              "head_error_fail_closed_skips_delete",
			headErr:           errors.New("s3 head timeout"),
			wantReaped:        0,
			wantSkippedRep:    1,
			wantExists:        true,
			wantBlockedReason: "head_error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newStaticClock(time.Date(2026, 11, 21, 17, 0, 0, 0, time.UTC))
			rk := mustTestRepoKey(t, "tenant-crr", "repo-crr")
			manifests := newFakeManifestStore(1)
			objects := newFakePackObjectStore(clk)

			tel, err := telemetry.NewInMemoryProvider("stravin-pack")
			if err != nil {
				t.Fatalf("NewInMemoryProvider: %v", err)
			}
			metrics, err := NewMetrics(tel)
			if err != nil {
				t.Fatalf("NewMetrics: %v", err)
			}

			replacementSHA := strings.Repeat("1", 64)
			supersededSHA := strings.Repeat("2", 64)
			ctx := context.Background()

			if err := objects.PutPack(ctx, rk, replacementSHA, []byte("PACK-new"), []byte("IDX-new")); err != nil {
				t.Fatalf("PutPack(replacement): %v", err)
			}
			if err := objects.PutPack(ctx, rk, supersededSHA, []byte("PACK-old"), []byte("IDX-old")); err != nil {
				t.Fatalf("PutPack(superseded): %v", err)
			}

			replPackKey, _ := s3.PackKey(rk, replacementSHA)
			replIdxKey, _ := s3.IndexKey(rk, replacementSHA)
			objects.setReplicationStatus(replPackKey, tc.packStatus)
			objects.setReplicationStatus(replIdxKey, tc.idxStatus)
			if tc.headErr != nil {
				objects.headErr = tc.headErr
			}

			manifests.put(ddb.PackManifest{
				Repo: rk, ULID: "01J00000000000000000000002", PackID: replacementSHA,
				SizeBytes: 120, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
			})
			superseded := ddb.PackManifest{
				Repo: rk, ULID: "01J00000000000000000000001", PackID: supersededSHA,
				SizeBytes: 60, OriginRegion: "us-east-1", Status: ddb.PackStatusCompacted, CreatedAt: clk.Now(),
			}
			manifests.put(superseded)
			_ = manifests.MarkGCEligibleWithReplacement(ctx, rk, 1, []ddb.PackManifest{superseded}, clk.Now(), clk.Now().Add(10*time.Millisecond), replacementSHA)

			breaker := NewCRRCircuitBreaker(CRRCircuitBreakerConfig{
				Threshold: 5,
				Cooldown:  15 * time.Minute,
				Clock:     clk,
			})
			gate, err := NewS3ReplicationGate(S3ReplicationGateConfig{
				Objects: objects,
				Breaker: breaker,
				Metrics: metrics,
			})
			if err != nil {
				t.Fatalf("NewS3ReplicationGate: %v", err)
			}

			sweeper, err := NewGCSweeper(GCSweeperConfig{
				Manifests:       manifests,
				ManifestGC:      manifests,
				Objects:         objects,
				ReplicationGate: gate,
				GraceWindow:     10 * time.Millisecond,
				Clock:           clk,
				Metrics:         metrics,
			})
			if err != nil {
				t.Fatalf("NewGCSweeper: %v", err)
			}

			clk.Advance(20 * time.Millisecond)

			res, err := sweeper.SweepRepo(ctx, rk, 1)
			if err != nil {
				t.Fatalf("SweepRepo: %v", err)
			}
			if res.CompactedReaped != tc.wantReaped || res.SkippedReplication != tc.wantSkippedRep {
				t.Fatalf("GCResult = %+v, want CompactedReaped=%d SkippedReplication=%d", res, tc.wantReaped, tc.wantSkippedRep)
			}

			supPackKey, _ := s3.PackKey(rk, supersededSHA)
			if objects.exists(supPackKey) != tc.wantExists {
				t.Fatalf("superseded pack exists = %v, want %v", objects.exists(supPackKey), tc.wantExists)
			}

			if tc.wantBlockedReason != "" {
				assertCounterMetricValue(t, tel, "stravin_pack_gc_crr_blocked_total", "reason", tc.wantBlockedReason, 1)
			}
		})
	}
}

func TestGCSweeper_CRRCircuitBreakerTripsPausesAllGCAndResetsAfterCooldown(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-brk", "repo-brk")
	manifests := newFakeManifestStore(1)
	objects := newFakePackObjectStore(clk)

	tel, err := telemetry.NewInMemoryProvider("stravin-pack")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	replSHA := strings.Repeat("a", 64)
	old1SHA := strings.Repeat("b", 64)
	old2SHA := strings.Repeat("c", 64)
	old3SHA := strings.Repeat("d", 64)
	orphanSHA := strings.Repeat("e", 64)
	ctx := context.Background()

	for _, sha := range []string{replSHA, old1SHA, old2SHA, old3SHA, orphanSHA} {
		if err := objects.PutPack(ctx, rk, sha, []byte("PACK-"+sha[:8]), []byte("IDX-"+sha[:8])); err != nil {
			t.Fatalf("PutPack(%s): %v", sha[:8], err)
		}
	}

	replPackKey, _ := s3.PackKey(rk, replSHA)
	replIdxKey, _ := s3.IndexKey(rk, replSHA)
	objects.setReplicationStatus(replPackKey, s3.ReplicationStatusFailed)
	objects.setReplicationStatus(replIdxKey, s3.ReplicationStatusFailed)

	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000009", PackID: replSHA,
		SizeBytes: 300, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})

	var compacted []ddb.PackManifest
	for i, sha := range []string{old1SHA, old2SHA, old3SHA} {
		m := ddb.PackManifest{
			Repo: rk, ULID: "01J0000000000000000000000" + string(rune('1'+i)), PackID: sha,
			SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusCompacted, CreatedAt: clk.Now(),
		}
		manifests.put(m)
		compacted = append(compacted, m)
	}
	_ = manifests.MarkGCEligibleWithReplacement(ctx, rk, 1, compacted, clk.Now(), clk.Now().Add(10*time.Millisecond), replSHA)

	breaker := NewCRRCircuitBreaker(CRRCircuitBreakerConfig{
		Threshold: 2,
		Cooldown:  10 * time.Minute,
		Clock:     clk,
	})
	gate, err := NewS3ReplicationGate(S3ReplicationGateConfig{
		Objects: objects,
		Breaker: breaker,
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("NewS3ReplicationGate: %v", err)
	}

	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:         manifests,
		ManifestGC:        manifests,
		Objects:           objects,
		ReplicationGate:   gate,
		GraceWindow:       10 * time.Millisecond,
		OrphanGraceWindow: 10 * time.Millisecond,
		Clock:             clk,
		Metrics:           metrics,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	// Advance past grace window. 2 failures trip breaker; 3rd compacted pack + orphan pack are paused.
	clk.Advance(20 * time.Millisecond)
	res1, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo (tripping): %v", err)
	}
	if res1.CompactedReaped != 0 || res1.OrphanedReaped != 0 || res1.SkippedReplication < 3 {
		t.Fatalf("expected breaker trip to pause all deletions, got %+v", res1)
	}
	if !breaker.IsOpen() {
		t.Fatal("expected CRR circuit breaker to be open")
	}
	assertGaugeMetricValue(t, tel, "stravin_pack_gc_crr_breaker_open", 1)

	// Even if replication recovers while cooldown is still active, breaker stays open and skips deletions.
	objects.setReplicationStatus(replPackKey, s3.ReplicationStatusCompleted)
	objects.setReplicationStatus(replIdxKey, s3.ReplicationStatusCompleted)
	clk.Advance(5 * time.Minute)

	res2, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo (during cooldown): %v", err)
	}
	if res2.CompactedReaped != 0 || res2.OrphanedReaped != 0 {
		t.Fatalf("expected zero deletions during cooldown, got %+v", res2)
	}

	// After cooldown expires, breaker closes and all 3 compacted + 1 orphan pack are reaped.
	clk.Advance(6 * time.Minute)
	res3, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo (post-cooldown): %v", err)
	}
	if res3.CompactedReaped != 3 || res3.OrphanedReaped != 1 || res3.SkippedReplication != 0 {
		t.Fatalf("expected full reap post-cooldown, got %+v", res3)
	}
	if breaker.IsOpen() {
		t.Fatal("expected breaker closed after successful post-cooldown sweep")
	}
	assertGaugeMetricValue(t, tel, "stravin_pack_gc_crr_breaker_open", 0)
}

func TestGCSweeper_DisabledModeUnchangedForSingleRegion(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 19, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-sr", "repo-sr")
	manifests := newFakeManifestStore(1)
	objects := newFakePackObjectStore(clk)
	ctx := context.Background()

	activeSHA := strings.Repeat("a", 64)
	compactedSHA := strings.Repeat("b", 64)
	_ = objects.PutPack(ctx, rk, activeSHA, []byte("PACK-active"), []byte("IDX-active"))
	_ = objects.PutPack(ctx, rk, compactedSHA, []byte("PACK-old"), []byte("IDX-old"))
	// Empty ReplicationStatus (single-region S3 / Floci) and HeadObject error if ever called.
	objects.headErr = errors.New("HeadObject must not be called when gate is disabled")

	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000002", PackID: activeSHA,
		SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})
	old := ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000001", PackID: compactedSHA,
		SizeBytes: 50, OriginRegion: "us-east-1", Status: ddb.PackStatusCompacted, CreatedAt: clk.Now(),
	}
	manifests.put(old)
	_ = manifests.MarkGCEligible(ctx, rk, 1, []ddb.PackManifest{old}, clk.Now(), clk.Now().Add(10*time.Millisecond))

	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:   manifests,
		ManifestGC:  manifests,
		Objects:     objects,
		GraceWindow: 10 * time.Millisecond,
		Clock:       clk,
		// ReplicationGate intentionally nil (single-region default).
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	clk.Advance(20 * time.Millisecond)
	res, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo: %v", err)
	}
	if res.CompactedReaped != 1 || res.SkippedReplication != 0 {
		t.Fatalf("unexpected single-region GCResult: %+v", res)
	}
	if objects.headCalls != 0 {
		t.Fatalf("expected 0 HeadObject calls when gate disabled, got %d", objects.headCalls)
	}
}

func TestGCSweeper_LegacyEntryWithoutReplacementFallsBackToActivePacks(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 19, 30, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-leg", "repo-leg")
	manifests := newFakeManifestStore(1)
	objects := newFakePackObjectStore(clk)
	ctx := context.Background()

	activeSHA := strings.Repeat("a", 64)
	compactedSHA := strings.Repeat("b", 64)
	_ = objects.PutPack(ctx, rk, activeSHA, []byte("PACK-active"), []byte("IDX-active"))
	_ = objects.PutPack(ctx, rk, compactedSHA, []byte("PACK-old"), []byte("IDX-old"))

	activePackKey, _ := s3.PackKey(rk, activeSHA)
	activeIdxKey, _ := s3.IndexKey(rk, activeSHA)
	objects.setReplicationStatus(activePackKey, s3.ReplicationStatusPending)
	objects.setReplicationStatus(activeIdxKey, s3.ReplicationStatusCompleted)

	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000002", PackID: activeSHA,
		SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})
	old := ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000001", PackID: compactedSHA,
		SizeBytes: 50, OriginRegion: "us-east-1", Status: ddb.PackStatusCompacted, CreatedAt: clk.Now(),
	}
	manifests.put(old)
	// MarkGCEligible without replacement pack ID (legacy entry).
	_ = manifests.MarkGCEligible(ctx, rk, 1, []ddb.PackManifest{old}, clk.Now(), clk.Now().Add(10*time.Millisecond))

	gate, err := NewS3ReplicationGate(S3ReplicationGateConfig{
		Objects: objects,
		Breaker: NewCRRCircuitBreaker(CRRCircuitBreakerConfig{Threshold: 5, Cooldown: time.Minute, Clock: clk}),
	})
	if err != nil {
		t.Fatalf("NewS3ReplicationGate: %v", err)
	}

	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:       manifests,
		ManifestGC:      manifests,
		Objects:         objects,
		ReplicationGate: gate,
		GraceWindow:     10 * time.Millisecond,
		Clock:           clk,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	clk.Advance(20 * time.Millisecond)

	// Active pack is PENDING -> must skip.
	res1, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo (pending active): %v", err)
	}
	if res1.CompactedReaped != 0 || res1.SkippedReplication != 1 {
		t.Fatalf("expected skip while active pack pending, got %+v", res1)
	}

	// Active pack becomes COMPLETED -> must reap.
	objects.setReplicationStatus(activePackKey, s3.ReplicationStatusCompleted)
	res2, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo (completed active): %v", err)
	}
	if res2.CompactedReaped != 1 || res2.SkippedReplication != 0 {
		t.Fatalf("expected reap once active pack completed, got %+v", res2)
	}
}

func TestEngineAndGCSweeper_TracksReplacementPackIDThroughCRRGate(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 20, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-e2e-crr", "repo-e2e-crr")
	leaseStore := newFakeLeaseStore()
	manifests := newFakeManifestStore(1)
	objects := newFakePackObjectStore(clk)

	objs1, commit1 := buildCommitTreeBlob(t, "a.txt", "alpha\n", "c1", nil)
	pack1, idx1, sha1 := buildTestGitPack(t, objs1...)
	objs2, _ := buildCommitTreeBlob(t, "b.txt", "beta\n", "c2", &commit1)
	pack2, idx2, sha2 := buildTestGitPack(t, objs2...)

	ctx := context.Background()
	_ = objects.PutPack(ctx, rk, sha1, pack1, idx1)
	_ = objects.PutPack(ctx, rk, sha2, pack2, idx2)
	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000001", PackID: sha1,
		SizeBytes: int64(len(pack1)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})
	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000002", PackID: sha2,
		SizeBytes: int64(len(pack2)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})

	leaseMgr, _ := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "w-crr", TTL: 5 * time.Second})
	tier0, _ := NewTier0Strategy(objects, pack.DefaultLimits())
	engine, err := NewEngine(EngineConfig{
		Leases:        leaseMgr,
		Manifests:     manifests,
		ManifestGC:    manifests,
		GCGraceWindow: 10 * time.Millisecond,
		Clock:         clk,
		Strategies:    map[Tier]CompactionStrategy{Tier0: tier0},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	jobRes, err := engine.CompactShard(ctx, ShardTarget{
		Repo: rk, Shard: 0, ManifestShards: 1, OriginRegion: "us-east-1", Tier: Tier0,
	})
	if err != nil {
		t.Fatalf("CompactShard: %v", err)
	}

	// Verify both superseded packs recorded replaced_by_pack_id == jobRes.OutputPackID.
	all, _ := manifests.ListManifestsSince(ctx, rk, 1, "", false)
	for _, m := range all {
		if m.Status == ddb.PackStatusCompacted {
			replID, ok, err := manifests.GetReplacementPackID(ctx, rk, 1, m)
			if err != nil || !ok || replID != jobRes.OutputPackID {
				t.Fatalf("GetReplacementPackID(%s) = (%q, %v, %v), want %q", m.PackID, replID, ok, err, jobRes.OutputPackID)
			}
		}
	}

	gate, err := NewS3ReplicationGate(S3ReplicationGateConfig{
		Objects: objects,
		Breaker: NewCRRCircuitBreaker(CRRCircuitBreakerConfig{Threshold: 5, Cooldown: time.Minute, Clock: clk}),
	})
	if err != nil {
		t.Fatalf("NewS3ReplicationGate: %v", err)
	}
	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:       manifests,
		ManifestGC:      manifests,
		Objects:         objects,
		ReplicationGate: gate,
		GraceWindow:     10 * time.Millisecond,
		Clock:           clk,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	// Advance past grace window while replacement pack is PENDING -> must not delete.
	replPackKey, _ := s3.PackKey(rk, jobRes.OutputPackID)
	replIdxKey, _ := s3.IndexKey(rk, jobRes.OutputPackID)
	objects.setReplicationStatus(replPackKey, s3.ReplicationStatusPending)
	objects.setReplicationStatus(replIdxKey, s3.ReplicationStatusPending)

	clk.Advance(20 * time.Millisecond)
	gc1, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo(pending): %v", err)
	}
	if gc1.CompactedReaped != 0 || gc1.SkippedReplication != 2 {
		t.Fatalf("expected 2 skipped while replacement pending, got %+v", gc1)
	}

	// Mark replacement pack + idx COMPLETED -> both superseded packs reaped in 1 pair of HeadObject calls.
	objects.setReplicationStatus(replPackKey, s3.ReplicationStatusCompleted)
	objects.setReplicationStatus(replIdxKey, s3.ReplicationStatusCompleted)
	headBefore := objects.headCalls

	gc2, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo(completed): %v", err)
	}
	if gc2.CompactedReaped != 2 || gc2.SkippedReplication != 0 {
		t.Fatalf("expected 2 compacted packs reaped, got %+v", gc2)
	}
	if gotHeads := objects.headCalls - headBefore; gotHeads != 2 {
		t.Fatalf("expected per-sweep cache to limit HeadObject calls to 2 (.pack + .idx), got %d", gotHeads)
	}
}

func assertCounterMetricValue(t *testing.T, tel *telemetry.Provider, metricName, attrKey, attrVal string, want int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tel.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s data type %T, want Sum[int64]", metricName, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, exists := dp.Attributes.Value(attribute.Key(attrKey)); exists && v.AsString() == attrVal {
					if dp.Value != want {
						t.Fatalf("metric %s{%s=%s} = %d, want %d", metricName, attrKey, attrVal, dp.Value, want)
					}
					return
				}
			}
		}
	}
	t.Fatalf("metric %s{%s=%s} not found", metricName, attrKey, attrVal)
}

func assertGaugeMetricValue(t *testing.T, tel *telemetry.Provider, metricName string, want int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tel.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s data type %T, want Gauge[int64]", metricName, m.Data)
			}
			if len(gauge.DataPoints) == 0 {
				t.Fatalf("metric %s has no datapoints", metricName)
			}
			got := gauge.DataPoints[len(gauge.DataPoints)-1].Value
			if got != want {
				t.Fatalf("metric %s = %d, want %d", metricName, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s not found", metricName)
}
