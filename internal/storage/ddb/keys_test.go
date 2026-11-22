package ddb

import (
	"errors"
	"strings"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

func mustRepoKey(t *testing.T, tid, rid string) tenant.RepoKey {
	t.Helper()
	k, err := tenant.NewRepoKey(tid, rid)
	if err != nil {
		t.Fatalf("NewRepoKey(%q, %q): %v", tid, rid, err)
	}
	return k
}

func TestKeyBuilders(t *testing.T) {
	repo := mustRepoKey(t, "acme", "monorepo")

	t.Run("META", func(t *testing.T) {
		k, err := MetaKey(repo)
		if err != nil {
			t.Fatalf("MetaKey: %v", err)
		}
		if k.PK != "T#acme#REPO#monorepo" || k.SK != "META" {
			t.Fatalf("got %+v, want PK=T#acme#REPO#monorepo SK=META", k)
		}
	})

	t.Run("RS and RH deterministic shard", func(t *testing.T) {
		s1, err := RefShard("refs/heads/main", 16)
		if err != nil {
			t.Fatalf("RefShard: %v", err)
		}
		s2, err := RefShard("refs/heads/main", 16)
		if err != nil {
			t.Fatalf("RefShard second call: %v", err)
		}
		if s1 != s2 || s1 < 0 || s1 >= 16 {
			t.Fatalf("non-deterministic or out-of-range shard: s1=%d s2=%d", s1, s2)
		}

		rs, err := RefStateKey(repo, "refs/heads/main", 16)
		if err != nil {
			t.Fatalf("RefStateKey: %v", err)
		}
		if !strings.HasPrefix(rs.PK, "T#acme#REPO#monorepo#RS#") || rs.SK != "REF#refs/heads/main" {
			t.Fatalf("unexpected RS key: %+v", rs)
		}

		rh, err := RefHistoryKey(repo, "refs/heads/main", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 16)
		if err != nil {
			t.Fatalf("RefHistoryKey: %v", err)
		}
		if !strings.HasPrefix(rh.PK, "T#acme#REPO#monorepo#RH#") || rh.SK != "REF#refs/heads/main#01ARZ3NDEKTSV4RRFFQ69G5FAV" {
			t.Fatalf("unexpected RH key: %+v", rh)
		}
		// RS and RH for the same ref must share the same shard index suffix.
		rsIdx := strings.TrimPrefix(rs.PK, "T#acme#REPO#monorepo#RS#")
		rhIdx := strings.TrimPrefix(rh.PK, "T#acme#REPO#monorepo#RH#")
		if rsIdx != rhIdx {
			t.Fatalf("RS shard %s != RH shard %s", rsIdx, rhIdx)
		}
	})

	t.Run("MS deterministic shard", func(t *testing.T) {
		m, err := ManifestShard("pack-abc", 8)
		if err != nil {
			t.Fatalf("ManifestShard: %v", err)
		}
		if m < 0 || m >= 8 {
			t.Fatalf("shard out of range: %d", m)
		}
		mk, err := ManifestKey(repo, "01ARZ3NDEKTSV4RRFFQ69G5FAV", "pack-abc", 8)
		if err != nil {
			t.Fatalf("ManifestKey: %v", err)
		}
		if !strings.HasPrefix(mk.PK, "T#acme#REPO#monorepo#MS#") || mk.SK != "PACK#01ARZ3NDEKTSV4RRFFQ69G5FAV#pack-abc" {
			t.Fatalf("unexpected MS key: %+v", mk)
		}
	})

	t.Run("LEASE keys", func(t *testing.T) {
		ck, err := CompactionLeaseKey(repo, 3)
		if err != nil {
			t.Fatalf("CompactionLeaseKey: %v", err)
		}
		if ck.PK != "T#acme#REPO#monorepo#LEASE" || ck.SK != "SHARD#3" {
			t.Fatalf("unexpected compaction lease key: %+v", ck)
		}

		rk, err := RegionalWriteLeaseKey(lease.Region("us-east-1"))
		if err != nil {
			t.Fatalf("RegionalWriteLeaseKey: %v", err)
		}
		if rk.PK != "REGION#us-east-1#LEASE" || rk.SK != "WRITE" {
			t.Fatalf("unexpected regional write lease key: %+v", rk)
		}
	})

	t.Run("HOOKS WEBHOOKS CHECKPOINT keys", func(t *testing.T) {
		hk, err := RepoHooksKey(repo)
		if err != nil || hk.PK != "T#acme#REPO#monorepo" || hk.SK != "HOOKS" {
			t.Fatalf("unexpected repo hooks key: %+v, err=%v", hk, err)
		}
		ohk, err := OrgHooksKey(tenant.TenantID("acme"), "eng")
		if err != nil || ohk.PK != "T#acme#ORG#eng" || ohk.SK != "HOOKS" {
			t.Fatalf("unexpected org hooks key: %+v, err=%v", ohk, err)
		}
		wk, err := WebhooksKey(repo)
		if err != nil || wk.PK != "T#acme#REPO#monorepo" || wk.SK != "WEBHOOKS" {
			t.Fatalf("unexpected webhooks key: %+v, err=%v", wk, err)
		}
		cpk, err := StreamCheckpointKey("shardId-000000000001")
		if err != nil || cpk.PK != "STREAMS#CHECKPOINT" || cpk.SK != "SHARD#shardId-000000000001" {
			t.Fatalf("unexpected checkpoint key: %+v, err=%v", cpk, err)
		}
	})
}

func TestShardValidationBounds(t *testing.T) {
	for _, bad := range []int{-1, 0, 65, 100} {
		if err := ValidateRefShards(bad); !errors.Is(err, ErrInvalidShardCount) {
			t.Fatalf("ValidateRefShards(%d) = %v, want ErrInvalidShardCount", bad, err)
		}
	}
	for _, good := range []int{1, 16, 64} {
		if err := ValidateRefShards(good); err != nil {
			t.Fatalf("ValidateRefShards(%d) unexpected err: %v", good, err)
		}
	}

	for _, bad := range []int{-1, 0, 33, 64} {
		if err := ValidateManifestShards(bad); !errors.Is(err, ErrInvalidShardCount) {
			t.Fatalf("ValidateManifestShards(%d) = %v, want ErrInvalidShardCount", bad, err)
		}
	}
	for _, good := range []int{1, 8, 32} {
		if err := ValidateManifestShards(good); err != nil {
			t.Fatalf("ValidateManifestShards(%d) unexpected err: %v", good, err)
		}
	}
}

func TestValidateOID(t *testing.T) {
	if err := ValidateOID(ZeroOID); err != nil {
		t.Fatalf("ZeroOID should be valid: %v", err)
	}
	if err := ValidateOID("0123456789abcdef0123456789abcdef01234567"); err != nil {
		t.Fatalf("valid sha1 hex failed: %v", err)
	}
	for _, bad := range []string{"", "1234", "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ", strings.Repeat("a", 41)} {
		if err := ValidateOID(bad); !errors.Is(err, ErrInvalidOID) {
			t.Fatalf("ValidateOID(%q) = %v, want ErrInvalidOID", bad, err)
		}
	}
}

func TestNewULIDLexicographicOrder(t *testing.T) {
	t1 := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC)
	t2 := t1.Add(2 * time.Millisecond)

	u1, err := NewULID(t1)
	if err != nil {
		t.Fatalf("NewULID(t1): %v", err)
	}
	u2, err := NewULID(t2)
	if err != nil {
		t.Fatalf("NewULID(t2): %v", err)
	}
	if len(u1) != 26 || len(u2) != 26 {
		t.Fatalf("unexpected ULID lengths: %d, %d", len(u1), len(u2))
	}
	if !(u1 < u2) {
		t.Fatalf("expected %q < %q", u1, u2)
	}
}
