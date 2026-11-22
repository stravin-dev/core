package edge

import (
	"errors"
	"fmt"
	"testing"

	"stravin.dev/core/internal/tenant"
)

func TestRendezvousRouter_DeterminismAndNoPeers(t *testing.T) {
	empty := NewRendezvousRouter(nil)
	if _, err := empty.SelectNode(tenant.RepoKey{TenantID: "acme", RepoID: "api"}); !errors.Is(err, ErrNoPeers) {
		t.Fatalf("expected ErrNoPeers, got %v", err)
	}

	r1 := NewRendezvousRouter([]string{"node-1:9090", "node-2:9090", "node-3:9090"})
	r2 := NewRendezvousRouter([]string{"node-3:9090", "node-1:9090", "node-2:9090", "node-1:9090"})

	key := tenant.RepoKey{TenantID: "acme", RepoID: "monorepo"}
	p1, err := r1.SelectNode(key)
	if err != nil {
		t.Fatalf("SelectNode r1: %v", err)
	}
	p2, err := r2.SelectNode(key)
	if err != nil {
		t.Fatalf("SelectNode r2: %v", err)
	}
	if p1 != p2 {
		t.Fatalf("HRW must be independent of peer ordering: got %q vs %q", p1, p2)
	}

	// Ref shard changes routing domain deterministically.
	ps1, err := r1.SelectNode(key, "RS#0")
	if err != nil {
		t.Fatalf("SelectNode with shard: %v", err)
	}
	ps2, err := r2.SelectNode(key, "RS#0")
	if err != nil {
		t.Fatalf("SelectNode with shard r2: %v", err)
	}
	if ps1 != ps2 {
		t.Fatalf("sharded HRW mismatch: %q vs %q", ps1, ps2)
	}
}

func TestRendezvousRouter_DistributionAndMinimalDisruption(t *testing.T) {
	peers3 := []string{"node-1:9090", "node-2:9090", "node-3:9090"}
	peers4 := []string{"node-1:9090", "node-2:9090", "node-3:9090", "node-4:9090"}
	r3 := NewRendezvousRouter(peers3)
	r4 := NewRendezvousRouter(peers4)

	const total = 1200
	counts := make(map[string]int, len(peers3))
	moved := 0

	for i := 0; i < total; i++ {
		key := tenant.RepoKey{
			TenantID: tenant.TenantID(fmt.Sprintf("tenant-%d", i%20)),
			RepoID:   tenant.RepoID(fmt.Sprintf("repo-%d", i)),
		}
		p3, err := r3.SelectNode(key)
		if err != nil {
			t.Fatalf("SelectNode r3: %v", err)
		}
		p4, err := r4.SelectNode(key)
		if err != nil {
			t.Fatalf("SelectNode r4: %v", err)
		}
		counts[p3]++
		if p3 != p4 {
			moved++
		}
	}

	// Uniform distribution: each of 3 peers should get roughly 400 +/- 120 keys.
	for _, peer := range peers3 {
		if counts[peer] < 280 || counts[peer] > 520 {
			t.Fatalf("skewed distribution for %s: got %d of %d", peer, counts[peer], total)
		}
	}

	// Minimal disruption: adding 4th node should move roughly 1/4 (~300) keys, well under 40%.
	if moved < 180 || moved > 450 {
		t.Fatalf("unexpected HRW churn when adding 4th peer: moved %d of %d", moved, total)
	}
}
