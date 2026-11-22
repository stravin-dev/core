package edge

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"sync"

	"stravin.dev/core/internal/tenant"
)

// ErrNoPeers is returned when no healthy stravin-node peers are configured.
var ErrNoPeers = errors.New("edge: no healthy node peers available")

// NodeSelector is the GoF Strategy interface for selecting a target stravin-node peer.
type NodeSelector interface {
	SelectNode(repo tenant.RepoKey, refShard ...string) (string, error)
}

// RendezvousRouter implements deterministic Highest Random Weight (HRW) rendezvous hashing
// keyed by (tenant_id, repo_id[, ref_shard]) over a set of stravin-node peers.
type RendezvousRouter struct {
	mu    sync.RWMutex
	peers []string
}

// NewRendezvousRouter constructs a RendezvousRouter with the given peer list.
func NewRendezvousRouter(peers []string) *RendezvousRouter {
	r := &RendezvousRouter{}
	r.SetPeers(peers)
	return r
}

// ParsePeers splits a comma-separated peer list (e.g. STRAVIN_NODE_PEERS) into trimmed, unique peers.
func ParsePeers(raw string) []string {
	parts := strings.Split(raw, ",")
	return normalizePeers(parts)
}

// SetPeers replaces the active healthy peer set safely at runtime.
func (r *RendezvousRouter) SetPeers(peers []string) {
	norm := normalizePeers(peers)
	r.mu.Lock()
	r.peers = norm
	r.mu.Unlock()
}

// Peers returns a snapshot of the configured peers.
func (r *RendezvousRouter) Peers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.peers))
	copy(out, r.peers)
	return out
}

// SelectNode deterministically selects the highest-weight peer for (tenant_id, repo_id)
// and optional refShard.
func (r *RendezvousRouter) SelectNode(repo tenant.RepoKey, refShard ...string) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}

	r.mu.RLock()
	peers := r.peers
	r.mu.RUnlock()

	if len(peers) == 0 {
		return "", ErrNoPeers
	}

	shard := ""
	if len(refShard) > 0 {
		shard = refShard[0]
	}

	var bestPeer string
	var bestWeight uint64
	for i, peer := range peers {
		w := hrwWeight(peer, repo, shard)
		if i == 0 || w > bestWeight || (w == bestWeight && peer < bestPeer) {
			bestPeer = peer
			bestWeight = w
		}
	}
	return bestPeer, nil
}

func hrwWeight(peer string, repo tenant.RepoKey, refShard string) uint64 {
	h := sha256.New()
	_, _ = h.Write([]byte(peer))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(repo.TenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(repo.RepoID))
	if refShard != "" {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(refShard))
	}
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func normalizePeers(peers []string) []string {
	seen := make(map[string]struct{}, len(peers))
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
