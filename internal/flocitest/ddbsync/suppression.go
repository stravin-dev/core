package ddbsync

import (
	"sync"
	"time"
)

const (
	// DefaultDeleteSuppressionTTL bounds how long replicated REMOVE keys are suppressed.
	DefaultDeleteSuppressionTTL = 5 * time.Second
	defaultMaxSuppressionKeys   = 10000
)

// DeleteSuppressor tracks recently replicated REMOVE keys in memory within a bounded TTL
// so replicated deletes do not bounce back between regions.
type DeleteSuppressor struct {
	ttl     time.Duration
	maxKeys int
	now     func() time.Time

	mu      sync.Mutex
	entries map[string]time.Time
}

// NewDeleteSuppressor creates a bounded-TTL delete suppression map.
func NewDeleteSuppressor(ttl time.Duration, now func() time.Time) *DeleteSuppressor {
	if ttl <= 0 {
		ttl = DefaultDeleteSuppressionTTL
	}
	if now == nil {
		now = time.Now
	}
	return &DeleteSuppressor{
		ttl:     ttl,
		maxKeys: defaultMaxSuppressionKeys,
		now:     now,
		entries: make(map[string]time.Time),
	}
}

// Record marks key as recently deleted until now + ttl.
func (s *DeleteSuppressor) Record(key string) {
	if key == "" {
		return
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.entries) >= s.maxKeys {
		s.pruneExpiredLocked(now)
		if len(s.entries) >= s.maxKeys {
			clear(s.entries)
		}
	}
	s.entries[key] = now.Add(s.ttl)
}

// IsSuppressed reports whether key is currently inside the delete suppression window.
func (s *DeleteSuppressor) IsSuppressed(key string) bool {
	if key == "" {
		return false
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	exp, ok := s.entries[key]
	if !ok {
		return false
	}
	if !now.Before(exp) {
		delete(s.entries, key)
		return false
	}
	return true
}

func (s *DeleteSuppressor) pruneExpiredLocked(now time.Time) {
	for k, exp := range s.entries {
		if !now.Before(exp) {
			delete(s.entries, k)
		}
	}
}
