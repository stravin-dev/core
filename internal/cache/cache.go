package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	bareHeadContent   = "ref: refs/heads/main\n"
	bareConfigContent = "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n"
	dirPerm           = 0o750
	filePerm          = 0o600
	maxPackIDLength   = 128
)

// Config configures the warm NVMe bare-repo cache.
type Config struct {
	Root              string
	MaxBytes          int64
	MaxTenantFraction float64
	EvictionPolicy    EvictionPolicy
	PackLimits        pack.Limits
	Telemetry         *telemetry.Provider
}

// RepoStats reports point-in-time pack, pending-prune, byte, and pin accounting
// for a single cached repository.
type RepoStats struct {
	ActivePacks  int
	PendingPrune int
	SizeBytes    int64
	Pins         int
}

// Stats reports point-in-time byte and repository accounting across tenants.
type Stats struct {
	TotalBytes        int64
	MaxBytes          int64
	RepoCount         int
	ActivePacks       int
	PendingPrunePacks int
	TenantBytes       map[tenant.TenantID]int64
	RepoStats         map[tenant.RepoKey]RepoStats
}

// repoEntry is the shared internal Flyweight state for a single materialized repository.
type repoEntry struct {
	key          tenant.RepoKey
	path         string
	packs        map[string]int64     // packID -> combined .pack + .idx size in bytes
	idxCache     map[string]*pack.Idx // packID -> parsed .idx for O(log N) lookups
	pendingPrune map[string]uint64    // superseded packID -> cutoff pruneGen (unlink when no pins <= cutoff)
	watermarks   map[int]string       // manifest shard -> last_seen_ulid
	sizeBytes    int64
	lastAccess   uint64
	pins         int
	pruneGen     uint64         // monotonically incremented when new packs are scheduled for pruning
	pinsByGen    map[uint64]int // generation -> count of active pins acquired at that generation
}

// RepoHandle is a GoF Proxy representing an active reader lease on a materialized
// bare repository. While held, eviction and superseded-pack pruning will not
// delete or unlink files belonging to this repository.
type RepoHandle struct {
	cache   *Cache
	repo    tenant.RepoKey
	path    string
	once    sync.Once
	onClose func()
}

// Path returns the absolute on-disk path of the materialized bare repository.
func (h *RepoHandle) Path() string {
	return h.path
}

// Repo returns the validated RepoKey associated with this handle.
func (h *RepoHandle) Repo() tenant.RepoKey {
	return h.repo
}

// BaseResolver returns a pack.BaseResolver scoped to this repository.
func (h *RepoHandle) BaseResolver() pack.BaseResolver {
	return h.cache.BaseResolver(h.repo)
}

// Release decrements the repository's active reader pin count. It is safe to call multiple times.
func (h *RepoHandle) Release() {
	h.once.Do(func() {
		if h.onClose != nil {
			h.onClose()
		}
	})
}

// Cache manages disposable bare Git repositories on local NVMe storage.
type Cache struct {
	root       string
	maxBytes   int64
	policy     EvictionPolicy
	packLimits pack.Limits
	flights    *repoFlightGroup
	metrics    *cacheMetrics

	mu          sync.Mutex
	accessSeq   uint64
	totalBytes  int64
	tenantBytes map[tenant.TenantID]int64
	repos       map[tenant.RepoKey]*repoEntry
}

// New initializes a Cache rooted at cfg.Root and recovers any existing warm
// repositories, packs, and persisted manifest watermarks found on disk.
func New(cfg Config) (*Cache, error) {
	root := strings.TrimSpace(cfg.Root)
	if root == "" {
		return nil, ErrEmptyRoot
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve cache root: %w", err)
	}

	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes < 0 {
		return nil, fmt.Errorf("%w: negative max bytes %d", ErrInvalidConfig, maxBytes)
	}

	fraction := cfg.MaxTenantFraction
	if fraction == 0 {
		fraction = DefaultMaxTenantFraction
	}
	if fraction < 0 || fraction > 1 {
		return nil, fmt.Errorf("%w: max tenant fraction %v must be in (0, 1]", ErrInvalidConfig, fraction)
	}

	policy := cfg.EvictionPolicy
	if policy == nil {
		policy = NewPartitionedLRUPolicy(fraction)
	}

	if err := os.MkdirAll(filepath.Join(absRoot, "t"), dirPerm); err != nil {
		return nil, fmt.Errorf("create cache root: %w", err)
	}

	c := &Cache{
		root:        absRoot,
		maxBytes:    maxBytes,
		policy:      policy,
		packLimits:  cfg.PackLimits,
		flights:     newRepoFlightGroup(),
		metrics:     newCacheMetrics(cfg.Telemetry),
		tenantBytes: make(map[tenant.TenantID]int64),
		repos:       make(map[tenant.RepoKey]*repoEntry),
	}

	if err := c.recoverExisting(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.recordPackMetricsLocked()
	c.mu.Unlock()
	return c, nil
}

// RepoPath returns the canonical on-disk bare repository path <root>/t/{tenant_id}/{repo_id}.
func (c *Cache) RepoPath(repo tenant.RepoKey) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	return filepath.Join(c.root, "t", string(repo.TenantID), string(repo.RepoID)), nil
}

// Materialize ensures the bare repository directory exists and coalesces concurrent
// sync/download callbacks for the same RepoKey via singleflight locking.
// On success, it returns a pinned RepoHandle; callers must invoke handle.Release().
func (c *Cache) Materialize(ctx context.Context, repo tenant.RepoKey, syncFn func(ctx context.Context, repoPath string) error) (*RepoHandle, error) {
	repoPath, err := c.RepoPath(repo)
	if err != nil {
		return nil, err
	}

	// Ensure entry exists and pin it during materialization so concurrent Evict cannot delete it.
	if err := initBareRepoLayout(repoPath); err != nil {
		return nil, err
	}
	c.mu.Lock()
	entry := c.getOrCreateEntryLocked(repo, repoPath)
	pinGen := c.acquirePinLocked(entry)
	c.touchLocked(entry)
	c.mu.Unlock()

	err = c.flights.Do(ctx, repo, func(flightCtx context.Context) error {
		if syncFn == nil {
			return nil
		}
		return syncFn(flightCtx, repoPath)
	})
	if err != nil {
		c.releasePin(repo, pinGen)
		return nil, err
	}

	return c.newHandle(repo, repoPath, pinGen), nil
}

// Pin acquires a reader lease on an existing materialized repository, protecting it
// from eviction and deferred pack unlinking until Release() is called.
func (c *Cache) Pin(repo tenant.RepoKey) (*RepoHandle, error) {
	if err := repo.Validate(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	entry, ok := c.repos[repo]
	if !ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("%s: %w", repo, ErrRepoNotFound)
	}
	pinGen := c.acquirePinLocked(entry)
	c.touchLocked(entry)
	repoPath := entry.path
	c.mu.Unlock()

	return c.newHandle(repo, repoPath, pinGen), nil
}

// InstallPack atomically installs pack-{packID}.pack and pack-{packID}.idx into
// <root>/t/{tenant_id}/{repo_id}/objects/pack/ using temp-file write + rename,
// updates tenant/global byte accounting, and enforces quotas against other unpinned repos.
func (c *Cache) InstallPack(repo tenant.RepoKey, packID string, packBytes, idxBytes []byte) error {
	repoPath, err := c.RepoPath(repo)
	if err != nil {
		return err
	}
	if err := validatePackID(packID); err != nil {
		return err
	}
	if err := initBareRepoLayout(repoPath); err != nil {
		return err
	}

	packDir := filepath.Join(repoPath, "objects", "pack")
	base := packFileBase(packID)
	if err := writeFileAtomic(packDir, base+".pack", packBytes); err != nil {
		return fmt.Errorf("install pack %s: %w", packID, err)
	}
	if err := writeFileAtomic(packDir, base+".idx", idxBytes); err != nil {
		return fmt.Errorf("install idx %s: %w", packID, err)
	}

	newSize := int64(len(packBytes)) + int64(len(idxBytes))

	c.mu.Lock()
	entry := c.getOrCreateEntryLocked(repo, repoPath)
	oldSize := entry.packs[packID]
	delta := newSize - oldSize
	entry.packs[packID] = newSize
	delete(entry.idxCache, packID)
	delete(entry.pendingPrune, packID)
	entry.sizeBytes += delta
	c.tenantBytes[repo.TenantID] += delta
	c.totalBytes += delta
	c.touchLocked(entry)
	c.evictLocked(repo)
	c.recordPackMetricsLocked()
	c.mu.Unlock()

	return nil
}

// PrunePacks removes superseded packs (e.g. after compaction refresh).
// Using epoch/generation tracking, packs are unlinked immediately if no pins from
// prior generations (<= cutoff) are active; otherwise deletion is deferred until
// those prior-generation pins release, without waiting for newer pins to drain to 0.
func (c *Cache) PrunePacks(repo tenant.RepoKey, packIDs []string) error {
	if err := repo.Validate(); err != nil {
		return err
	}
	for _, id := range packIDs {
		if err := validatePackID(id); err != nil {
			return err
		}
	}

	c.mu.Lock()
	entry, ok := c.repos[repo]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("%s: %w", repo, ErrRepoNotFound)
	}

	seen := make(map[string]struct{}, len(packIDs))
	newlyScheduled := make([]string, 0, len(packIDs))
	for _, id := range packIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if _, exists := entry.packs[id]; !exists {
			continue
		}
		// Keep the earliest cutoff generation for already-pending packs so
		// repeated PrunePacks calls cannot push a pack's cutoff forward.
		if _, alreadyPending := entry.pendingPrune[id]; alreadyPending {
			continue
		}
		newlyScheduled = append(newlyScheduled, id)
	}
	if len(newlyScheduled) == 0 {
		c.mu.Unlock()
		return nil
	}

	cutoff := entry.pruneGen
	entry.pruneGen++

	if !hasPinsAtOrBefore(entry, cutoff) {
		c.unlinkPacksLocked(entry, newlyScheduled)
		c.recordPackMetricsLocked()
		c.mu.Unlock()
		return nil
	}

	for _, id := range newlyScheduled {
		entry.pendingPrune[id] = cutoff
	}
	// Invalidate any existing multi-pack-index immediately so newer-generation readers
	// do not open a MIDX referencing packs that will be unlinked once prior pins drain.
	invalidatePackAuxIndexes(filepath.Join(entry.path, "objects", "pack"))
	c.recordPackMetricsLocked()
	c.mu.Unlock()
	return nil
}

// PendingPruneCount returns the number of superseded packs awaiting prior-generation
// pin release for repo.
func (c *Cache) PendingPruneCount(repo tenant.RepoKey) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return 0
	}
	return len(entry.pendingPrune)
}

// Evict runs the configured EvictionPolicy across all unpinned repositories and
// returns the list of repositories that were evicted.
func (c *Cache) Evict() []tenant.RepoKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictLocked(tenant.RepoKey{})
}

// HasPack reports whether packID is currently cached for repo (and not pending deletion).
func (c *Cache) HasPack(repo tenant.RepoKey, packID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return false
	}
	if _, pending := entry.pendingPrune[packID]; pending {
		return false
	}
	_, exists := entry.packs[packID]
	return exists
}

// CachedPackIDs returns a sorted snapshot of active cached pack IDs for repo.
func (c *Cache) CachedPackIDs(repo tenant.RepoKey) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(entry.packs))
	for id := range entry.packs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Watermark returns the last_seen_ulid for manifest shard MS#{shard} of repo, or "".
func (c *Cache) Watermark(repo tenant.RepoKey, shard int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return ""
	}
	return entry.watermarks[shard]
}

// Watermarks returns a copy of all manifest shard watermarks tracked for repo.
func (c *Cache) Watermarks(repo tenant.RepoKey) map[int]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return map[int]string{}
	}
	out := make(map[int]string, len(entry.watermarks))
	for k, v := range entry.watermarks {
		out[k] = v
	}
	return out
}

// AdvanceWatermark monotonically advances the last_seen_ulid for MS#{shard} on repo
// and persists the watermark metadata atomically to disk. Returns true if updated.
func (c *Cache) AdvanceWatermark(repo tenant.RepoKey, shard int, ulid string) bool {
	if shard < 0 || ulid == "" {
		return false
	}
	repoPath, err := c.RepoPath(repo)
	if err != nil {
		return false
	}

	c.mu.Lock()
	entry, ok := c.repos[repo]
	if !ok {
		if err := initBareRepoLayout(repoPath); err != nil {
			c.mu.Unlock()
			return false
		}
		entry = c.getOrCreateEntryLocked(repo, repoPath)
	}
	if cur := entry.watermarks[shard]; cur != "" && ulid <= cur {
		c.mu.Unlock()
		return false
	}
	entry.watermarks[shard] = ulid
	snapshot := make(map[int]string, len(entry.watermarks))
	for k, v := range entry.watermarks {
		snapshot[k] = v
	}
	path := entry.path
	c.mu.Unlock()

	_ = saveWatermarks(path, snapshot)
	return true
}

// BaseResolver returns a pack.BaseResolver backed by repo's warm NVMe packs.
func (c *Cache) BaseResolver(repo tenant.RepoKey) *RepoBaseResolver {
	return &RepoBaseResolver{
		cache:  c,
		repo:   repo,
		limits: c.packLimits,
	}
}

// Stats returns a snapshot of global, per-tenant, and per-repository cache accounting.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	tb := make(map[tenant.TenantID]int64, len(c.tenantBytes))
	for k, v := range c.tenantBytes {
		tb[k] = v
	}
	rs := make(map[tenant.RepoKey]RepoStats, len(c.repos))
	var totalActive, totalPending int
	for k, entry := range c.repos {
		active := len(entry.packs)
		pending := len(entry.pendingPrune)
		totalActive += active
		totalPending += pending
		rs[k] = RepoStats{
			ActivePacks:  active,
			PendingPrune: pending,
			SizeBytes:    entry.sizeBytes,
			Pins:         entry.pins,
		}
	}
	return Stats{
		TotalBytes:        c.totalBytes,
		MaxBytes:          c.maxBytes,
		RepoCount:         len(c.repos),
		ActivePacks:       totalActive,
		PendingPrunePacks: totalPending,
		TenantBytes:       tb,
		RepoStats:         rs,
	}
}

func (c *Cache) newHandle(repo tenant.RepoKey, repoPath string, pinGen uint64) *RepoHandle {
	return &RepoHandle{
		cache: c,
		repo:  repo,
		path:  repoPath,
		onClose: func() {
			c.releasePin(repo, pinGen)
		},
	}
}

func (c *Cache) acquirePinLocked(entry *repoEntry) uint64 {
	gen := entry.pruneGen
	entry.pins++
	entry.pinsByGen[gen]++
	return gen
}

func (c *Cache) releasePin(repo tenant.RepoKey, pinGen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok || entry.pins == 0 {
		return
	}
	entry.pins--
	decrementPinGenLocked(entry, pinGen)
	if c.drainEligiblePrunesLocked(entry) {
		c.recordPackMetricsLocked()
	}
}

func decrementPinGenLocked(entry *repoEntry, gen uint64) {
	if cnt := entry.pinsByGen[gen]; cnt <= 1 {
		delete(entry.pinsByGen, gen)
	} else {
		entry.pinsByGen[gen] = cnt - 1
	}
}

func hasPinsAtOrBefore(entry *repoEntry, cutoff uint64) bool {
	for gen, cnt := range entry.pinsByGen {
		if cnt > 0 && gen <= cutoff {
			return true
		}
	}
	return false
}

func minActivePinGen(entry *repoEntry) (uint64, bool) {
	var minGen uint64
	found := false
	for gen, cnt := range entry.pinsByGen {
		if cnt <= 0 {
			continue
		}
		if !found || gen < minGen {
			minGen = gen
			found = true
		}
	}
	return minGen, found
}

func (c *Cache) drainEligiblePrunesLocked(entry *repoEntry) bool {
	if len(entry.pendingPrune) == 0 {
		return false
	}
	minGen, hasActive := minActivePinGen(entry)
	var ready []string
	for id, cutoff := range entry.pendingPrune {
		if !hasActive || cutoff < minGen {
			ready = append(ready, id)
		}
	}
	if len(ready) == 0 {
		return false
	}
	c.unlinkPacksLocked(entry, ready)
	return true
}

func (c *Cache) recordPackMetricsLocked() {
	if c.metrics == nil {
		return
	}
	var totalActive, totalPending, maxActive, maxPending int64
	for _, entry := range c.repos {
		a := int64(len(entry.packs))
		p := int64(len(entry.pendingPrune))
		totalActive += a
		totalPending += p
		if a > maxActive {
			maxActive = a
		}
		if p > maxPending {
			maxPending = p
		}
	}
	c.metrics.recordPackCounts(totalActive, totalPending, maxActive, maxPending)
}

func (c *Cache) getOrCreateEntryLocked(repo tenant.RepoKey, repoPath string) *repoEntry {
	if entry, ok := c.repos[repo]; ok {
		return entry
	}
	entry := &repoEntry{
		key:          repo,
		path:         repoPath,
		packs:        make(map[string]int64),
		idxCache:     make(map[string]*pack.Idx),
		pendingPrune: make(map[string]uint64),
		watermarks:   make(map[int]string),
		pinsByGen:    make(map[uint64]int),
	}
	c.repos[repo] = entry
	return entry
}

func (c *Cache) touchLocked(entry *repoEntry) {
	c.accessSeq++
	entry.lastAccess = c.accessSeq
}

func (c *Cache) evictLocked(exclude tenant.RepoKey) []tenant.RepoKey {
	candidates := make([]EvictionCandidate, 0, len(c.repos))
	for key, entry := range c.repos {
		pinned := entry.pins > 0 || key == exclude
		candidates = append(candidates, EvictionCandidate{
			Repo:       key,
			SizeBytes:  entry.sizeBytes,
			LastAccess: entry.lastAccess,
			Pinned:     pinned,
		})
	}

	tb := make(map[tenant.TenantID]int64, len(c.tenantBytes))
	for k, v := range c.tenantBytes {
		tb[k] = v
	}

	victims := c.policy.SelectEvictions(EvictionState{
		TotalBytes:  c.totalBytes,
		MaxBytes:    c.maxBytes,
		TenantBytes: tb,
		Candidates:  candidates,
	})

	evicted := make([]tenant.RepoKey, 0, len(victims))
	for _, key := range victims {
		entry, ok := c.repos[key]
		if !ok || entry.pins > 0 || key == exclude {
			continue
		}
		if err := os.RemoveAll(entry.path); err != nil {
			continue
		}
		c.totalBytes -= entry.sizeBytes
		c.tenantBytes[key.TenantID] -= entry.sizeBytes
		if c.tenantBytes[key.TenantID] <= 0 {
			delete(c.tenantBytes, key.TenantID)
		}
		delete(c.repos, key)
		evicted = append(evicted, key)
	}
	if len(evicted) > 0 {
		c.recordPackMetricsLocked()
	}
	return evicted
}

func (c *Cache) unlinkPacksLocked(entry *repoEntry, packIDs []string) {
	packDir := filepath.Join(entry.path, "objects", "pack")
	var removedCount int64
	for _, id := range packIDs {
		sz, exists := entry.packs[id]
		if !exists {
			delete(entry.pendingPrune, id)
			continue
		}
		base := packFileBase(id)
		_ = os.Remove(filepath.Join(packDir, base+".pack"))
		_ = os.Remove(filepath.Join(packDir, base+".idx"))
		delete(entry.packs, id)
		delete(entry.idxCache, id)
		delete(entry.pendingPrune, id)
		entry.sizeBytes -= sz
		c.tenantBytes[entry.key.TenantID] -= sz
		c.totalBytes -= sz
		removedCount++
	}
	if removedCount > 0 {
		invalidatePackAuxIndexes(packDir)
		c.metrics.recordPacksPruned(removedCount)
	}
	if c.tenantBytes[entry.key.TenantID] <= 0 {
		delete(c.tenantBytes, entry.key.TenantID)
	}
}

// invalidatePackAuxIndexes removes multi-pack-index and associated bitmap/rev
// files whenever any pack is unlinked so git upload-pack never reads a stale
// MIDX referencing deleted packfiles.
func invalidatePackAuxIndexes(packDir string) {
	_ = os.Remove(filepath.Join(packDir, "multi-pack-index"))
	for _, pattern := range []string{"multi-pack-index*.bitmap", "multi-pack-index*.rev"} {
		matches, err := filepath.Glob(filepath.Join(packDir, pattern))
		if err != nil {
			continue
		}
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
}

func (c *Cache) getCachedIdx(repo tenant.RepoKey, packID string) (*pack.Idx, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return nil, false
	}
	idx, ok := entry.idxCache[packID]
	return idx, ok
}

func (c *Cache) putCachedIdx(repo tenant.RepoKey, packID string, idx *pack.Idx) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.repos[repo]
	if !ok {
		return
	}
	if _, exists := entry.packs[packID]; exists {
		entry.idxCache[packID] = idx
	}
}

func (c *Cache) recoverExisting() error {
	tenantsDir := filepath.Join(c.root, "t")
	tenantDirs, err := os.ReadDir(tenantsDir)
	if err != nil {
		return fmt.Errorf("scan cache tenants: %w", err)
	}
	for _, td := range tenantDirs {
		if !td.IsDir() {
			continue
		}
		tid, err := tenant.ParseTenantID(td.Name())
		if err != nil {
			continue
		}
		repoDirs, err := os.ReadDir(filepath.Join(tenantsDir, td.Name()))
		if err != nil {
			continue
		}
		for _, rd := range repoDirs {
			if !rd.IsDir() {
				continue
			}
			rid, err := tenant.ParseRepoID(rd.Name())
			if err != nil {
				continue
			}
			key := tenant.RepoKey{TenantID: tid, RepoID: rid}
			repoPath := filepath.Join(tenantsDir, td.Name(), rd.Name())
			c.recoverRepo(key, repoPath)
		}
	}
	return nil
}

func (c *Cache) recoverRepo(key tenant.RepoKey, repoPath string) {
	entry := c.getOrCreateEntryLocked(key, repoPath)
	c.touchLocked(entry)

	if wms, err := loadWatermarks(repoPath); err == nil {
		entry.watermarks = wms
	}

	packDir := filepath.Join(repoPath, "objects", "pack")
	files, err := os.ReadDir(packDir)
	if err != nil {
		return
	}

	sizes := make(map[string]int64)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if !strings.HasPrefix(name, "pack-") {
			continue
		}
		var packID string
		switch {
		case strings.HasSuffix(name, ".pack"):
			packID = strings.TrimSuffix(strings.TrimPrefix(name, "pack-"), ".pack")
		case strings.HasSuffix(name, ".idx"):
			packID = strings.TrimSuffix(strings.TrimPrefix(name, "pack-"), ".idx")
		default:
			continue
		}
		if validatePackID(packID) != nil {
			continue
		}
		info, err := f.Info()
		if err != nil {
			continue
		}
		sizes[packID] += info.Size()
	}

	for packID, sz := range sizes {
		entry.packs[packID] = sz
		entry.sizeBytes += sz
		c.tenantBytes[key.TenantID] += sz
		c.totalBytes += sz
	}
}

func initBareRepoLayout(repoPath string) error {
	// If the bare repository is already initialized with Git's reftable backend,
	// refs/heads is a regular stub file (not a directory); only ensure objects/pack.
	if _, err := os.Stat(filepath.Join(repoPath, "reftable", "tables.list")); err == nil {
		if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), dirPerm); err != nil {
			return fmt.Errorf("create bare repo dir objects/pack: %w", err)
		}
		return nil
	}
	for _, rel := range []string{"objects/pack", "refs/heads", "refs/tags"} {
		if err := os.MkdirAll(filepath.Join(repoPath, rel), dirPerm); err != nil {
			return fmt.Errorf("create bare repo dir %s: %w", rel, err)
		}
	}
	if err := ensureFile(filepath.Join(repoPath, "HEAD"), []byte(bareHeadContent)); err != nil {
		return err
	}
	return ensureFile(filepath.Join(repoPath, "config"), []byte(bareConfigContent))
}

func ensureFile(path string, content []byte) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return writeFileAtomic(filepath.Dir(path), filepath.Base(path), content)
}

func writeFileAtomic(dir, filename string, data []byte) error {
	tmp, err := os.CreateTemp(dir, ".tmp-"+filename+"-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(filePerm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, filename)); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func packFileBase(packID string) string {
	return "pack-" + packID
}

func validatePackID(packID string) error {
	if packID == "" || len(packID) > maxPackIDLength || strings.Contains(packID, "..") {
		return fmt.Errorf("%w: %q", ErrInvalidPackID, packID)
	}
	for i := 0; i < len(packID); i++ {
		c := packID[i]
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !isAlphaNum && c != '-' && c != '_' {
			return fmt.Errorf("%w: %q", ErrInvalidPackID, packID)
		}
	}
	return nil
}
