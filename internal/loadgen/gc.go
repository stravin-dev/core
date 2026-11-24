package loadgen

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultGCPackThreshold matches Git's gc.autoPackLimit default (50 .idx files).
	DefaultGCPackThreshold = 50
	// DefaultGCLooseThreshold matches Git's gc.auto default (~6700 loose objects / 256 buckets ≈ 27 in objects/17).
	DefaultGCLooseThreshold = 27
)

// GCThresholds configures the zero-subprocess heuristic for on-demand git gc
// on an idle per-worker base clone.
type GCThresholds struct {
	MaxPacks        int
	MaxLooseObjects int
}

// DefaultGCThresholds returns the default pack and loose-object GC thresholds.
func DefaultGCThresholds() GCThresholds {
	return GCThresholds{
		MaxPacks:        DefaultGCPackThreshold,
		MaxLooseObjects: DefaultGCLooseThreshold,
	}
}

func (t GCThresholds) normalize() GCThresholds {
	if t.MaxPacks <= 0 {
		t.MaxPacks = DefaultGCPackThreshold
	}
	if t.MaxLooseObjects <= 0 {
		t.MaxLooseObjects = DefaultGCLooseThreshold
	}
	return t
}

// GCTrigger identifies why on-demand GC was triggered on a base clone.
type GCTrigger string

const (
	// GCTriggerNone indicates neither pack nor loose-object threshold was met.
	GCTriggerNone GCTrigger = ""
	// GCTriggerPacks indicates the .git/objects/pack .idx threshold was met.
	GCTriggerPacks GCTrigger = "packs"
	// GCTriggerLoose indicates the .git/objects/17 loose-object threshold was met.
	GCTriggerLoose GCTrigger = "loose_objects"
)

// GCStatus holds the zero-subprocess object-store counts and trigger decision.
type GCStatus struct {
	PackCount        int
	LooseBucketCount int
	Trigger          GCTrigger
}

// GCResult captures the outcome of a MaybeGC check and optional synchronous git gc run.
type GCResult struct {
	Triggered        bool
	Trigger          GCTrigger
	PackCount        int
	LooseBucketCount int
	Duration         time.Duration
}

// SetGCThresholds updates the on-demand GC thresholds for this BaseCloneCache.
// Non-positive values fall back to the defaults.
func (c *BaseCloneCache) SetGCThresholds(t GCThresholds) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gcThresholds = t.normalize()
}

// GCThresholds returns the active on-demand GC thresholds for this BaseCloneCache.
func (c *BaseCloneCache) GCThresholds() GCThresholds {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gcThresholds.normalize()
}

// InspectGC performs the zero-subprocess heuristic check (2 os.ReadDir calls)
// against baseDir/.git/objects/pack and baseDir/.git/objects/17.
func (c *BaseCloneCache) InspectGC(baseDir string) GCStatus {
	thresholds := c.GCThresholds()
	packCount := countPackIdx(baseDir)
	looseCount := countLooseBucket17(baseDir)

	status := GCStatus{
		PackCount:        packCount,
		LooseBucketCount: looseCount,
		Trigger:          GCTriggerNone,
	}
	switch {
	case packCount >= thresholds.MaxPacks:
		status.Trigger = GCTriggerPacks
	case looseCount >= thresholds.MaxLooseObjects:
		status.Trigger = GCTriggerLoose
	}
	return status
}

// MaybeGC checks the zero-subprocess object-store heuristics for baseGit.WorkDir()
// and, when a threshold is met and no linked worktree is active, runs synchronous
// foreground `git -c gc.autoDetach=false gc`.
func (c *BaseCloneCache) MaybeGC(ctx context.Context, baseGit *GitClient) (GCResult, error) {
	if ctx.Err() != nil || baseGit == nil || baseGit.WorkDir() == "" {
		return GCResult{}, nil
	}

	baseDir := baseGit.WorkDir()
	status := c.InspectGC(baseDir)
	res := GCResult{
		PackCount:        status.PackCount,
		LooseBucketCount: status.LooseBucketCount,
	}
	if status.Trigger == GCTriggerNone {
		return res, nil
	}
	if hasActiveWorktrees(baseDir) {
		return res, nil
	}

	start := time.Now()
	err := baseGit.GC(ctx)
	res.Triggered = true
	res.Trigger = status.Trigger
	res.Duration = time.Since(start)
	return res, err
}

func countPackIdx(baseDir string) int {
	entries, err := os.ReadDir(filepath.Join(baseDir, ".git", "objects", "pack"))
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".idx") {
			count++
		}
	}
	return count
}

func countLooseBucket17(baseDir string) int {
	entries, err := os.ReadDir(filepath.Join(baseDir, ".git", "objects", "17"))
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
		}
	}
	return count
}

func hasActiveWorktrees(baseDir string) bool {
	entries, err := os.ReadDir(filepath.Join(baseDir, ".git", "worktrees"))
	if err != nil {
		return false
	}
	return len(entries) > 0
}
