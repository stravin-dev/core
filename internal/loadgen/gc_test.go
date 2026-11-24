package loadgen

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // git object IDs use SHA-1
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func initTestRepo(t *testing.T) (string, *GitClient) {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "base")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	g := NewGitClient(dir, GitOptions{}).WithAuthor("Test User", "test@stravin.local")
	if _, err := g.Run(ctx, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := g.WriteFile("README.md", []byte("initial\n")); err != nil {
		t.Fatal(err)
	}
	if err := g.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Commit(ctx, "initial commit"); err != nil {
		t.Fatal(err)
	}
	return dir, g
}

func countPackIdxFiles(t *testing.T, baseDir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(baseDir, ".git", "objects", "pack"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("ReadDir pack: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".idx") {
			n++
		}
	}
	return n
}

// writeValidLooseObjectsInBucket writes count valid zlib-compressed git blob objects
// whose SHA-1 hashes begin with bucketHex (e.g. "17") into <baseDir>/.git/objects/<bucketHex>.
func writeValidLooseObjectsInBucket(t *testing.T, baseDir, bucketHex string, count int) {
	t.Helper()
	bucketDir := filepath.Join(baseDir, ".git", "objects", bucketHex)
	if err := os.MkdirAll(bucketDir, 0o750); err != nil {
		t.Fatal(err)
	}
	written := 0
	for i := 0; written < count; i++ {
		payload := fmt.Sprintf("loadgen-gc-test-blob-%d\n", i)
		raw := fmt.Sprintf("blob %d\x00%s", len(payload), payload)
		sum := sha1.Sum([]byte(raw)) //nolint:gosec // git blob object hashing
		oid := hex.EncodeToString(sum[:])
		if !strings.HasPrefix(oid, bucketHex) {
			continue
		}
		var compressed bytes.Buffer
		zw := zlib.NewWriter(&compressed)
		if _, err := zw.Write([]byte(raw)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		objPath := filepath.Join(bucketDir, oid[2:])
		if err := os.WriteFile(objPath, compressed.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
		written++
	}
}

func TestBaseCloneCacheMaybeGC_BelowThresholdsNoOp(t *testing.T) {
	ctx := context.Background()
	baseDir, baseGit := initTestRepo(t)
	cache := NewBaseCloneCache(filepath.Dir(baseDir))

	res, err := cache.MaybeGC(ctx, baseGit)
	if err != nil {
		t.Fatalf("MaybeGC below thresholds returned error: %v", err)
	}
	if res.Triggered {
		t.Fatalf("expected Triggered=false below thresholds, got %+v", res)
	}
	if res.Trigger != GCTriggerNone {
		t.Fatalf("expected Trigger=%q, got %q", GCTriggerNone, res.Trigger)
	}
	if packs := countPackIdxFiles(t, baseDir); packs != 0 {
		t.Fatalf("expected 0 packfiles (git gc should not have run), got %d", packs)
	}
}

func TestBaseCloneCacheMaybeGC_AbovePackThresholdTriggersSyncGC(t *testing.T) {
	ctx := context.Background()
	baseDir, baseGit := initTestRepo(t)
	cache := NewBaseCloneCache(filepath.Dir(baseDir))
	cache.SetGCThresholds(GCThresholds{MaxPacks: 2, MaxLooseObjects: DefaultGCLooseThreshold})

	// Create 1st packfile from initial commit.
	if _, err := baseGit.Run(ctx, "repack", "-d"); err != nil {
		t.Fatalf("1st repack: %v", err)
	}
	// Create 2nd commit and 2nd incremental packfile.
	if err := baseGit.WriteFile("second.txt", []byte("second pack\n")); err != nil {
		t.Fatal(err)
	}
	if err := baseGit.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := baseGit.Commit(ctx, "second commit"); err != nil {
		t.Fatal(err)
	}
	if _, err := baseGit.Run(ctx, "repack", "-d"); err != nil {
		t.Fatalf("2nd repack: %v", err)
	}

	if got := countPackIdxFiles(t, baseDir); got < 2 {
		t.Fatalf("setup expected >= 2 packfiles before GC, got %d", got)
	}

	res, err := cache.MaybeGC(ctx, baseGit)
	if err != nil {
		t.Fatalf("MaybeGC above pack threshold failed: %v", err)
	}
	if !res.Triggered {
		t.Fatalf("expected Triggered=true, got %+v", res)
	}
	if res.Trigger != GCTriggerPacks {
		t.Fatalf("Trigger = %q, want %q", res.Trigger, GCTriggerPacks)
	}
	if res.PackCount < 2 {
		t.Fatalf("PackCount = %d, want >= 2", res.PackCount)
	}
	if got := countPackIdxFiles(t, baseDir); got != 1 {
		t.Fatalf("expected synchronous git gc to consolidate packs to 1, got %d", got)
	}
	if err := baseGit.Fsck(ctx); err != nil {
		t.Fatalf("git fsck after GC failed: %v", err)
	}
}

func TestBaseCloneCacheMaybeGC_AboveLooseObjectThresholdTriggersSyncGC(t *testing.T) {
	ctx := context.Background()
	baseDir, baseGit := initTestRepo(t)
	cache := NewBaseCloneCache(filepath.Dir(baseDir))

	// Populate default threshold (27) valid loose objects in .git/objects/17.
	writeValidLooseObjectsInBucket(t, baseDir, "17", DefaultGCLooseThreshold)

	res, err := cache.MaybeGC(ctx, baseGit)
	if err != nil {
		t.Fatalf("MaybeGC above loose threshold failed: %v", err)
	}
	if !res.Triggered {
		t.Fatalf("expected Triggered=true, got %+v", res)
	}
	if res.Trigger != GCTriggerLoose {
		t.Fatalf("Trigger = %q, want %q", res.Trigger, GCTriggerLoose)
	}
	if res.LooseBucketCount < DefaultGCLooseThreshold {
		t.Fatalf("LooseBucketCount = %d, want >= %d", res.LooseBucketCount, DefaultGCLooseThreshold)
	}
	// git gc packs reachable objects into a packfile.
	if got := countPackIdxFiles(t, baseDir); got < 1 {
		t.Fatalf("expected git gc to create at least 1 packfile, got %d", got)
	}
	if err := baseGit.Fsck(ctx); err != nil {
		t.Fatalf("git fsck after GC failed: %v", err)
	}
}

func TestBaseCloneCacheMaybeGC_SkipsWhenLinkedWorktreeActive(t *testing.T) {
	ctx := context.Background()
	baseDir, baseGit := initTestRepo(t)
	cache := NewBaseCloneCache(filepath.Dir(baseDir))
	cache.SetGCThresholds(GCThresholds{MaxPacks: 1, MaxLooseObjects: DefaultGCLooseThreshold})

	if _, err := baseGit.Run(ctx, "repack", "-d"); err != nil {
		t.Fatalf("repack: %v", err)
	}
	headSHA, err := baseGit.RevParse(ctx, "HEAD")
	if err != nil {
		t.Fatal(err)
	}

	wtDir := filepath.Join(t.TempDir(), "active-wt")
	if err := baseGit.WorktreeAddDetach(ctx, wtDir, headSHA); err != nil {
		t.Fatalf("WorktreeAddDetach: %v", err)
	}

	// While linked worktree is active, MaybeGC must not trigger GC.
	res, err := cache.MaybeGC(ctx, baseGit)
	if err != nil {
		t.Fatalf("MaybeGC with active worktree error: %v", err)
	}
	if res.Triggered {
		t.Fatalf("expected Triggered=false while linked worktree is active, got %+v", res)
	}

	// Once linked worktree is removed, MaybeGC triggers cleanly.
	if err := baseGit.WorktreeRemove(ctx, wtDir); err != nil {
		t.Fatalf("WorktreeRemove: %v", err)
	}
	resAfter, err := cache.MaybeGC(ctx, baseGit)
	if err != nil {
		t.Fatalf("MaybeGC after worktree removal error: %v", err)
	}
	if !resAfter.Triggered || resAfter.Trigger != GCTriggerPacks {
		t.Fatalf("expected Triggered=true (packs) after worktree removal, got %+v", resAfter)
	}
}
