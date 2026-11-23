package node_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/node"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

type mapBundleResolver struct {
	mu   sync.RWMutex
	urls map[tenant.RepoKey]string
}

func newMapBundleResolver() *mapBundleResolver {
	return &mapBundleResolver{urls: make(map[tenant.RepoKey]string)}
}

func (r *mapBundleResolver) Set(repo tenant.RepoKey, url string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if url == "" {
		delete(r.urls, repo)
		return
	}
	r.urls[repo] = url
}

func (r *mapBundleResolver) ResolveBundleURI(_ context.Context, repo tenant.RepoKey) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.urls[repo], nil
}

func TestAdvertiseV2Capabilities_BundleURI(t *testing.T) {
	withBundle := string(node.AdvertiseV2Capabilities(node.AdvertiseOptions{
		IncludeServiceHeader: true,
		ServiceName:          "git-upload-pack",
		ObjectFormat:         ddb.ObjectFormatSHA1,
		AdvertiseBundleURI:   true,
	}))
	if !strings.Contains(withBundle, "bundle-uri\n") {
		t.Fatalf("expected bundle-uri capability when AdvertiseBundleURI=true, got:\n%s", withBundle)
	}

	withoutBundle := string(node.AdvertiseV2Capabilities(node.AdvertiseOptions{
		IncludeServiceHeader: true,
		ServiceName:          "git-upload-pack",
		ObjectFormat:         ddb.ObjectFormatSHA1,
		AdvertiseBundleURI:   false,
	}))
	if strings.Contains(withoutBundle, "bundle-uri") {
		t.Fatalf("unexpected bundle-uri capability when AdvertiseBundleURI=false:\n%s", withoutBundle)
	}

	// Protocol v0/v1 AdvertiseRefs must never include bundle-uri.
	v1 := string(node.AdvertiseRefs(nil, "refs/heads/main", node.AdvertiseOptions{
		IncludeServiceHeader: true,
		ServiceName:          "git-upload-pack",
		AdvertiseBundleURI:   true,
	}))
	if strings.Contains(v1, "bundle-uri") {
		t.Fatalf("v1 AdvertiseRefs must not advertise bundle-uri:\n%s", v1)
	}
}

func TestGit3Phase4_Reftable_MIDXBitmapCommitGraph_And_BundleURI_DualHash(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	bundleResolver := newMapBundleResolver()
	stack := newInMemoryStackWithBundleResolver(t, bundleResolver)

	// Start an HTTP server simulating presigned S3 bundle GET downloads.
	var (
		bundleMu       sync.RWMutex
		bundlePayloads = make(map[string][]byte)
		bundleHits     atomic.Int32
	)
	bundleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bundleMu.RLock()
		data, ok := bundlePayloads[r.URL.Path]
		bundleMu.RUnlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		bundleHits.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	t.Cleanup(bundleSrv.Close)

	for _, tc := range []struct {
		name      string
		repoID    string
		format    ddb.ObjectFormat
		objFormat string
	}{
		{name: "SHA1", repoID: "phase4-sha1", format: ddb.ObjectFormatSHA1, objFormat: "sha1"},
		{name: "SHA256", repoID: "phase4-sha256", format: ddb.ObjectFormatSHA256, objFormat: "sha256"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rk := stack.createRepo(tc.repoID, tc.format)
			httpURL := stack.httpRepoURL(tc.repoID)

			// 1. Push a commit to populate the repository.
			seedDir := filepath.Join(t.TempDir(), "seed")
			stack.runGit("", nil, "init", "--initial-branch=main", "--object-format="+tc.objFormat, seedDir)
			if err := os.WriteFile(filepath.Join(seedDir, "README.md"), []byte("phase4 "+tc.objFormat+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			stack.runGit(seedDir, nil, "add", "-A")
			stack.runGit(seedDir, nil, "commit", "-m", "initial commit")
			commit1 := strings.TrimSpace(stack.runGit(seedDir, nil, "rev-parse", "HEAD"))
			stack.runGit(seedDir, nil, "-c", "protocol.version=2", "push", httpURL, "HEAD:refs/heads/main")

			// 2. Clone WITHOUT bundle-uri first -> triggers SyncAndPin on stravin-node.
			cloneNoBundle := filepath.Join(t.TempDir(), "clone-no-bundle")
			noBundleOut := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=2", "clone", httpURL, cloneNoBundle)
			if strings.Contains(noBundleOut, "git< bundle-uri") {
				t.Fatalf("did not expect bundle-uri before bundle is published:\n%s", noBundleOut)
			}
			stack.runGit(cloneNoBundle, nil, "fsck", "--full", "--strict")

			// 3. Verify NVMe bare cache repo uses reftable + MIDX + .bitmap + commit-graph.
			repoPath, err := stack.nvmeCache.RepoPath(rk)
			if err != nil {
				t.Fatalf("RepoPath: %v", err)
			}
			if _, err := os.Stat(filepath.Join(repoPath, "reftable", "tables.list")); err != nil {
				t.Fatalf("missing reftable/tables.list: %v", err)
			}
			refFmt := strings.TrimSpace(stack.runGit("", nil, "--git-dir", repoPath, "rev-parse", "--show-ref-format"))
			if refFmt != "reftable" {
				t.Fatalf("bare cache ref-format = %q, want reftable", refFmt)
			}
			objFmt := strings.TrimSpace(stack.runGit("", nil, "--git-dir", repoPath, "rev-parse", "--show-object-format"))
			if objFmt != tc.objFormat {
				t.Fatalf("bare cache object-format = %q, want %s", objFmt, tc.objFormat)
			}
			if _, err := os.Stat(filepath.Join(repoPath, "objects", "pack", "multi-pack-index")); err != nil {
				t.Fatalf("missing multi-pack-index in NVMe cache: %v", err)
			}
			bitmaps, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.bitmap"))
			if err != nil || len(bitmaps) == 0 {
				t.Fatalf("missing .bitmap in NVMe cache: glob=%v err=%v", bitmaps, err)
			}
			if _, err := os.Stat(filepath.Join(repoPath, "objects", "info", "commit-graph")); err != nil {
				t.Fatalf("missing commit-graph in NVMe cache: %v", err)
			}

			// 4. Generate a Git bundle from the bare repo and expose it via bundle-uri.
			bundlePath := filepath.Join(t.TempDir(), tc.repoID+".bundle")
			gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
			if err := gitExec.ExecBundleCreate(context.Background(), gitexec.BundleCreateRequest{
				Repo:       rk,
				RepoDir:    repoPath,
				OutputPath: bundlePath,
			}); err != nil {
				t.Fatalf("ExecBundleCreate: %v", err)
			}
			bundleData, err := os.ReadFile(bundlePath)
			if err != nil {
				t.Fatalf("read bundle: %v", err)
			}
			bundleRel := "/bundles/acme/" + tc.repoID + "/latest.bundle"
			bundleMu.Lock()
			bundlePayloads[bundleRel] = bundleData
			bundleMu.Unlock()
			bundleResolver.Set(rk, bundleSrv.URL+bundleRel)

			// 5. Clone with Protocol v2 + transfer.bundleURI=true -> verifies bundle-uri offload!
			hitsBefore := bundleHits.Load()
			cloneWithBundle := filepath.Join(t.TempDir(), "clone-with-bundle")
			withBundleOut := stack.runGit(
				"",
				[]string{"GIT_TRACE_PACKET=1"},
				"-c", "protocol.version=2",
				"-c", "transfer.bundleURI=true",
				"clone", httpURL, cloneWithBundle,
			)
			if !strings.Contains(withBundleOut, "bundle-uri") {
				t.Fatalf("expected bundle-uri in Protocol v2 clone trace:\n%s", withBundleOut)
			}
			if bundleHits.Load() <= hitsBefore {
				t.Fatalf("expected bundle HTTP server to be hit during bundle-uri clone (hits=%d)", bundleHits.Load())
			}
			if got := strings.TrimSpace(stack.runGit(cloneWithBundle, nil, "rev-parse", "HEAD")); got != commit1 {
				t.Fatalf("bundle-uri clone HEAD = %s, want %s", got, commit1)
			}
			stack.runGit(cloneWithBundle, nil, "fsck", "--full", "--strict")

			// 6. Protocol v0 fallback ignores bundle-uri and succeeds via standard upload-pack.
			hitsBeforeV0 := bundleHits.Load()
			cloneV0 := filepath.Join(t.TempDir(), "clone-v0-fallback")
			v0Out := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=0", "clone", httpURL, cloneV0)
			if strings.Contains(v0Out, "bundle-uri") {
				t.Fatalf("Protocol v0 clone must not advertise bundle-uri:\n%s", v0Out)
			}
			if bundleHits.Load() != hitsBeforeV0 {
				t.Fatalf("Protocol v0 clone should not fetch bundle URI")
			}
			stack.runGit(cloneV0, nil, "fsck", "--full", "--strict")
		})
	}
}

// Helper to expose nvmeCache and optional BundleURIResolver from inMemoryStack.
func newInMemoryStackWithBundleResolver(t *testing.T, resolver node.BundleURIResolver) *inMemoryStackWithCache {
	t.Helper()
	stack := newInMemoryStackWithOptions(t, resolver)
	return stack
}

type inMemoryStackWithCache struct {
	*inMemoryStack
	nvmeCache *cache.Cache
}
