package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"image"
	"image/color"
	"image/png"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type gitDataFixture struct {
	repoKey       tenant.RepoKey
	client        rpc.GitDataClient
	grpcSrv       *grpc.Server
	tp            *telemetry.Provider
	rootCommit    string
	secondCommit  string
	branchCommit  string
	mainCommit    string
	pngBlobOID    string
	largeBlobOID  string
	largeBlobHash [32]byte
	emptyBlobOID  string
	assetsTreeOID string
	readmeBlobOID string
}

func makeTestPNG(t *testing.T, c color.RGBA) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	for y := range 2 {
		for x := range 2 {
			img.SetRGBA(x, y, c)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("png.Encode: %v", err)
	}
	return buf.Bytes()
}

func setupGitDataFixture(t *testing.T, extraServerOpts ...grpc.ServerOption) *gitDataFixture {
	t.Helper()

	workDir := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test",
			"GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test",
			"GIT_COMMITTER_EMAIL=test@example.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	runGit("init", "-b", "main")

	// Commit 1 (root): README.md, main.go, keep.txt, to_delete.txt, assets/logo.png, assets/raw.bin, empty.txt
	writeFile := func(rel string, data []byte) {
		t.Helper()
		full := filepath.Join(workDir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeFile("README.md", []byte("# Stravin\nline 2\nline 3\n"))
	writeFile("main.go", []byte("package main\n\nfunc main() {}\n"))
	writeFile("old_name.txt", []byte("alpha\nbeta\ngamma\ndelta\nepsilon\n"))
	writeFile("to_delete.txt", []byte("goodbye line 1\ngoodbye line 2\n"))
	writeFile("empty.txt", []byte{})
	png1 := makeTestPNG(t, color.RGBA{R: 255, G: 0, B: 0, A: 255})
	writeFile("assets/logo.png", png1)
	writeFile("assets/raw.bin", []byte{0x00, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02})

	// 512 KiB deterministic large blob for streaming test.
	largeData := make([]byte, 512*1024)
	for i := range largeData {
		largeData[i] = byte((i*31 + 7) & 0xff)
	}
	writeFile("assets/large.bin", largeData)

	runGit("add", ".")
	runGit("commit", "-m", "initial commit")
	rootCommit := runGit("rev-parse", "HEAD")

	// Commit 2: modify README.md, rename old_name.txt -> new_name.txt with edit, delete to_delete.txt,
	// add added.go, update assets/logo.png (new PNG), update assets/raw.bin.
	writeFile("README.md", []byte("# Stravin Core\nline 2\nline 3\nline 4\n"))
	if err := os.Remove(filepath.Join(workDir, "old_name.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile("new_name.txt", []byte("alpha\nbeta\ngamma-edited\ndelta\nepsilon\n"))
	if err := os.Remove(filepath.Join(workDir, "to_delete.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile("added.go", []byte("package main\n\nfunc helper() int { return 42 }\n"))
	png2 := makeTestPNG(t, color.RGBA{R: 0, G: 0, B: 255, A: 255})
	writeFile("assets/logo.png", png2)
	writeFile("assets/raw.bin", []byte{0x00, 0xca, 0xfe, 0xba, 0xbe, 0x00, 0x09})

	runGit("add", "-A")
	runGit("commit", "-m", "second commit")
	secondCommit := runGit("rev-parse", "HEAD")

	// Divergent branch for three-dot merge-base testing.
	runGit("checkout", "-b", "feature")
	writeFile("feature.go", []byte("package main\n\nfunc feature() {}\n"))
	runGit("add", "feature.go")
	runGit("commit", "-m", "feature commit")
	branchCommit := runGit("rev-parse", "HEAD")

	runGit("checkout", "main")
	writeFile("main_only.txt", []byte("main branch change\n"))
	runGit("add", "main_only.txt")
	runGit("commit", "-m", "main diverge commit")
	mainCommit := runGit("rev-parse", "HEAD")

	pngBlobOID := runGit("rev-parse", secondCommit+":assets/logo.png")
	largeBlobOID := runGit("rev-parse", secondCommit+":assets/large.bin")
	emptyBlobOID := runGit("rev-parse", secondCommit+":empty.txt")
	assetsTreeOID := runGit("rev-parse", secondCommit+":assets")
	readmeBlobOID := runGit("rev-parse", secondCommit+":README.md")

	// Repack all objects into a single pack + idx so Materializer can hydrate from fakePackFetcher.
	runGit("repack", "-a", "-d", "-q")
	packDir := filepath.Join(workDir, ".git", "objects", "pack")
	packMatches, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil || len(packMatches) != 1 {
		t.Fatalf("expected 1 packfile, got %v (err=%v)", packMatches, err)
	}
	packBytes, err := os.ReadFile(packMatches[0])
	if err != nil {
		t.Fatalf("read pack: %v", err)
	}
	idxPath := strings.TrimSuffix(packMatches[0], ".pack") + ".idx"
	idxBytes, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read idx: %v", err)
	}
	packSum := sha256.Sum256(packBytes)
	packID := hex.EncodeToString(packSum[:])

	rk, err := tenant.NewRepoKey("acme", "gitdata-repo")
	if err != nil {
		t.Fatal(err)
	}

	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: mainCommit},
		{Repo: rk, RefName: "refs/heads/feature", OID: branchCommit},
		{Repo: rk, RefName: "refs/tags/v1.0.0", OID: secondCommit},
		{Repo: rk, RefName: "refs/stravin/pr/42/head", OID: branchCommit},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo:      rk,
		ULID:      "01JDF8Z7Q2K9M4N6P8R0T2V4W6",
		PackID:    packID,
		SizeBytes: int64(len(packBytes)),
		Status:    ddb.PackStatusActive,
	})

	packFetcher := newFakePackFetcher()
	packFetcher.Put(packID, packBytes, idxBytes)

	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        metaStore,
		Packs:       packFetcher,
		Cache:       nvmeCache,
		GitExecutor: gitExec,
		Telemetry:   tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	gitDataSrv, err := NewGitDataServer(GitDataServerConfig{
		Materializer: mat,
		GitDataExec:  gitExec,
		Telemetry:    tp,
		Region:       "us-east-1",
		BlobChunkSz:  16 * 1024, // 16 KiB chunks to exercise multi-chunk streaming
	})
	if err != nil {
		t.Fatalf("NewGitDataServer: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	grpcSrv := grpc.NewServer(extraServerOpts...)
	rpc.RegisterGitDataServer(grpcSrv, gitDataSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	return &gitDataFixture{
		repoKey:       rk,
		client:        rpc.NewGRPCGitDataClient(cc),
		grpcSrv:       grpcSrv,
		tp:            tp,
		rootCommit:    rootCommit,
		secondCommit:  secondCommit,
		branchCommit:  branchCommit,
		mainCommit:    mainCommit,
		pngBlobOID:    pngBlobOID,
		largeBlobOID:  largeBlobOID,
		largeBlobHash: sha256.Sum256(largeData),
		emptyBlobOID:  emptyBlobOID,
		assetsTreeOID: assetsTreeOID,
		readmeBlobOID: readmeBlobOID,
	}
}

func TestGitData_GetDiff_TextRenamesDeletesAndBinaries(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	resp, err := fx.client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BaseOID:  fx.rootCommit,
		HeadOID:  fx.secondCommit,
	})
	if err != nil {
		t.Fatalf("GetDiff: %v", err)
	}
	if resp.BaseOID != fx.rootCommit || resp.HeadOID != fx.secondCommit {
		t.Fatalf("base/head = %s/%s, want %s/%s", resp.BaseOID, resp.HeadOID, fx.rootCommit, fx.secondCommit)
	}

	byPath := make(map[string]rpc.DiffFileEntry, len(resp.Files))
	for _, f := range resp.Files {
		byPath[f.Path] = f
	}

	// 1. Modified text file (README.md)
	readme, ok := byPath["README.md"]
	if !ok {
		t.Fatalf("missing README.md in diff files: %+v", resp.Files)
	}
	if readme.ChangeKind != "modify" || readme.IsBinary || readme.MimeType != "text/markdown" {
		t.Fatalf("README.md unexpected metadata: %+v", readme)
	}
	if readme.OldBlobOID == "" || readme.NewBlobOID == "" || readme.OldSizeBytes == 0 || readme.NewSizeBytes == 0 {
		t.Fatalf("README.md missing blob OIDs/sizes: %+v", readme)
	}
	if len(readme.Hunks) == 0 || readme.Additions != 2 || readme.Deletions != 1 {
		t.Fatalf("README.md hunks=%d adds=%d dels=%d, want >0, 2, 1", len(readme.Hunks), readme.Additions, readme.Deletions)
	}

	// 2. Added Go file (added.go)
	added, ok := byPath["added.go"]
	if !ok {
		t.Fatalf("missing added.go")
	}
	if added.ChangeKind != "add" || added.IsBinary || added.MimeType != "text/x-go" {
		t.Fatalf("added.go unexpected metadata: %+v", added)
	}
	if added.OldBlobOID != "" || added.NewBlobOID == "" || added.OldSizeBytes != 0 || added.NewSizeBytes == 0 {
		t.Fatalf("added.go blob metadata invalid: %+v", added)
	}
	if len(added.Hunks) != 1 || added.Additions != 3 || added.Deletions != 0 {
		t.Fatalf("added.go hunks=%d adds=%d dels=%d", len(added.Hunks), added.Additions, added.Deletions)
	}

	// 3. Deleted text file (to_delete.txt)
	deleted, ok := byPath["to_delete.txt"]
	if !ok {
		t.Fatalf("missing to_delete.txt")
	}
	if deleted.ChangeKind != "delete" || deleted.IsBinary || deleted.OldBlobOID == "" || deleted.NewBlobOID != "" {
		t.Fatalf("to_delete.txt unexpected metadata: %+v", deleted)
	}
	if deleted.Deletions != 2 || deleted.Additions != 0 {
		t.Fatalf("to_delete.txt adds=%d dels=%d, want 0, 2", deleted.Additions, deleted.Deletions)
	}

	// 4. Renamed text file with edit (old_name.txt -> new_name.txt)
	renamed, ok := byPath["new_name.txt"]
	if !ok {
		t.Fatalf("missing new_name.txt rename")
	}
	if renamed.ChangeKind != "rename" || renamed.OldPath != "old_name.txt" || renamed.IsBinary {
		t.Fatalf("new_name.txt unexpected rename metadata: %+v", renamed)
	}
	if len(renamed.Hunks) == 0 || renamed.Additions != 1 || renamed.Deletions != 1 {
		t.Fatalf("new_name.txt hunks=%d adds=%d dels=%d", len(renamed.Hunks), renamed.Additions, renamed.Deletions)
	}

	// 5. Binary PNG raster image (assets/logo.png)
	logo, ok := byPath["assets/logo.png"]
	if !ok {
		t.Fatalf("missing assets/logo.png")
	}
	if !logo.IsBinary || logo.MimeType != "image/png" {
		t.Fatalf("assets/logo.png is_binary=%v mime=%q, want true image/png", logo.IsBinary, logo.MimeType)
	}
	if logo.OldBlobOID == "" || logo.NewBlobOID != fx.pngBlobOID || logo.OldSizeBytes == 0 || logo.NewSizeBytes == 0 {
		t.Fatalf("assets/logo.png OIDs/sizes invalid: %+v", logo)
	}
	if len(logo.Hunks) != 0 || len(logo.UnifiedPatch) != 0 {
		t.Fatalf("binary PNG must not emit text hunks or unified patch: hunks=%d patch=%d", len(logo.Hunks), len(logo.UnifiedPatch))
	}

	// 6. Arbitrary binary file (assets/raw.bin)
	rawBin, ok := byPath["assets/raw.bin"]
	if !ok {
		t.Fatalf("missing assets/raw.bin")
	}
	if !rawBin.IsBinary || rawBin.MimeType != "application/octet-stream" {
		t.Fatalf("assets/raw.bin is_binary=%v mime=%q, want true application/octet-stream", rawBin.IsBinary, rawBin.MimeType)
	}
	if len(rawBin.Hunks) != 0 || len(rawBin.UnifiedPatch) != 0 {
		t.Fatalf("binary raw.bin must not emit text hunks")
	}
}

func TestGitData_GetDiff_ParentRootThreeDotPaginationAndTruncation(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	// Empty base_oid on secondCommit diffs against its first parent (rootCommit).
	parentResp, err := fx.client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		HeadOID:  fx.secondCommit,
	})
	if err != nil {
		t.Fatalf("GetDiff parent: %v", err)
	}
	if parentResp.BaseOID != fx.rootCommit || len(parentResp.Files) != 6 {
		t.Fatalf("parent diff base=%s files=%d, want %s / 6", parentResp.BaseOID, len(parentResp.Files), fx.rootCommit)
	}

	// Empty base_oid on rootCommit diffs against empty tree.
	rootResp, err := fx.client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		HeadOID:  fx.rootCommit,
	})
	if err != nil {
		t.Fatalf("GetDiff root: %v", err)
	}
	if rootResp.BaseOID != emptyTreeSHA1 || len(rootResp.Files) == 0 {
		t.Fatalf("root diff base=%s files=%d", rootResp.BaseOID, len(rootResp.Files))
	}

	// Three-dot diff between mainCommit and branchCommit uses merge-base (secondCommit),
	// so only feature.go appears (not main_only.txt).
	threeDotResp, err := fx.client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BaseOID:  fx.mainCommit,
		HeadOID:  fx.branchCommit,
		ThreeDot: true,
	})
	if err != nil {
		t.Fatalf("GetDiff three_dot: %v", err)
	}
	if threeDotResp.MergeBaseOID != fx.secondCommit {
		t.Fatalf("merge_base_oid = %s, want %s", threeDotResp.MergeBaseOID, fx.secondCommit)
	}
	if len(threeDotResp.Files) != 1 || threeDotResp.Files[0].Path != "feature.go" {
		t.Fatalf("three_dot files = %+v, want only feature.go", threeDotResp.Files)
	}

	// Pagination + patch truncation.
	page1, err := fx.client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID:      string(fx.repoKey.TenantID),
		RepoID:        string(fx.repoKey.RepoID),
		BaseOID:       fx.rootCommit,
		HeadOID:       fx.secondCommit,
		MaxFiles:      2,
		MaxPatchBytes: 16,
	})
	if err != nil {
		t.Fatalf("GetDiff page1: %v", err)
	}
	if len(page1.Files) != 2 || page1.NextPageToken != "2" {
		t.Fatalf("page1 len=%d next=%q, want 2 / \"2\"", len(page1.Files), page1.NextPageToken)
	}
	for _, f := range page1.Files {
		if !f.IsBinary && (!f.PatchTruncated || len(f.UnifiedPatch) != 16) {
			t.Fatalf("expected truncated patch of 16 bytes for %s, got truncated=%v len=%d", f.Path, f.PatchTruncated, len(f.UnifiedPatch))
		}
	}
}

func TestGitData_GetBlob_StreamsLargeBlobInBoundedChunks(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	stream, err := fx.client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BlobOID:  fx.largeBlobOID,
	})
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}

	h := sha256.New()
	var (
		totalReceived uint64
		reportedTotal uint64
		dataChunks    int
		sawEOF        bool
	)
	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("GetBlob Recv: %v", recvErr)
		}
		reportedTotal = chunk.TotalBytes
		if len(chunk.Data) > 0 {
			if len(chunk.Data) > 16*1024 {
				t.Fatalf("chunk size %d exceeded configured 16 KiB bound", len(chunk.Data))
			}
			dataChunks++
			totalReceived += uint64(len(chunk.Data))
			_, _ = h.Write(chunk.Data)
		}
		if chunk.EOF {
			sawEOF = true
			break
		}
	}
	if !sawEOF {
		t.Fatal("expected final EOF chunk")
	}
	if totalReceived != 512*1024 || reportedTotal != 512*1024 {
		t.Fatalf("received=%d reported=%d, want %d", totalReceived, reportedTotal, 512*1024)
	}
	if dataChunks < 32 {
		t.Fatalf("expected >= 32 bounded chunks for 512 KiB blob, got %d", dataChunks)
	}
	if !bytes.Equal(h.Sum(nil), fx.largeBlobHash[:]) {
		t.Fatal("streamed blob SHA-256 mismatch")
	}

	// Empty 0-byte blob streams cleanly with EOF.
	emptyStream, err := fx.client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BlobOID:  fx.emptyBlobOID,
	})
	if err != nil {
		t.Fatalf("GetBlob empty: %v", err)
	}
	emptyChunk, err := emptyStream.Recv()
	if err != nil || !emptyChunk.EOF || emptyChunk.TotalBytes != 0 || len(emptyChunk.Data) != 0 {
		t.Fatalf("empty blob chunk = (%+v, %v), want EOF total=0", emptyChunk, err)
	}

	// MaxBytes cap enforced.
	capStream, err := fx.client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BlobOID:  fx.largeBlobOID,
		MaxBytes: 1024,
	})
	if err == nil {
		_, err = capStream.Recv()
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("GetBlob over MaxBytes code = %v, want ResourceExhausted", status.Code(err))
	}

	// Non-blob OID rejected with FailedPrecondition.
	treeStream, err := fx.client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BlobOID:  fx.assetsTreeOID,
	})
	if err == nil {
		_, err = treeStream.Recv()
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetBlob on tree OID code = %v, want FailedPrecondition", status.Code(err))
	}

	// Missing OID returns NotFound.
	missStream, err := fx.client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BlobOID:  strings.Repeat("f", 40),
	})
	if err == nil {
		_, err = missStream.Recv()
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetBlob missing OID code = %v, want NotFound", status.Code(err))
	}
}

func TestGitData_ListTree(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	// Root tree by commit_oid.
	rootResp, err := fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
	})
	if err != nil {
		t.Fatalf("ListTree root: %v", err)
	}
	if rootResp.TreeOID == "" || len(rootResp.Entries) < 5 {
		t.Fatalf("unexpected root tree response: %+v", rootResp)
	}
	var sawAssetsDir, sawReadme bool
	for _, e := range rootResp.Entries {
		if e.Path == "assets" && e.Name == "assets" && e.Type == "tree" && e.Mode == "040000" && e.SizeBytes == 0 && e.OID == fx.assetsTreeOID {
			sawAssetsDir = true
		}
		if e.Path == "README.md" && e.Name == "README.md" && e.Type == "blob" && e.Mode == "100644" && e.SizeBytes > 0 && e.OID == fx.readmeBlobOID {
			sawReadme = true
		}
	}
	if !sawAssetsDir || !sawReadme {
		t.Fatalf("root entries missing expected items: %+v", rootResp.Entries)
	}

	// Subdirectory path by commit_oid.
	subResp, err := fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
		Path:      "/assets/",
	})
	if err != nil {
		t.Fatalf("ListTree assets: %v", err)
	}
	if subResp.TreeOID != fx.assetsTreeOID || len(subResp.Entries) != 3 {
		t.Fatalf("assets tree = %+v", subResp)
	}
	var sawLogo bool
	for _, e := range subResp.Entries {
		if e.Name == "logo.png" && e.Path == "assets/logo.png" && e.OID == fx.pngBlobOID && e.SizeBytes > 0 {
			sawLogo = true
		}
	}
	if !sawLogo {
		t.Fatalf("assets entries missing logo.png: %+v", subResp.Entries)
	}

	// Direct tree_oid lookup.
	byTreeResp, err := fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		TreeOID:  fx.assetsTreeOID,
	})
	if err != nil {
		t.Fatalf("ListTree by tree_oid: %v", err)
	}
	if len(byTreeResp.Entries) != 3 {
		t.Fatalf("byTreeResp entries = %d, want 3", len(byTreeResp.Entries))
	}

	// Error paths: traversal, blob path, missing path, ambiguous OIDs.
	_, err = fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
		Path:      "../secret",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("traversal path code = %v, want InvalidArgument", status.Code(err))
	}

	_, err = fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
		Path:      "README.md",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("blob path code = %v, want FailedPrecondition", status.Code(err))
	}

	_, err = fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
		Path:      "does-not-exist",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing path code = %v, want NotFound", status.Code(err))
	}

	_, err = fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
		TreeOID:   fx.assetsTreeOID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("both commit_oid and tree_oid code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestGitData_TenantIsolationAndTelemetrySpans(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	// Cross-tenant query for same repo_id under different tenant_id must fail with NotFound.
	_, err := fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  "other-tenant",
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("cross-tenant ListTree code = %v, want NotFound", status.Code(err))
	}

	// Mismatched metadata header vs request tenant_id rejected with PermissionDenied.
	spoofCtx := metadata.AppendToOutgoingContext(ctx,
		rpc.HeaderTenantID, "other-tenant",
		rpc.HeaderRepoID, string(fx.repoKey.RepoID),
	)
	_, err = fx.client.GetDiff(spoofCtx, &rpc.GetDiffRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		HeadOID:  fx.secondCommit,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed tenant metadata code = %v, want PermissionDenied", status.Code(err))
	}

	// Valid call records OTel span.
	_, err = fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
	})
	if err != nil {
		t.Fatalf("ListTree: %v", err)
	}

	// Await full server handler completion before inspecting recorded spans.
	fx.grpcSrv.GracefulStop()

	var sawTreeSpan bool
	for _, sp := range fx.tp.SpanExporter.GetSpans() {
		if sp.Name == "node.gitdata.list_tree" {
			sawTreeSpan = true
			break
		}
	}
	if !sawTreeSpan {
		t.Fatal("expected node.gitdata.list_tree span to be recorded")
	}
}

func TestGitData_ListRefs_BranchesTagsDefaultPaginationAndValidation(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	// Unfiltered ListRefs returns default branch, HEAD OID, counts, and all refs sorted.
	allResp, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
	})
	if err != nil {
		t.Fatalf("ListRefs all: %v", err)
	}
	if allResp.DefaultBranch != "refs/heads/main" || allResp.HeadOID != fx.mainCommit {
		t.Fatalf("default/head = (%q, %q), want (refs/heads/main, %q)", allResp.DefaultBranch, allResp.HeadOID, fx.mainCommit)
	}
	if allResp.BranchCount != 2 || allResp.TagCount != 1 || len(allResp.Refs) != 4 || allResp.NextCursor != "" {
		t.Fatalf("counts/refs = (branches=%d tags=%d refs=%d next=%q), want (2, 1, 4, \"\")", allResp.BranchCount, allResp.TagCount, len(allResp.Refs), allResp.NextCursor)
	}

	var sawMainDefault, sawTag, sawOther bool
	for _, r := range allResp.Refs {
		switch r.Name {
		case "refs/heads/main":
			if r.ShortName == "main" && r.Kind == "branch" && r.IsDefault && r.TargetOID == fx.mainCommit {
				sawMainDefault = true
			}
		case "refs/tags/v1.0.0":
			if r.ShortName == "v1.0.0" && r.Kind == "tag" && !r.IsDefault && r.TargetOID == fx.secondCommit {
				sawTag = true
			}
		case "refs/stravin/pr/42/head":
			if r.ShortName == "stravin/pr/42/head" && r.Kind == "other" && !r.IsDefault {
				sawOther = true
			}
		}
	}
	if !sawMainDefault || !sawTag || !sawOther {
		t.Fatalf("unexpected ref entries: %+v", allResp.Refs)
	}

	// Prefix filter keeps total repo counts and HeadOID while filtering Refs.
	tagResp, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Prefix:   "refs/tags/",
	})
	if err != nil {
		t.Fatalf("ListRefs tags: %v", err)
	}
	if len(tagResp.Refs) != 1 || tagResp.Refs[0].ShortName != "v1.0.0" || tagResp.BranchCount != 2 || tagResp.TagCount != 1 || tagResp.HeadOID != fx.mainCommit {
		t.Fatalf("tag filtered resp unexpected: %+v", tagResp)
	}

	// Pagination with Limit=1 and Cursor.
	p1, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Prefix:   "refs/heads/",
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("ListRefs page1: %v", err)
	}
	if len(p1.Refs) != 1 || p1.Refs[0].ShortName != "feature" || p1.NextCursor != "1" {
		t.Fatalf("page1 = %+v, want feature + cursor=1", p1)
	}
	p2, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Prefix:   "refs/heads/",
		Limit:    1,
		Cursor:   p1.NextCursor,
	})
	if err != nil {
		t.Fatalf("ListRefs page2: %v", err)
	}
	if len(p2.Refs) != 1 || p2.Refs[0].ShortName != "main" || p2.NextCursor != "" {
		t.Fatalf("page2 = %+v, want main + empty cursor", p2)
	}

	// Validation errors.
	for _, badPrefix := range []string{"../evil", "--flag", "refs/heads/\x00bad"} {
		if _, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
			TenantID: string(fx.repoKey.TenantID),
			RepoID:   string(fx.repoKey.RepoID),
			Prefix:   badPrefix,
		}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("prefix %q code = %v, want InvalidArgument", badPrefix, status.Code(err))
		}
	}
	if _, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Cursor:   "not-a-number",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid cursor code = %v, want InvalidArgument", status.Code(err))
	}

	// Tenant mismatch rejected.
	spoofCtx := metadata.AppendToOutgoingContext(ctx,
		rpc.HeaderTenantID, "other-tenant",
		rpc.HeaderRepoID, string(fx.repoKey.RepoID),
	)
	if _, err := fx.client.ListRefs(spoofCtx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed tenant ListRefs code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestGitData_GetCommitLog_RefsPathsTreePathCommitsAndValidation(t *testing.T) {
	fx := setupGitDataFixture(t)
	ctx := context.Background()

	// Default ref (HEAD -> main) with IncludePathLastCommits=true at root.
	rootLog, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID:               string(fx.repoKey.TenantID),
		RepoID:                 string(fx.repoKey.RepoID),
		IncludePathLastCommits: true,
	})
	if err != nil {
		t.Fatalf("GetCommitLog root: %v", err)
	}
	if rootLog.TotalCommits != 3 || len(rootLog.Commits) != 3 {
		t.Fatalf("total/len = (%d, %d), want (3, 3)", rootLog.TotalCommits, len(rootLog.Commits))
	}
	top := rootLog.Commits[0]
	if top.OID != fx.mainCommit || top.ShortOID != fx.mainCommit[:7] || top.Subject != "main diverge commit" || top.AuthorName != "Test" || top.AuthorEmail != "test@example.com" || top.AuthorTime == "" || top.TreeOID == "" || len(top.ParentOIDs) != 1 || top.ParentOIDs[0] != fx.secondCommit {
		t.Fatalf("top commit unexpected: %+v", top)
	}
	rootCommitEntry := rootLog.Commits[2]
	if rootCommitEntry.OID != fx.rootCommit || len(rootCommitEntry.ParentOIDs) != 0 {
		t.Fatalf("root commit entry unexpected: %+v", rootCommitEntry)
	}

	// Verify per-path last commits at root:
	// main_only.txt -> mainCommit, README.md -> secondCommit, assets -> secondCommit, main.go -> rootCommit.
	if got := rootLog.TreePathCommits["main_only.txt"].OID; got != fx.mainCommit {
		t.Fatalf("TreePathCommits[main_only.txt] = %q, want %q", got, fx.mainCommit)
	}
	if got := rootLog.TreePathCommits["README.md"].OID; got != fx.secondCommit {
		t.Fatalf("TreePathCommits[README.md] = %q, want %q", got, fx.secondCommit)
	}
	if got := rootLog.TreePathCommits["assets"].OID; got != fx.secondCommit {
		t.Fatalf("TreePathCommits[assets] = %q, want %q", got, fx.secondCommit)
	}
	if got := rootLog.TreePathCommits["main.go"].OID; got != fx.rootCommit {
		t.Fatalf("TreePathCommits[main.go] = %q, want %q", got, fx.rootCommit)
	}

	// Subdirectory Path="assets" with IncludePathLastCommits=true:
	// logo.png & raw.bin -> secondCommit, large.bin -> rootCommit.
	assetsLog, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID:               string(fx.repoKey.TenantID),
		RepoID:                 string(fx.repoKey.RepoID),
		Ref:                    "main",
		Path:                   "/assets/",
		IncludePathLastCommits: true,
	})
	if err != nil {
		t.Fatalf("GetCommitLog assets: %v", err)
	}
	if assetsLog.TotalCommits != 2 || len(assetsLog.Commits) != 2 {
		t.Fatalf("assets total/len = (%d, %d), want (2, 2)", assetsLog.TotalCommits, len(assetsLog.Commits))
	}
	if got := assetsLog.TreePathCommits["logo.png"].OID; got != fx.secondCommit {
		t.Fatalf("TreePathCommits[logo.png] = %q, want %q", got, fx.secondCommit)
	}
	if got := assetsLog.TreePathCommits["large.bin"].OID; got != fx.rootCommit {
		t.Fatalf("TreePathCommits[large.bin] = %q, want %q", got, fx.rootCommit)
	}

	// Branch ref + Limit=1.
	featLog, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Ref:      "refs/heads/feature",
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("GetCommitLog feature: %v", err)
	}
	if featLog.TotalCommits != 3 || len(featLog.Commits) != 1 || featLog.Commits[0].OID != fx.branchCommit {
		t.Fatalf("feature log unexpected: %+v", featLog)
	}

	// Tag ref.
	tagLog, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Ref:      "v1.0.0",
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("GetCommitLog tag: %v", err)
	}
	if tagLog.TotalCommits != 2 || len(tagLog.Commits) != 1 || tagLog.Commits[0].OID != fx.secondCommit {
		t.Fatalf("tag log unexpected: %+v", tagLog)
	}

	// File path filter (non-tree) with IncludePathLastCommits=true does not error.
	fileLog, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID:               string(fx.repoKey.TenantID),
		RepoID:                 string(fx.repoKey.RepoID),
		Path:                   "main.go",
		IncludePathLastCommits: true,
	})
	if err != nil {
		t.Fatalf("GetCommitLog file path: %v", err)
	}
	if fileLog.TotalCommits != 1 || len(fileLog.Commits) != 1 || fileLog.Commits[0].OID != fx.rootCommit {
		t.Fatalf("file log unexpected: %+v", fileLog)
	}

	// Validation: invalid ref, path traversal, flag injection, missing ref, tenant mismatch.
	for _, badRef := range []string{"--all", "main..feature", "HEAD@{0}", "main\x00evil"} {
		if _, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
			TenantID: string(fx.repoKey.TenantID),
			RepoID:   string(fx.repoKey.RepoID),
			Ref:      badRef,
		}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ref %q code = %v, want InvalidArgument", badRef, status.Code(err))
		}
	}
	for _, badPath := range []string{"../secret", "-flag", "a/\x00/b"} {
		if _, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
			TenantID: string(fx.repoKey.TenantID),
			RepoID:   string(fx.repoKey.RepoID),
			Path:     badPath,
		}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("path %q code = %v, want InvalidArgument", badPath, status.Code(err))
		}
	}
	if _, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Ref:      "does-not-exist",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing ref code = %v, want NotFound", status.Code(err))
	}
	spoofCtx := metadata.AppendToOutgoingContext(ctx,
		rpc.HeaderTenantID, "other-tenant",
		rpc.HeaderRepoID, string(fx.repoKey.RepoID),
	)
	if _, err := fx.client.GetCommitLog(spoofCtx, &rpc.GetCommitLogRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed tenant GetCommitLog code = %v, want PermissionDenied", status.Code(err))
	}
}

func TestGitData_GetCommitLog_SignedCommitsGPGAndSSH(t *testing.T) {
	workDir := t.TempDir()
	runGit := func(stdin string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = workDir
		if stdin != "" {
			cmd.Stdin = strings.NewReader(stdin)
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	runGit("", "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("signed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("", "add", "README.md")
	treeOID := runGit("", "write-tree")

	pgpSig := "-----BEGIN PGP SIGNATURE-----\n\niQEzBAABCAAdFiEEtest\n-----END PGP SIGNATURE-----"
	pgpCommitRaw := "tree " + treeOID + "\n" +
		"author Alice <alice@example.com> 1700000000 +0000\n" +
		"committer Alice <alice@example.com> 1700000000 +0000\n" +
		"gpgsig -----BEGIN PGP SIGNATURE-----\n" +
		" \n" +
		" iQEzBAABCAAdFiEEtest\n" +
		" -----END PGP SIGNATURE-----\n" +
		"\n" +
		"gpg signed commit\n" +
		"\n" +
		"body line\n"
	pgpExpectedPayload := "tree " + treeOID + "\n" +
		"author Alice <alice@example.com> 1700000000 +0000\n" +
		"committer Alice <alice@example.com> 1700000000 +0000\n" +
		"\n" +
		"gpg signed commit\n" +
		"\n" +
		"body line\n"
	c1 := runGit(pgpCommitRaw, "hash-object", "-t", "commit", "-w", "--stdin")

	sshSig := "-----BEGIN SSH SIGNATURE-----\nU1NIU0lHtest\n-----END SSH SIGNATURE-----"
	sshCommitRaw := "tree " + treeOID + "\n" +
		"parent " + c1 + "\n" +
		"author Bob <bob@example.com> 1700000100 +0000\n" +
		"committer Bob <bob@example.com> 1700000100 +0000\n" +
		"gpgsig -----BEGIN SSH SIGNATURE-----\n" +
		" U1NIU0lHtest\n" +
		" -----END SSH SIGNATURE-----\n" +
		"\n" +
		"ssh signed commit\n"
	sshExpectedPayload := "tree " + treeOID + "\n" +
		"parent " + c1 + "\n" +
		"author Bob <bob@example.com> 1700000100 +0000\n" +
		"committer Bob <bob@example.com> 1700000100 +0000\n" +
		"\n" +
		"ssh signed commit\n"
	c2 := runGit(sshCommitRaw, "hash-object", "-t", "commit", "-w", "--stdin")

	runGit("", "update-ref", "refs/heads/main", c2)
	runGit("", "repack", "-a", "-d", "-q")

	packDir := filepath.Join(workDir, ".git", "objects", "pack")
	packMatches, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil || len(packMatches) != 1 {
		t.Fatalf("expected 1 packfile, got %v (err=%v)", packMatches, err)
	}
	packBytes, err := os.ReadFile(packMatches[0])
	if err != nil {
		t.Fatal(err)
	}
	idxBytes, err := os.ReadFile(strings.TrimSuffix(packMatches[0], ".pack") + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	packSum := sha256.Sum256(packBytes)
	packID := hex.EncodeToString(packSum[:])

	rk, _ := tenant.NewRepoKey("acme", "signed-repo")
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/main", OID: c2}})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01JDF8Z7Q2K9M4N6P8R0T2V4W6", PackID: packID, SizeBytes: int64(len(packBytes)), Status: ddb.PackStatusActive,
	})
	packFetcher := newFakePackFetcher()
	packFetcher.Put(packID, packBytes, idxBytes)

	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{Meta: metaStore, Packs: packFetcher, Cache: nvmeCache, GitExecutor: gitExec})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mat.Close)
	srv, err := NewGitDataServer(GitDataServerConfig{Materializer: mat, GitDataExec: gitExec, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := srv.GetCommitLog(context.Background(), &rpc.GetCommitLogRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
	})
	if err != nil {
		t.Fatalf("GetCommitLog signed: %v", err)
	}
	if len(resp.Commits) != 2 {
		t.Fatalf("commits = %d, want 2", len(resp.Commits))
	}
	if resp.Commits[0].RawSignature != sshSig || resp.Commits[0].SignedPayload != sshExpectedPayload {
		t.Fatalf("SSH commit signature mismatch:\ngot sig=%q\nwant sig=%q\ngot payload=%q\nwant payload=%q",
			resp.Commits[0].RawSignature, sshSig, resp.Commits[0].SignedPayload, sshExpectedPayload)
	}
	if resp.Commits[1].RawSignature != pgpSig || resp.Commits[1].SignedPayload != pgpExpectedPayload || resp.Commits[1].Body != "body line" {
		t.Fatalf("GPG commit signature mismatch:\ngot sig=%q\nwant sig=%q\ngot payload=%q\nwant payload=%q\nbody=%q",
			resp.Commits[1].RawSignature, pgpSig, resp.Commits[1].SignedPayload, pgpExpectedPayload, resp.Commits[1].Body)
	}
}

func TestGitData_EmptyRepositoryGracefulResponses(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "empty-repo")
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta: metaStore, Packs: newFakePackFetcher(), Cache: nvmeCache, GitExecutor: gitExec,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mat.Close)
	srv, err := NewGitDataServer(GitDataServerConfig{Materializer: mat, GitDataExec: gitExec, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}

	refsResp, err := srv.ListRefs(context.Background(), &rpc.ListRefsRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
	})
	if err != nil {
		t.Fatalf("ListRefs empty repo: %v", err)
	}
	if refsResp.DefaultBranch != "refs/heads/main" || refsResp.HeadOID != "" || len(refsResp.Refs) != 0 || refsResp.BranchCount != 0 || refsResp.TagCount != 0 {
		t.Fatalf("unexpected empty repo ListRefs response: %+v", refsResp)
	}

	logResp, err := srv.GetCommitLog(context.Background(), &rpc.GetCommitLogRequest{
		TenantID:               string(rk.TenantID),
		RepoID:                 string(rk.RepoID),
		IncludePathLastCommits: true,
	})
	if err != nil {
		t.Fatalf("GetCommitLog empty repo: %v", err)
	}
	if logResp.TotalCommits != 0 || len(logResp.Commits) != 0 {
		t.Fatalf("unexpected empty repo GetCommitLog response: %+v", logResp)
	}
}

type fakeInitPackStore struct {
	fetcher *fakePackFetcher
}

func (s *fakeInitPackStore) PutPack(_ context.Context, _ tenant.RepoKey, packSHA256 string, packData, idxData []byte) error {
	s.fetcher.Put(packSHA256, packData, idxData)
	return nil
}

type fakeInitCommitter struct {
	metaStore *fakeMetadataStore
}

func (c *fakeInitCommitter) CommitPush(_ context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error) {
	c.metaStore.mu.Lock()
	defer c.metaStore.mu.Unlock()

	existing := make(map[string]string)
	for _, r := range c.metaStore.refs[in.Repo] {
		existing[r.RefName] = r.OID
	}
	for _, u := range in.Updates {
		if u.IsCreate() {
			if _, ok := existing[u.RefName]; ok {
				return ddb.CommitPushResult{}, &ddb.RefConflictError{ConflictingRefs: []string{u.RefName}}
			}
			existing[u.RefName] = u.NewOID
		} else {
			existing[u.RefName] = u.NewOID
		}
	}

	nextRefs := make([]ddb.RefState, 0, len(existing))
	for refName, oid := range existing {
		nextRefs = append(nextRefs, ddb.RefState{
			Repo:         in.Repo,
			RefName:      refName,
			OID:          oid,
			Epoch:        in.Epoch,
			OriginRegion: in.OriginRegion,
		})
	}
	c.metaStore.refs[in.Repo] = nextRefs

	meta := c.metaStore.metas[in.Repo]
	meta.RefVersion++
	if in.Pack != nil {
		meta.ManifestVersion++
		if c.metaStore.manifestsByShard[in.Repo] == nil {
			c.metaStore.manifestsByShard[in.Repo] = make(map[int][]ddb.PackManifest)
		}
		ulid, err := ddb.NewULID(lease.RealClock{}.Now().UTC())
		if err != nil {
			return ddb.CommitPushResult{}, err
		}
		c.metaStore.manifestsByShard[in.Repo][0] = append(c.metaStore.manifestsByShard[in.Repo][0], ddb.PackManifest{
			Repo:         in.Repo,
			ULID:         ulid,
			PackID:       in.Pack.PackID,
			SizeBytes:    in.Pack.SizeBytes,
			OriginRegion: in.Pack.OriginRegion,
			Status:       ddb.PackStatusActive,
		})
	}
	c.metaStore.metas[in.Repo] = meta

	return ddb.CommitPushResult{RefCount: len(in.Updates)}, nil
}

func (c *fakeInitCommitter) CreateMeta(_ context.Context, meta ddb.RepoMeta) error {
	if err := meta.Validate(); err != nil {
		return err
	}
	c.metaStore.mu.Lock()
	defer c.metaStore.mu.Unlock()
	if _, exists := c.metaStore.metas[meta.Repo]; exists {
		return ddb.ErrAlreadyExists
	}
	c.metaStore.metas[meta.Repo] = meta
	return nil
}

func (c *fakeInitCommitter) UpdateMeta(_ context.Context, meta ddb.RepoMeta, expectedEpoch lease.Epoch) error {
	c.metaStore.mu.Lock()
	defer c.metaStore.mu.Unlock()
	cur, ok := c.metaStore.metas[meta.Repo]
	if !ok {
		return ddb.ErrNotFound
	}
	if cur.FailoverEpoch != expectedEpoch {
		return ddb.ErrEpochConflict
	}
	cur.DefaultBranch = meta.DefaultBranch
	c.metaStore.metas[meta.Repo] = cur
	return nil
}

func setupInitRepoTestServer(t *testing.T, repoID string) (tenant.RepoKey, rpc.GitDataClient) {
	t.Helper()
	return setupInitRepoTestServerWithMeta(t, repoID, true)
}

func setupInitRepoTestServerWithMeta(t *testing.T, repoID string, seedMeta bool) (tenant.RepoKey, rpc.GitDataClient) {
	t.Helper()
	rk, err := tenant.NewRepoKey("acme", repoID)
	if err != nil {
		t.Fatal(err)
	}
	metaStore := newFakeMetadataStore()
	if seedMeta {
		metaStore.PutMeta(ddb.RepoMeta{
			Repo:           rk,
			HomeRegion:     "us-east-1",
			FailoverEpoch:  1,
			RefShards:      1,
			ManifestShards: 1,
			WriteState:     lease.ModeActive,
			DefaultBranch:  "refs/heads/main",
		})
	}
	packFetcher := newFakePackFetcher()
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                    metaStore,
		Packs:                   packFetcher,
		Cache:                   nvmeCache,
		GitExecutor:             gitExec,
		AuxIndexRefreshInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mat.Close)

	committer := &fakeInitCommitter{metaStore: metaStore}
	srv, err := NewGitDataServer(GitDataServerConfig{
		Materializer: mat,
		GitDataExec:  gitExec,
		Cache:        nvmeCache,
		Packs:        &fakeInitPackStore{fetcher: packFetcher},
		Committer:    committer,
		MetaUpdater:  committer,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })
	grpcSrv := grpc.NewServer()
	rpc.RegisterGitDataServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return rk, rpc.NewGRPCGitDataClient(cc)
}

func TestGitData_InitRepo_EndToEndAndAlreadyExists(t *testing.T) {
	rk, client := setupInitRepoTestServer(t, "brand-new-repo")
	ctx := context.Background()

	readmeContent := []byte("# Brand New Repo\n\nInitialized via InitRepo.\n")
	licenseContent := []byte("Apache License 2.0\n")

	initResp, err := client.InitRepo(ctx, &rpc.InitRepoRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
		Files: []rpc.InitFile{
			{Path: "README.md", Content: readmeContent},
			{Path: "LICENSE", Content: licenseContent},
		},
	})
	if err != nil {
		t.Fatalf("InitRepo: %v", err)
	}
	if initResp.DefaultBranch != "main" {
		t.Fatalf("DefaultBranch = %q, want %q", initResp.DefaultBranch, "main")
	}
	if !gitexec.IsValidHexOID(initResp.CommitOID) || !gitexec.IsValidHexOID(initResp.TreeOID) {
		t.Fatalf("invalid OIDs in InitRepoResponse: %+v", initResp)
	}

	// ListRefs immediately sees refs/heads/main and HEAD.
	refsResp, err := client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
	})
	if err != nil {
		t.Fatalf("ListRefs after InitRepo: %v", err)
	}
	if refsResp.DefaultBranch != "refs/heads/main" || refsResp.HeadOID != initResp.CommitOID || refsResp.BranchCount != 1 {
		t.Fatalf("unexpected ListRefs response: %+v", refsResp)
	}
	if len(refsResp.Refs) != 1 || refsResp.Refs[0].Name != "refs/heads/main" || refsResp.Refs[0].TargetOID != initResp.CommitOID || !refsResp.Refs[0].IsDefault {
		t.Fatalf("unexpected refs list: %+v", refsResp.Refs)
	}

	// ListTree immediately sees README.md and LICENSE.
	treeResp, err := client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(rk.TenantID),
		RepoID:    string(rk.RepoID),
		CommitOID: initResp.CommitOID,
	})
	if err != nil {
		t.Fatalf("ListTree after InitRepo: %v", err)
	}
	if treeResp.TreeOID != initResp.TreeOID || len(treeResp.Entries) != 2 {
		t.Fatalf("unexpected ListTree response: %+v", treeResp)
	}
	var readmeOID string
	for _, entry := range treeResp.Entries {
		if entry.Path == "README.md" {
			readmeOID = entry.OID
		}
	}
	if readmeOID == "" {
		t.Fatalf("README.md missing from ListTree: %+v", treeResp.Entries)
	}

	// GetCommitLog immediately reads back the initial commit via HEAD.
	logResp, err := client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
		Ref:      "HEAD",
	})
	if err != nil {
		t.Fatalf("GetCommitLog after InitRepo: %v", err)
	}
	if logResp.TotalCommits != 1 || len(logResp.Commits) != 1 {
		t.Fatalf("unexpected GetCommitLog response: %+v", logResp)
	}
	c := logResp.Commits[0]
	if c.OID != initResp.CommitOID || c.TreeOID != initResp.TreeOID || c.AuthorName != "Stravin" || c.AuthorEmail != "noreply@stravin.dev" || c.Subject != "Initial commit" {
		t.Fatalf("unexpected initial commit metadata: %+v", c)
	}

	// GetBlob immediately streams back the exact README.md bytes.
	blobStream, err := client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
		BlobOID:  readmeOID,
	})
	if err != nil {
		t.Fatalf("GetBlob after InitRepo: %v", err)
	}
	var blobBuf bytes.Buffer
	for {
		chunk, recvErr := blobStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("GetBlob Recv: %v", recvErr)
		}
		blobBuf.Write(chunk.Data)
		if chunk.EOF {
			break
		}
	}
	if !bytes.Equal(blobBuf.Bytes(), readmeContent) {
		t.Fatalf("README.md blob = %q, want %q", blobBuf.String(), string(readmeContent))
	}

	// Calling InitRepo a second time must return AlreadyExists and never overwrite.
	_, err = client.InitRepo(ctx, &rpc.InitRepoRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
		Files: []rpc.InitFile{
			{Path: "README.md", Content: []byte("overwrite attempt")},
		},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second InitRepo code = %v (%v), want AlreadyExists", status.Code(err), err)
	}
}

func TestGitData_InitRepo_CustomDefaultBranch(t *testing.T) {
	rk, client := setupInitRepoTestServer(t, "custom-branch-repo")
	ctx := context.Background()

	initResp, err := client.InitRepo(ctx, &rpc.InitRepoRequest{
		TenantID:      string(rk.TenantID),
		RepoID:        string(rk.RepoID),
		DefaultBranch: "develop",
		AuthorName:    "Alice",
		AuthorEmail:   "alice@example.com",
		CommitMessage: "Bootstrap repo",
		Files: []rpc.InitFile{
			{Path: "docs/README.md", Content: []byte("# Develop\n")},
		},
	})
	if err != nil {
		t.Fatalf("InitRepo custom branch: %v", err)
	}
	if initResp.DefaultBranch != "develop" {
		t.Fatalf("DefaultBranch = %q, want develop", initResp.DefaultBranch)
	}

	refsResp, err := client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
	})
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if refsResp.DefaultBranch != "refs/heads/develop" || refsResp.HeadOID != initResp.CommitOID {
		t.Fatalf("unexpected ListRefs for custom branch: %+v", refsResp)
	}
}

func TestGitData_InitRepo_ValidationAndIsolation(t *testing.T) {
	rk, client := setupInitRepoTestServer(t, "validation-repo")
	ctx := context.Background()

	tooManyFiles := make([]rpc.InitFile, maxInitRepoFiles+1)
	for i := range tooManyFiles {
		tooManyFiles[i] = rpc.InitFile{Path: "f" + string(rune('a'+i%26)) + "/" + string(rune('a'+i/26)) + ".txt", Content: []byte("x")}
	}

	invalidReqs := []struct {
		name string
		req  *rpc.InitRepoRequest
	}{
		{
			name: "empty files",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: nil},
		},
		{
			name: "parent traversal path",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: []rpc.InitFile{{Path: "../escape", Content: []byte("x")}}},
		},
		{
			name: "dotgit segment",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: []rpc.InitFile{{Path: ".git/config", Content: []byte("x")}}},
		},
		{
			name: "absolute path",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: []rpc.InitFile{{Path: "/README.md", Content: []byte("x")}}},
		},
		{
			name: "duplicate path",
			req: &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: []rpc.InitFile{
				{Path: "README.md", Content: []byte("a")},
				{Path: "README.md", Content: []byte("b")},
			}},
		},
		{
			name: "file directory collision",
			req: &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: []rpc.InitFile{
				{Path: "docs", Content: []byte("a")},
				{Path: "docs/README.md", Content: []byte("b")},
			}},
		},
		{
			name: "too many files",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: tooManyFiles},
		},
		{
			name: "exceeds max total bytes",
			req: &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), Files: []rpc.InitFile{
				{Path: "big.bin", Content: make([]byte, maxInitRepoTotalBytes+1)},
			}},
		},
		{
			name: "invalid branch leading dash",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), DefaultBranch: "-main", Files: []rpc.InitFile{{Path: "README.md", Content: []byte("x")}}},
		},
		{
			name: "invalid branch double dot",
			req:  &rpc.InitRepoRequest{TenantID: string(rk.TenantID), RepoID: string(rk.RepoID), DefaultBranch: "a..b", Files: []rpc.InitFile{{Path: "README.md", Content: []byte("x")}}},
		},
	}

	for _, tc := range invalidReqs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.InitRepo(ctx, tc.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%s: code = %v (%v), want InvalidArgument", tc.name, status.Code(err), err)
			}
		})
	}

	// Mismatched tenant/repo metadata is rejected with PermissionDenied.
	badCtx := metadata.AppendToOutgoingContext(ctx,
		rpc.HeaderTenantID, "other-tenant",
		rpc.HeaderRepoID, string(rk.RepoID),
	)
	_, err := client.InitRepo(badCtx, &rpc.InitRepoRequest{
		TenantID: string(rk.TenantID),
		RepoID:   string(rk.RepoID),
		Files:    []rpc.InitFile{{Path: "README.md", Content: []byte("x")}},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("tenant mismatch code = %v (%v), want PermissionDenied", status.Code(err), err)
	}
}

func TestGitData_InitRepo_BootstrapsMissingRepoMeta(t *testing.T) {
	ctx := context.Background()

	t.Run("default branch without pre-existing meta", func(t *testing.T) {
		rk, client := setupInitRepoTestServerWithMeta(t, "unseeded-repo", false)
		readmeContent := []byte("# Unseeded Repo\n\nBootstrapped via InitRepo.\n")

		initResp, err := client.InitRepo(ctx, &rpc.InitRepoRequest{
			TenantID: string(rk.TenantID),
			RepoID:   string(rk.RepoID),
			Files: []rpc.InitFile{
				{Path: "README.md", Content: readmeContent},
			},
		})
		if err != nil {
			t.Fatalf("InitRepo without pre-existing RepoMeta: %v", err)
		}
		if initResp.DefaultBranch != "main" {
			t.Fatalf("DefaultBranch = %q, want %q", initResp.DefaultBranch, "main")
		}
		if !gitexec.IsValidHexOID(initResp.CommitOID) || !gitexec.IsValidHexOID(initResp.TreeOID) {
			t.Fatalf("invalid OIDs in InitRepoResponse: %+v", initResp)
		}

		// ListRefs immediately sees refs/heads/main and HEAD.
		refsResp, err := client.ListRefs(ctx, &rpc.ListRefsRequest{
			TenantID: string(rk.TenantID),
			RepoID:   string(rk.RepoID),
		})
		if err != nil {
			t.Fatalf("ListRefs after bootstrap InitRepo: %v", err)
		}
		if refsResp.DefaultBranch != "refs/heads/main" || refsResp.HeadOID != initResp.CommitOID || refsResp.BranchCount != 1 {
			t.Fatalf("unexpected ListRefs response: %+v", refsResp)
		}
		if len(refsResp.Refs) != 1 || refsResp.Refs[0].Name != "refs/heads/main" || !refsResp.Refs[0].IsDefault {
			t.Fatalf("unexpected refs list: %+v", refsResp.Refs)
		}

		// ListTree immediately sees README.md.
		treeResp, err := client.ListTree(ctx, &rpc.ListTreeRequest{
			TenantID:  string(rk.TenantID),
			RepoID:    string(rk.RepoID),
			CommitOID: initResp.CommitOID,
		})
		if err != nil {
			t.Fatalf("ListTree after bootstrap InitRepo: %v", err)
		}
		if treeResp.TreeOID != initResp.TreeOID || len(treeResp.Entries) != 1 || treeResp.Entries[0].Path != "README.md" {
			t.Fatalf("unexpected ListTree response: %+v", treeResp)
		}
		readmeOID := treeResp.Entries[0].OID

		// GetCommitLog immediately reads back the initial commit via HEAD.
		logResp, err := client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
			TenantID: string(rk.TenantID),
			RepoID:   string(rk.RepoID),
			Ref:      "HEAD",
		})
		if err != nil {
			t.Fatalf("GetCommitLog after bootstrap InitRepo: %v", err)
		}
		if logResp.TotalCommits != 1 || len(logResp.Commits) != 1 || logResp.Commits[0].OID != initResp.CommitOID {
			t.Fatalf("unexpected GetCommitLog response: %+v", logResp)
		}

		// GetBlob immediately streams back README.md bytes.
		blobStream, err := client.GetBlob(ctx, &rpc.GetBlobRequest{
			TenantID: string(rk.TenantID),
			RepoID:   string(rk.RepoID),
			BlobOID:  readmeOID,
		})
		if err != nil {
			t.Fatalf("GetBlob after bootstrap InitRepo: %v", err)
		}
		var blobBuf bytes.Buffer
		for {
			chunk, recvErr := blobStream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				t.Fatalf("GetBlob Recv: %v", recvErr)
			}
			blobBuf.Write(chunk.Data)
			if chunk.EOF {
				break
			}
		}
		if !bytes.Equal(blobBuf.Bytes(), readmeContent) {
			t.Fatalf("README.md blob = %q, want %q", blobBuf.String(), string(readmeContent))
		}

		// A second InitRepo call must return AlreadyExists.
		_, err = client.InitRepo(ctx, &rpc.InitRepoRequest{
			TenantID: string(rk.TenantID),
			RepoID:   string(rk.RepoID),
			Files:    []rpc.InitFile{{Path: "README.md", Content: []byte("second attempt")}},
		})
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("second InitRepo code = %v (%v), want AlreadyExists", status.Code(err), err)
		}
	})

	t.Run("custom default branch without pre-existing meta", func(t *testing.T) {
		rk, client := setupInitRepoTestServerWithMeta(t, "unseeded-custom-branch", false)
		initResp, err := client.InitRepo(ctx, &rpc.InitRepoRequest{
			TenantID:      string(rk.TenantID),
			RepoID:        string(rk.RepoID),
			DefaultBranch: "develop",
			Files:         []rpc.InitFile{{Path: "README.md", Content: []byte("# Develop\n")}},
		})
		if err != nil {
			t.Fatalf("InitRepo custom branch without pre-existing RepoMeta: %v", err)
		}
		if initResp.DefaultBranch != "develop" {
			t.Fatalf("DefaultBranch = %q, want %q", initResp.DefaultBranch, "develop")
		}

		refsResp, err := client.ListRefs(ctx, &rpc.ListRefsRequest{
			TenantID: string(rk.TenantID),
			RepoID:   string(rk.RepoID),
		})
		if err != nil {
			t.Fatalf("ListRefs: %v", err)
		}
		if refsResp.DefaultBranch != "refs/heads/develop" || refsResp.HeadOID != initResp.CommitOID {
			t.Fatalf("unexpected ListRefs for custom branch: %+v", refsResp)
		}
	})

	t.Run("concurrent InitRepo on missing meta", func(t *testing.T) {
		rk, client := setupInitRepoTestServerWithMeta(t, "unseeded-concurrent", false)
		const callers = 4
		errs := make([]error, callers)
		var wg sync.WaitGroup
		wg.Add(callers)
		for i := range callers {
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = client.InitRepo(ctx, &rpc.InitRepoRequest{
					TenantID: string(rk.TenantID),
					RepoID:   string(rk.RepoID),
					Files:    []rpc.InitFile{{Path: "README.md", Content: []byte("# Concurrent\n")}},
				})
			}(i)
		}
		wg.Wait()

		var okCount, alreadyExistsCount int
		for _, err := range errs {
			switch {
			case err == nil:
				okCount++
			case status.Code(err) == codes.AlreadyExists:
				alreadyExistsCount++
			default:
				t.Fatalf("unexpected concurrent InitRepo error: %v", err)
			}
		}
		if okCount != 1 || alreadyExistsCount != callers-1 {
			t.Fatalf("concurrent InitRepo results: ok=%d alreadyExists=%d, want 1 / %d", okCount, alreadyExistsCount, callers-1)
		}
	})
}

func TestGitData_TraceparentPropagation(t *testing.T) {
	fx := setupGitDataFixture(t)

	const (
		wantTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		wantSpanID  = "00f067aa0ba902b7"
		traceparent = "00-" + wantTraceID + "-" + wantSpanID + "-01"
	)
	ctx := metadata.AppendToOutgoingContext(context.Background(), rpc.HeaderTraceParent, traceparent)

	if _, err := fx.client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		HeadOID:  fx.secondCommit,
	}); err != nil {
		t.Fatalf("GetDiff: %v", err)
	}

	blobStream, err := fx.client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		BlobOID:  fx.pngBlobOID,
	})
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	for {
		_, recvErr := blobStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("GetBlob Recv: %v", recvErr)
		}
	}

	if _, err := fx.client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID:  string(fx.repoKey.TenantID),
		RepoID:    string(fx.repoKey.RepoID),
		CommitOID: fx.secondCommit,
	}); err != nil {
		t.Fatalf("ListTree: %v", err)
	}

	if _, err := fx.client.ListRefs(ctx, &rpc.ListRefsRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
	}); err != nil {
		t.Fatalf("ListRefs: %v", err)
	}

	if _, err := fx.client.GetCommitLog(ctx, &rpc.GetCommitLogRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Ref:      "main",
	}); err != nil {
		t.Fatalf("GetCommitLog: %v", err)
	}

	// InitRepo extracts traceparent and starts node.gitdata.init_repo before checking persistence/state.
	_, _ = fx.client.InitRepo(ctx, &rpc.InitRepoRequest{
		TenantID: string(fx.repoKey.TenantID),
		RepoID:   string(fx.repoKey.RepoID),
		Files:    []rpc.InitFile{{Path: "README.md", Content: []byte("traceparent test")}},
	})

	// Await full server handler completion before inspecting recorded spans.
	fx.grpcSrv.GracefulStop()

	wantSpans := map[string]bool{
		"node.gitdata.get_diff":       false,
		"node.gitdata.get_blob":       false,
		"node.gitdata.list_tree":      false,
		"node.gitdata.list_refs":      false,
		"node.gitdata.get_commit_log": false,
		"node.gitdata.init_repo":      false,
	}
	for _, sp := range fx.tp.SpanExporter.GetSpans() {
		if _, tracked := wantSpans[sp.Name]; !tracked {
			continue
		}
		if got := sp.SpanContext.TraceID().String(); got != wantTraceID {
			t.Errorf("span %q trace_id = %q, want %q", sp.Name, got, wantTraceID)
		}
		if got := sp.Parent.SpanID().String(); got != wantSpanID {
			t.Errorf("span %q parent_span_id = %q, want %q", sp.Name, got, wantSpanID)
		}
		wantSpans[sp.Name] = true
	}
	for name, seen := range wantSpans {
		if !seen {
			t.Errorf("expected span %q to be recorded", name)
		}
	}
}
