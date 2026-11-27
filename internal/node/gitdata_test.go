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
