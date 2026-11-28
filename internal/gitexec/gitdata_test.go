package gitexec

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"stravin.dev/core/internal/tenant"
)

func initTestBareRepoWithCommits(t *testing.T) (string, string, string, string) {
	t.Helper()
	workDir := t.TempDir()
	bareDir := filepath.Join(t.TempDir(), "repo.git")

	runGit := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
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

	runGit(workDir, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(workDir, "hello.txt"), []byte("line1\nline2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(workDir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "sub", "data.bin"), bytes.Repeat([]byte{0x00, 0x01, 0x02, 0x03}, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(workDir, "add", ".")
	runGit(workDir, "commit", "-m", "initial")
	c1 := runGit(workDir, "rev-parse", "HEAD")

	if err := os.WriteFile(filepath.Join(workDir, "hello.txt"), []byte("line1\nline2-mod\nline3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(workDir, "add", "hello.txt")
	runGit(workDir, "commit", "-m", "second")
	c2 := runGit(workDir, "rev-parse", "HEAD")
	blobOID := runGit(workDir, "rev-parse", "HEAD:sub/data.bin")

	runGit(workDir, "clone", "--bare", workDir, bareDir)
	return bareDir, c1, c2, blobOID
}

func TestSubprocessExecutor_GitDataOperations(t *testing.T) {
	bareDir, c1, c2, blobOID := initTestBareRepoWithCommits(t)
	e := NewSubprocessExecutor(Config{})
	rk, _ := tenant.NewRepoKey("acme", "gitdata-test")
	ctx := context.Background()

	gotC2, err := e.ExecRevParse(ctx, RevParseRequest{Repo: rk, RepoDir: bareDir, Rev: c2 + "^{commit}"})
	if err != nil || gotC2 != c2 {
		t.Fatalf("ExecRevParse = (%q, %v), want (%q, nil)", gotC2, err, c2)
	}

	mb, err := e.ExecMergeBase(ctx, MergeBaseRequest{Repo: rk, RepoDir: bareDir, BaseOID: c1, HeadOID: c2})
	if err != nil || mb != c1 {
		t.Fatalf("ExecMergeBase = (%q, %v), want (%q, nil)", mb, err, c1)
	}

	var rawBuf bytes.Buffer
	if err := e.ExecDiffTreeRaw(ctx, DiffTreeRawRequest{Repo: rk, RepoDir: bareDir, BaseOID: c1, HeadOID: c2, Stdout: &rawBuf}); err != nil {
		t.Fatalf("ExecDiffTreeRaw: %v", err)
	}
	if !strings.Contains(rawBuf.String(), "hello.txt") {
		t.Fatalf("ExecDiffTreeRaw output missing hello.txt: %q", rawBuf.String())
	}

	var patchBuf bytes.Buffer
	if err := e.ExecDiffTreePatch(ctx, DiffTreePatchRequest{
		Repo: rk, RepoDir: bareDir, BaseOID: c1, HeadOID: c2, ContextLines: 3, Paths: []string{"hello.txt"}, Stdout: &patchBuf,
	}); err != nil {
		t.Fatalf("ExecDiffTreePatch: %v", err)
	}
	if !strings.Contains(patchBuf.String(), "+line2-mod") {
		t.Fatalf("ExecDiffTreePatch output missing +line2-mod: %q", patchBuf.String())
	}

	metas, err := e.ExecCatFileBatchCheck(ctx, CatFileBatchCheckRequest{
		Repo: rk, RepoDir: bareDir, OIDs: []string{blobOID, strings.Repeat("f", 40)},
	})
	if err != nil {
		t.Fatalf("ExecCatFileBatchCheck: %v", err)
	}
	if !metas[blobOID].Exists || metas[blobOID].Type != "blob" || metas[blobOID].Size != 16384 {
		t.Fatalf("blob meta = %+v, want exists blob 16384", metas[blobOID])
	}
	if metas[strings.Repeat("f", 40)].Exists {
		t.Fatalf("missing OID unexpectedly marked exists")
	}

	headBytes, err := e.ExecCatFileBlobHead(ctx, CatFileBlobHeadRequest{
		Repo: rk, RepoDir: bareDir, BlobOID: blobOID, MaxBytes: 64,
	})
	if err != nil {
		t.Fatalf("ExecCatFileBlobHead: %v", err)
	}
	if len(headBytes) != 64 {
		t.Fatalf("ExecCatFileBlobHead len = %d, want 64", len(headBytes))
	}

	var blobBuf bytes.Buffer
	if err := e.ExecCatFileBlob(ctx, CatFileBlobRequest{Repo: rk, RepoDir: bareDir, BlobOID: blobOID, Stdout: &blobBuf}); err != nil {
		t.Fatalf("ExecCatFileBlob: %v", err)
	}
	if blobBuf.Len() != 16384 {
		t.Fatalf("ExecCatFileBlob len = %d, want 16384", blobBuf.Len())
	}

	treeOID, err := e.ExecRevParse(ctx, RevParseRequest{Repo: rk, RepoDir: bareDir, Rev: c2 + "^{tree}"})
	if err != nil {
		t.Fatalf("ExecRevParse tree: %v", err)
	}
	var lsBuf bytes.Buffer
	if err := e.ExecLsTree(ctx, LsTreeRequest{Repo: rk, RepoDir: bareDir, TreeOID: treeOID, Stdout: &lsBuf}); err != nil {
		t.Fatalf("ExecLsTree: %v", err)
	}
	if !strings.Contains(lsBuf.String(), "hello.txt") || !strings.Contains(lsBuf.String(), "sub") {
		t.Fatalf("ExecLsTree output missing entries: %q", lsBuf.String())
	}

	var logBuf bytes.Buffer
	if err := e.ExecLog(ctx, LogRequest{
		Repo: rk, RepoDir: bareDir, CommitOID: c2, MaxCount: 5, Format: "%H%x00%s", Stdout: &logBuf,
	}); err != nil {
		t.Fatalf("ExecLog: %v", err)
	}
	if !strings.Contains(logBuf.String(), c2) || !strings.Contains(logBuf.String(), "second") || !strings.Contains(logBuf.String(), c1) {
		t.Fatalf("ExecLog output unexpected: %q", logBuf.String())
	}

	var pathLogBuf bytes.Buffer
	if err := e.ExecLog(ctx, LogRequest{
		Repo: rk, RepoDir: bareDir, CommitOID: c2, Path: "sub/data.bin", MaxCount: 5, Format: "%H%x00%s", Stdout: &pathLogBuf,
	}); err != nil {
		t.Fatalf("ExecLog path: %v", err)
	}
	if strings.Contains(pathLogBuf.String(), c2) || !strings.Contains(pathLogBuf.String(), c1) {
		t.Fatalf("ExecLog path output unexpected: %q", pathLogBuf.String())
	}

	var nameOnlyBuf bytes.Buffer
	if err := e.ExecLog(ctx, LogRequest{
		Repo: rk, RepoDir: bareDir, CommitOID: c2, MaxCount: 5, Format: "%x1e%H%x00%s%x00", NameOnly: true, Stdout: &nameOnlyBuf,
	}); err != nil {
		t.Fatalf("ExecLog name-only: %v", err)
	}
	if !strings.Contains(nameOnlyBuf.String(), "hello.txt") || !strings.Contains(nameOnlyBuf.String(), "sub/data.bin") {
		t.Fatalf("ExecLog name-only output unexpected: %q", nameOnlyBuf.String())
	}

	totalCount, err := e.ExecRevListCount(ctx, RevListCountRequest{Repo: rk, RepoDir: bareDir, CommitOID: c2})
	if err != nil || totalCount != 2 {
		t.Fatalf("ExecRevListCount = (%d, %v), want (2, nil)", totalCount, err)
	}
	cappedCount, err := e.ExecRevListCount(ctx, RevListCountRequest{Repo: rk, RepoDir: bareDir, CommitOID: c2, MaxCount: 1})
	if err != nil || cappedCount != 1 {
		t.Fatalf("ExecRevListCount capped = (%d, %v), want (1, nil)", cappedCount, err)
	}
	pathCount, err := e.ExecRevListCount(ctx, RevListCountRequest{Repo: rk, RepoDir: bareDir, CommitOID: c2, Path: "sub/data.bin"})
	if err != nil || pathCount != 1 {
		t.Fatalf("ExecRevListCount path = (%d, %v), want (1, nil)", pathCount, err)
	}

	batchObjs, err := e.ExecCatFileBatch(ctx, CatFileBatchRequest{
		Repo: rk, RepoDir: bareDir, OIDs: []string{c2, c1, c2, strings.Repeat("f", 40)},
	})
	if err != nil {
		t.Fatalf("ExecCatFileBatch: %v", err)
	}
	if len(batchObjs) != 2 || !bytes.Contains(batchObjs[c2], []byte("second")) || !bytes.Contains(batchObjs[c1], []byte("initial")) {
		t.Fatalf("ExecCatFileBatch unexpected result keys=%d", len(batchObjs))
	}

	// Oversized object is skipped cleanly without failing the batch.
	smallBatch, err := e.ExecCatFileBatch(ctx, CatFileBatchRequest{
		Repo: rk, RepoDir: bareDir, OIDs: []string{blobOID, c2}, MaxObjectBytes: 1024,
	})
	if err != nil {
		t.Fatalf("ExecCatFileBatch max bytes: %v", err)
	}
	if _, hasBlob := smallBatch[blobOID]; hasBlob {
		t.Fatal("expected oversized blob to be skipped")
	}
	if _, hasCommit := smallBatch[c2]; !hasCommit {
		t.Fatal("expected small commit to be present")
	}

	// Invalid arguments rejected before spawning git.
	if err := e.ExecLog(ctx, LogRequest{Repo: rk, RepoDir: bareDir, CommitOID: "--evil", MaxCount: 1, Stdout: &logBuf}); err == nil {
		t.Fatal("expected ExecLog to reject flag-like commit OID")
	}
	if err := e.ExecLog(ctx, LogRequest{Repo: rk, RepoDir: bareDir, CommitOID: c2, MaxCount: 0, Stdout: &logBuf}); err == nil {
		t.Fatal("expected ExecLog to reject zero MaxCount")
	}
	if _, err := e.ExecRevListCount(ctx, RevListCountRequest{Repo: rk, RepoDir: bareDir, CommitOID: "not-hex"}); err == nil {
		t.Fatal("expected ExecRevListCount to reject non-hex OID")
	}
	if _, err := e.ExecCatFileBatch(ctx, CatFileBatchRequest{Repo: rk, RepoDir: bareDir, OIDs: []string{"--flag"}}); err == nil {
		t.Fatal("expected ExecCatFileBatch to reject invalid OID")
	}
}

func TestIsValidHexOID(t *testing.T) {
	if !IsValidHexOID(strings.Repeat("a", 40)) {
		t.Fatal("expected valid 40-char sha1 OID")
	}
	if !IsValidHexOID(strings.Repeat("0", 64)) {
		t.Fatal("expected valid 64-char sha256 OID")
	}
	for _, bad := range []string{"", "HEAD", "--flag", strings.Repeat("A", 40), strings.Repeat("g", 40), strings.Repeat("a", 39)} {
		if IsValidHexOID(bad) {
			t.Errorf("IsValidHexOID(%q) = true, want false", bad)
		}
	}
}

func TestSubprocessExecutor_ExecInitRepoCommit(t *testing.T) {
	e := NewSubprocessExecutor(Config{})
	rk, _ := tenant.NewRepoKey("acme", "init-test")
	ctx := context.Background()

	res, err := e.ExecInitRepoCommit(ctx, InitRepoCommitRequest{
		Repo:          rk,
		ObjectFormat:  "sha1",
		DefaultBranch: "main",
		AuthorName:    "Stravin",
		AuthorEmail:   "noreply@stravin.dev",
		CommitMessage: "Initial commit",
		Files: []InitRepoFile{
			{Path: "README.md", Content: []byte("# Hello Stravin\n")},
			{Path: "docs/LICENSE", Content: []byte("Apache-2.0\n")},
		},
	})
	if err != nil {
		t.Fatalf("ExecInitRepoCommit: %v", err)
	}
	if !IsValidHexOID(res.CommitOID) || len(res.CommitOID) != 40 {
		t.Fatalf("CommitOID = %q, want 40-char sha1", res.CommitOID)
	}
	if !IsValidHexOID(res.TreeOID) || len(res.TreeOID) != 40 {
		t.Fatalf("TreeOID = %q, want 40-char sha1", res.TreeOID)
	}
	if !bytes.HasPrefix(res.PackBytes, []byte("PACK")) {
		t.Fatalf("PackBytes missing PACK header (len=%d)", len(res.PackBytes))
	}

	// Verify the generated packfile unpacks cleanly into a fresh bare repo and contains both files.
	verifyDir := filepath.Join(t.TempDir(), "verify.git")
	if err := e.ExecInitBare(ctx, InitBareRequest{Repo: rk, RepoDir: verifyDir, ObjectFormat: "sha1", RefFormat: "files", DefaultBranch: "main"}); err != nil {
		t.Fatalf("ExecInitBare: %v", err)
	}
	if err := e.ExecIndexPack(ctx, IndexPackRequest{
		Repo:           rk,
		RepoDir:        verifyDir,
		FromStdin:      true,
		OutputPackPath: filepath.Join(verifyDir, "objects", "pack", "init.pack"),
		Stdin:          bytes.NewReader(res.PackBytes),
	}); err != nil {
		t.Fatalf("ExecIndexPack: %v", err)
	}

	gotTree, err := e.ExecRevParse(ctx, RevParseRequest{Repo: rk, RepoDir: verifyDir, Rev: res.CommitOID + "^{tree}"})
	if err != nil || gotTree != res.TreeOID {
		t.Fatalf("rev-parse tree = (%q, %v), want (%q, nil)", gotTree, err, res.TreeOID)
	}
	readmeOID, err := e.ExecRevParse(ctx, RevParseRequest{Repo: rk, RepoDir: verifyDir, Rev: res.CommitOID + ":README.md"})
	if err != nil {
		t.Fatalf("rev-parse README.md: %v", err)
	}
	var blobBuf bytes.Buffer
	if err := e.ExecCatFileBlob(ctx, CatFileBlobRequest{Repo: rk, RepoDir: verifyDir, BlobOID: readmeOID, Stdout: &blobBuf}); err != nil {
		t.Fatalf("ExecCatFileBlob: %v", err)
	}
	if blobBuf.String() != "# Hello Stravin\n" {
		t.Fatalf("README.md = %q, want %q", blobBuf.String(), "# Hello Stravin\n")
	}

	// Reject invalid inputs before spawning git.
	invalidCases := []InitRepoCommitRequest{
		{Repo: rk, DefaultBranch: "-bad", AuthorName: "A", AuthorEmail: "a@b", CommitMessage: "m", Files: []InitRepoFile{{Path: "README.md", Content: []byte("x")}}},
		{Repo: rk, DefaultBranch: "main", AuthorName: "A", AuthorEmail: "a@b", CommitMessage: "m", Files: nil},
		{Repo: rk, DefaultBranch: "main", AuthorName: "A", AuthorEmail: "a@b", CommitMessage: "m", Files: []InitRepoFile{{Path: "../escape", Content: []byte("x")}}},
		{Repo: rk, DefaultBranch: "main", AuthorName: "A", AuthorEmail: "a@b", CommitMessage: "m", Files: []InitRepoFile{{Path: ".git/config", Content: []byte("x")}}},
		{Repo: rk, DefaultBranch: "main", AuthorName: "A", AuthorEmail: "a@b", CommitMessage: "m", Files: []InitRepoFile{{Path: "/abs.txt", Content: []byte("x")}}},
	}
	for i, tc := range invalidCases {
		if _, err := e.ExecInitRepoCommit(ctx, tc); err == nil {
			t.Errorf("invalid case %d unexpectedly succeeded", i)
		}
	}
}
