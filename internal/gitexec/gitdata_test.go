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
