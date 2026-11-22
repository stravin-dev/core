package pack

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompleteThinPack(t *testing.T) {
	base := []byte("base content that lives in an existing repository pack\n")
	baseOID := ComputeOID(ObjBlob, base)
	delta := buildDeltaCopyInsert(len(base), len(base), []byte("appended line\n"))
	derived := append(append([]byte(nil), base...), []byte("appended line\n")...)
	derivedOID := ComputeOID(ObjBlob, derived)

	thinPack := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjRefDelta, refBase: baseOID, data: delta},
	})

	// Parsing thin pack without a BaseResolver must fail with ErrMissingBase.
	if _, err := Parse(bytes.NewReader(thinPack), ParseOptions{}); !errors.Is(err, ErrMissingBase) {
		t.Fatalf("Parse thin without resolver: got %v, want ErrMissingBase", err)
	}

	resolver := MapBaseResolver{
		baseOID: {OID: baseOID, Type: ObjBlob, Data: base},
	}
	completer := NewPureGoThinCompleter(DefaultLimits())
	completed, err := completer.Complete(context.Background(), thinPack, resolver)
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	// Completed pack must be completely self-contained (BaseResolver == nil).
	parsed, err := Parse(bytes.NewReader(completed), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse completed pack failed: %v", err)
	}
	if len(parsed.Entries) != 2 {
		t.Fatalf("got %d entries, want 2 (delta + appended base)", len(parsed.Entries))
	}
	if got := parsed.Objects[derivedOID].Data; !bytes.Equal(got, derived) {
		t.Fatalf("derived mismatch: got %q, want %q", got, derived)
	}
	if got := parsed.Objects[baseOID].Data; !bytes.Equal(got, base) {
		t.Fatalf("base mismatch: got %q, want %q", got, base)
	}

	// Cross-check completed pack + generated .idx with real git verify-pack when available.
	if _, err := exec.LookPath("git"); err == nil {
		idxBytes, err := BuildIdx(parsed)
		if err != nil {
			t.Fatalf("BuildIdx failed: %v", err)
		}
		dir := t.TempDir()
		packPath := filepath.Join(dir, "pack-test.pack")
		idxPath := filepath.Join(dir, "pack-test.idx")
		if err := os.WriteFile(packPath, completed, 0o600); err != nil {
			t.Fatalf("write pack: %v", err)
		}
		if err := os.WriteFile(idxPath, idxBytes, 0o600); err != nil {
			t.Fatalf("write idx: %v", err)
		}
		cmd := exec.Command("git", "verify-pack", "-v", idxPath)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git verify-pack failed: %v\n%s", err, string(out))
		}
	}
}

func TestCompleteThinPackNonThinPassthrough(t *testing.T) {
	blob := []byte("standalone blob")
	packBytes := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjBlob, data: blob},
	})

	completed, err := CompleteThinPack(packBytes, nil, DefaultLimits())
	if err != nil {
		t.Fatalf("CompleteThinPack on non-thin pack failed: %v", err)
	}
	if !bytes.Equal(completed, packBytes) {
		t.Fatal("non-thin pack should be returned unchanged")
	}
}

func TestCompleteThinPackErrors(t *testing.T) {
	base := []byte("missing base")
	baseOID := ComputeOID(ObjBlob, base)
	delta := buildDeltaCopyInsert(len(base), len(base), []byte("!"))
	thinPack := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjRefDelta, refBase: baseOID, data: delta},
	})

	t.Run("nil resolver", func(t *testing.T) {
		_, err := CompleteThinPack(thinPack, nil, DefaultLimits())
		if !errors.Is(err, ErrMissingBase) {
			t.Fatalf("got %v, want ErrMissingBase", err)
		}
	})

	t.Run("corrupt resolved base checksum", func(t *testing.T) {
		badResolver := MapBaseResolver{
			baseOID: {OID: baseOID, Type: ObjBlob, Data: []byte("wrong bytes")},
		}
		_, err := CompleteThinPack(thinPack, badResolver, DefaultLimits())
		if !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("got %v, want ErrChecksumMismatch", err)
		}
	})
}

func TestCompleteThinPackRealGitThinPack(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	repoDir := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repoDir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Stravin Test",
			"GIT_AUTHOR_EMAIL=test@stravin.dev",
			"GIT_COMMITTER_NAME=Stravin Test",
			"GIT_COMMITTER_EMAIL=test@stravin.dev",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	runGit("init", "-q")
	largeContent := strings.Repeat("stravin core packfile engine line\n", 200)
	filePath := filepath.Join(repoDir, "big.txt")
	if err := os.WriteFile(filePath, []byte(largeContent), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	runGit("add", "big.txt")
	runGit("commit", "-q", "-m", "commit 1")
	c1 := runGit("rev-parse", "HEAD")

	// Append a small line so git pack-objects --thin produces a REF_DELTA referencing commit 1's blob.
	if err := os.WriteFile(filePath, []byte(largeContent+"incremental push change\n"), 0o600); err != nil {
		t.Fatalf("write file v2: %v", err)
	}
	runGit("add", "big.txt")
	runGit("commit", "-q", "-m", "commit 2")
	c2 := runGit("rev-parse", "HEAD")

	// Generate a real thin pack for c2 excluding c1.
	packCmd := exec.Command("git", "-C", repoDir, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset")
	packCmd.Stdin = strings.NewReader(c2 + "\n^" + c1 + "\n")
	thinPack, err := packCmd.Output()
	if err != nil {
		t.Fatalf("git pack-objects --thin: %v", err)
	}

	// Build a BaseResolver backed by `git cat-file` against the repo.
	gitResolver := FuncBaseResolver(func(oid OID) (ObjectType, []byte, error) {
		typStr := runGit("cat-file", "-t", oid.String())
		var typ ObjectType
		switch typStr {
		case "commit":
			typ = ObjCommit
		case "tree":
			typ = ObjTree
		case "blob":
			typ = ObjBlob
		case "tag":
			typ = ObjTag
		default:
			return 0, nil, ErrInvalidObjectType
		}
		cmd := exec.Command("git", "-C", repoDir, "cat-file", typStr, oid.String())
		data, err := cmd.Output()
		if err != nil {
			return 0, nil, err
		}
		return typ, data, nil
	})

	completed, err := CompleteThinPack(thinPack, gitResolver, DefaultLimits())
	if err != nil {
		t.Fatalf("CompleteThinPack on real git thin pack failed: %v", err)
	}

	parsed, err := Parse(bytes.NewReader(completed), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse completed real git pack failed: %v", err)
	}
	idxBytes, err := BuildIdx(parsed)
	if err != nil {
		t.Fatalf("BuildIdx failed: %v", err)
	}

	outDir := t.TempDir()
	packPath := filepath.Join(outDir, "pack-real.pack")
	idxPath := filepath.Join(outDir, "pack-real.idx")
	if err := os.WriteFile(packPath, completed, 0o600); err != nil {
		t.Fatalf("write pack: %v", err)
	}
	if err := os.WriteFile(idxPath, idxBytes, 0o600); err != nil {
		t.Fatalf("write idx: %v", err)
	}

	verifyCmd := exec.Command("git", "verify-pack", "-v", idxPath)
	verifyCmd.Dir = outDir
	if out, err := verifyCmd.CombinedOutput(); err != nil {
		t.Fatalf("git verify-pack failed on completed real thin pack: %v\n%s", err, string(out))
	}
}
