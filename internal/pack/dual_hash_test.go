package pack

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type dualPackEntry struct {
	typ        ObjectType
	data       []byte
	ofsBaseIdx int
	refBase    OID
}

func buildPackWithAlgo(t *testing.T, algo HashAlgorithm, entries []dualPackEntry) []byte {
	t.Helper()
	algo = DefaultHashAlgorithm(algo)

	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(entries)))

	offsets := make([]uint64, len(entries))
	for i, e := range entries {
		offsets[i] = uint64(buf.Len())
		buf.Write(encodePackEntryHeader(e.typ, int64(len(e.data))))
		switch e.typ {
		case ObjOFSDelta:
			dist := offsets[i] - offsets[e.ofsBaseIdx]
			buf.Write(encodeOFSDeltaOffset(dist))
		case ObjRefDelta:
			buf.Write(e.refBase.Bytes())
		}
		zw := zlib.NewWriter(&buf)
		if _, err := zw.Write(e.data); err != nil {
			t.Fatalf("zlib write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zlib close: %v", err)
		}
	}

	sum := algo.Sum(buf.Bytes())
	buf.Write(sum.Bytes())
	return buf.Bytes()
}

func TestHashAlgorithmStrategyAndOID(t *testing.T) {
	for _, tc := range []struct {
		format   string
		wantName string
		wantSize int
		wantHex  int
	}{
		{"", "sha1", 20, 40},
		{"sha1", "sha1", 20, 40},
		{"SHA1", "sha1", 20, 40},
		{"sha256", "sha256", 32, 64},
		{"SHA256", "sha256", 32, 64},
	} {
		algo, err := HashAlgorithmForFormat(tc.format)
		if err != nil {
			t.Fatalf("HashAlgorithmForFormat(%q): %v", tc.format, err)
		}
		if algo.Name() != tc.wantName || algo.Size() != tc.wantSize || algo.HexSize() != tc.wantHex {
			t.Fatalf("format %q: got (%s,%d,%d), want (%s,%d,%d)",
				tc.format, algo.Name(), algo.Size(), algo.HexSize(), tc.wantName, tc.wantSize, tc.wantHex)
		}
		zero := algo.ZeroOID()
		if !zero.IsZero() || zero.Len() != tc.wantSize || len(zero.String()) != tc.wantHex {
			t.Fatalf("zero OID for %s invalid: %+v", algo.Name(), zero)
		}
	}

	if _, err := HashAlgorithmForFormat("md5"); !errors.Is(err, ErrUnsupportedHashAlgorithm) {
		t.Fatalf("expected ErrUnsupportedHashAlgorithm, got %v", err)
	}

	// Zero value OID{} must equal SHA1.ZeroOID() and ParseOID(40 zeros).
	parsedZeroSHA1, err := ParseOID(strings.Repeat("0", 40))
	if err != nil {
		t.Fatalf("ParseOID(40 zeros): %v", err)
	}
	if (OID{}) != SHA1.ZeroOID() || (OID{}) != parsedZeroSHA1 {
		t.Fatalf("zero SHA-1 OID equality mismatch")
	}

	parsedZeroSHA256, err := ParseOID(strings.Repeat("0", 64))
	if err != nil {
		t.Fatalf("ParseOID(64 zeros): %v", err)
	}
	if !parsedZeroSHA256.IsZero() || parsedZeroSHA256 != SHA256.ZeroOID() {
		t.Fatalf("zero SHA-256 OID mismatch")
	}
	if parsedZeroSHA1 == parsedZeroSHA256 {
		t.Fatalf("SHA-1 zero OID must not equal SHA-256 zero OID")
	}

	// Strict algorithm parsing rejects mismatched lengths.
	if _, err := ParseOIDForAlgorithm(strings.Repeat("a", 64), SHA1); err == nil {
		t.Fatalf("expected error parsing 64-hex with SHA1")
	}
	if _, err := ParseOIDForAlgorithm(strings.Repeat("a", 40), SHA256); err == nil {
		t.Fatalf("expected error parsing 40-hex with SHA256")
	}

	// ComputeOIDWith produces distinct 20B vs 32B OIDs.
	b1 := ComputeOIDWith(SHA1, ObjBlob, []byte("hello world\n"))
	b2 := ComputeOIDWith(SHA256, ObjBlob, []byte("hello world\n"))
	if b1.Len() != 20 || len(b1.String()) != 40 {
		t.Fatalf("unexpected SHA-1 blob OID: %s", b1)
	}
	if b2.Len() != 32 || len(b2.String()) != 64 {
		t.Fatalf("unexpected SHA-256 blob OID: %s", b2)
	}
}

func TestDualHashPackParseIdxThinAndReader(t *testing.T) {
	for _, algo := range []HashAlgorithm{SHA1, SHA256} {
		t.Run(algo.Name(), func(t *testing.T) {
			blob1 := []byte("line1\nline2\nline3\n")
			blob2 := []byte("line1\nline2-modified\nline3\n")
			blob1OID := ComputeOIDWith(algo, ObjBlob, blob1)
			blob2OID := ComputeOIDWith(algo, ObjBlob, blob2)

			tree1Data := buildTreePayload([]TreeEntry{
				{Mode: 0o100644, Name: "README.md", OID: blob1OID},
			})
			tree1OID := ComputeOIDWith(algo, ObjTree, tree1Data)

			tree2Data := buildTreePayload([]TreeEntry{
				{Mode: 0o100644, Name: "README.md", OID: blob2OID},
			})
			tree2OID := ComputeOIDWith(algo, ObjTree, tree2Data)

			c1Data := buildCommitPayload(tree1OID, nil,
				"Alice <alice@stravin.dev> 1700000000 +0000",
				"Alice <alice@stravin.dev> 1700000000 +0000",
				"initial commit\n")
			c1OID := ComputeOIDWith(algo, ObjCommit, c1Data)

			c2Data := buildCommitPayload(tree2OID, []OID{c1OID},
				"Bob <bob@stravin.dev> 1700000100 +0000",
				"Bob <bob@stravin.dev> 1700000100 +0000",
				"second commit\n")
			c2OID := ComputeOIDWith(algo, ObjCommit, c2Data)

			delta12 := buildDeltaCopyInsert(len(blob1), 0, blob2)

			// 1. Parse pack containing bases + OFS_DELTA + REF_DELTA.
			packBytes := buildPackWithAlgo(t, algo, []dualPackEntry{
				{typ: ObjBlob, data: blob1},
				{typ: ObjOFSDelta, ofsBaseIdx: 0, data: delta12},
				{typ: ObjTree, data: tree1Data},
				{typ: ObjTree, data: tree2Data},
				{typ: ObjCommit, data: c1Data},
				{typ: ObjCommit, data: c2Data},
			})

			parsed, err := Parse(bytes.NewReader(packBytes), ParseOptions{HashAlgorithm: algo})
			if err != nil {
				t.Fatalf("Parse(%s): %v", algo.Name(), err)
			}
			if parsed.HashAlgorithm.Name() != algo.Name() {
				t.Fatalf("parsed.HashAlgorithm = %s, want %s", parsed.HashAlgorithm.Name(), algo.Name())
			}
			if len(parsed.Objects) != 6 {
				t.Fatalf("got %d objects, want 6", len(parsed.Objects))
			}
			if got := parsed.Objects[blob2OID]; got == nil || !bytes.Equal(got.Data, blob2) {
				t.Fatalf("delta-resolved blob2 mismatch: %+v", got)
			}

			// Corrupting trailer fails with ErrChecksumMismatch.
			corrupt := append([]byte(nil), packBytes...)
			corrupt[len(corrupt)-1] ^= 0xff
			if _, err := Parse(bytes.NewReader(corrupt), ParseOptions{HashAlgorithm: algo}); !errors.Is(err, ErrChecksumMismatch) {
				t.Fatalf("corrupt pack err = %v, want ErrChecksumMismatch", err)
			}

			// 2. Build & parse .idx v2 for this algorithm.
			idxBytes, err := BuildIdx(parsed)
			if err != nil {
				t.Fatalf("BuildIdx(%s): %v", algo.Name(), err)
			}
			idx, err := ParseIdxWithAlgorithm(idxBytes, algo)
			if err != nil {
				t.Fatalf("ParseIdxWithAlgorithm(%s): %v", algo.Name(), err)
			}
			if idx.Count() != 6 || idx.PackChecksum() != parsed.PackChecksum {
				t.Fatalf("idx metadata mismatch: count=%d checksum=%s", idx.Count(), idx.PackChecksum())
			}
			for _, e := range parsed.Entries {
				off, crc, ok := idx.Lookup(e.OID)
				if !ok || off != e.Offset || crc != e.CRC32 {
					t.Fatalf("idx.Lookup(%s) = (%d,%d,%v), want (%d,%d,true)", e.OID, off, crc, ok, e.Offset, e.CRC32)
				}
			}

			// Cross-algorithm lookup returns false.
			otherAlgo := SHA256
			if algo.Name() == "sha256" {
				otherAlgo = SHA1
			}
			if _, _, ok := idx.Lookup(ComputeOIDWith(otherAlgo, ObjBlob, blob1)); ok {
				t.Fatalf("expected cross-algorithm lookup to return false")
			}

			// 3. Thin-pack completion with external REF_DELTA base.
			thinPack := buildPackWithAlgo(t, algo, []dualPackEntry{
				{typ: ObjRefDelta, refBase: blob1OID, data: delta12},
			})
			resolver := MapBaseResolver{
				blob1OID: {OID: blob1OID, Type: ObjBlob, Data: blob1},
			}
			completer := NewPureGoThinCompleter(DefaultLimits())
			completed, err := completer.CompleteWithAlgorithm(context.Background(), thinPack, resolver, algo)
			if err != nil {
				t.Fatalf("CompleteWithAlgorithm(%s): %v", algo.Name(), err)
			}
			parsedCompleted, err := Parse(bytes.NewReader(completed), ParseOptions{HashAlgorithm: algo})
			if err != nil {
				t.Fatalf("Parse(completed %s): %v", algo.Name(), err)
			}
			if len(parsedCompleted.Objects) != 2 || parsedCompleted.Objects[blob2OID] == nil {
				t.Fatalf("completed pack missing expected objects: %d", len(parsedCompleted.Objects))
			}

			// 4. PackReader commit walk, tree diff, and blob chunk reads.
			reader := NewPackReader(parsed, nil)
			commits, err := reader.ListNewCommits(algo.ZeroOID(), c2OID, 10)
			if err != nil {
				t.Fatalf("ListNewCommits(%s): %v", algo.Name(), err)
			}
			if len(commits) != 2 || commits[0].OID != c2OID || commits[1].OID != c1OID {
				t.Fatalf("unexpected commits: %+v", commits)
			}
			changes, err := reader.DiffTree(tree1OID, tree2OID, 10)
			if err != nil {
				t.Fatalf("DiffTree(%s): %v", algo.Name(), err)
			}
			if len(changes) != 1 || changes[0].Path != "README.md" || changes[0].Kind != ChangeModify ||
				changes[0].OldOID != blob1OID || changes[0].NewOID != blob2OID {
				t.Fatalf("unexpected diff changes: %+v", changes)
			}
			chunk, err := reader.ReadBlobChunk(blob2OID, 6, 14)
			if err != nil {
				t.Fatalf("ReadBlobChunk(%s): %v", algo.Name(), err)
			}
			if string(chunk) != "line2-modified" {
				t.Fatalf("chunk = %q, want line2-modified", string(chunk))
			}
		})
	}
}

func TestSHA256PackInteropWithRealGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not found")
	}

	tmp := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = tmp
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Stravin Test",
			"GIT_AUTHOR_EMAIL=test@stravin.dev",
			"GIT_COMMITTER_NAME=Stravin Test",
			"GIT_COMMITTER_EMAIL=test@stravin.dev",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	runGit("init", "--object-format=sha256", "-b", "main")
	if err := os.WriteFile(filepath.Join(tmp, "hello.txt"), []byte("hello sha256 git\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "hello.txt")
	runGit("commit", "-m", "initial sha256 commit")
	headHex := runGit("rev-parse", "HEAD")
	if len(headHex) != 64 {
		t.Fatalf("expected 64-char SHA-256 HEAD, got %q", headHex)
	}
	runGit("repack", "-a", "-d")

	packDir := filepath.Join(tmp, ".git", "objects", "pack")
	entries, err := os.ReadDir(packDir)
	if err != nil {
		t.Fatalf("readdir pack: %v", err)
	}
	var packPath, idxPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pack") {
			packPath = filepath.Join(packDir, e.Name())
			idxPath = strings.TrimSuffix(packPath, ".pack") + ".idx"
			break
		}
	}
	if packPath == "" {
		t.Fatalf("no .pack produced by git repack")
	}

	packBytes, err := os.ReadFile(packPath)
	if err != nil {
		t.Fatal(err)
	}
	gitIdxBytes, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := Parse(bytes.NewReader(packBytes), ParseOptions{HashAlgorithm: SHA256})
	if err != nil {
		t.Fatalf("pack.Parse(git sha256 pack): %v", err)
	}
	headOID, err := ParseOIDForAlgorithm(headHex, SHA256)
	if err != nil {
		t.Fatalf("ParseOIDForAlgorithm(%q): %v", headHex, err)
	}
	if _, ok := parsed.Objects[headOID]; !ok {
		t.Fatalf("parsed SHA-256 pack missing HEAD commit %s", headHex)
	}

	// Our generated .idx v2 must match Git's generated .idx v2 byte-for-byte!
	ourIdxBytes, err := BuildIdx(parsed)
	if err != nil {
		t.Fatalf("BuildIdx(sha256): %v", err)
	}
	if !bytes.Equal(ourIdxBytes, gitIdxBytes) {
		t.Fatalf("BuildIdx SHA-256 output differs from git's .idx v2 (got %d bytes, want %d bytes)", len(ourIdxBytes), len(gitIdxBytes))
	}

	parsedIdx, err := ParseIdxWithAlgorithm(gitIdxBytes, SHA256)
	if err != nil {
		t.Fatalf("ParseIdxWithAlgorithm(git sha256 idx): %v", err)
	}
	if _, _, ok := parsedIdx.Lookup(headOID); !ok {
		t.Fatalf("parsed git SHA-256 idx missing HEAD %s", headOID)
	}

	// Verify PackReader can walk the real Git SHA-256 commit and tree.
	reader := NewPackReader(parsed, nil)
	commits, err := reader.ListNewCommits(SHA256.ZeroOID(), headOID, 10)
	if err != nil {
		t.Fatalf("ListNewCommits(real git sha256): %v", err)
	}
	if len(commits) != 1 || commits[0].OID != headOID {
		t.Fatalf("unexpected commits from real git sha256 pack: %+v", commits)
	}
	changes, err := reader.DiffTree(SHA256.ZeroOID(), commits[0].TreeOID, 10)
	if err != nil {
		t.Fatalf("DiffTree(real git sha256): %v", err)
	}
	if len(changes) != 1 || changes[0].Path != "hello.txt" {
		t.Fatalf("unexpected tree diff from real git sha256 pack: %+v", changes)
	}
	fmt.Printf("Verified byte-for-byte SHA-256 .pack/.idx v2 interop with git (HEAD=%s)\n", headHex)
}
