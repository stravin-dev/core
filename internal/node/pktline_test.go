package node

import (
	"bytes"
	"strings"
	"testing"

	"stravin.dev/core/internal/storage/ddb"
)

func TestEncodePktLine(t *testing.T) {
	got := EncodePktLine([]byte("a=b\n"))
	want := []byte("0008a=b\n")
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodePktLine = %q, want %q", got, want)
	}

	if !bytes.Equal(FlushPkt, []byte("0000")) {
		t.Fatalf("FlushPkt = %q, want 0000", FlushPkt)
	}
}

func TestAdvertiseRefs_EmptyRepo(t *testing.T) {
	out := AdvertiseRefs(nil, "refs/heads/main", AdvertiseOptions{
		IncludeServiceHeader: true,
	})
	s := string(out)

	if !strings.HasPrefix(s, "001e# service=git-upload-pack\n0000") {
		t.Fatalf("missing service header prefix, got: %q", s)
	}
	if !strings.Contains(s, ddb.ZeroOID+" capabilities^{}\x00") {
		t.Fatalf("expected empty-repo capabilities^{} line, got: %q", s)
	}
	if !strings.HasSuffix(s, "0000") {
		t.Fatalf("expected trailing flush-pkt 0000, got: %q", s)
	}
}

func TestAdvertiseRefs_HEADSymrefAndMultipleSortedRefs(t *testing.T) {
	oidMain := strings.Repeat("a", 40)
	oidDev := strings.Repeat("b", 40)
	oidTag := strings.Repeat("c", 40)

	// Intentionally unsorted input to verify deterministic lexicographic ordering.
	refs := []ddb.RefState{
		{RefName: "refs/tags/v1.0.0", OID: oidTag},
		{RefName: "refs/heads/main", OID: oidMain},
		{RefName: "refs/heads/dev", OID: oidDev},
	}

	out := AdvertiseRefs(refs, "refs/heads/main", AdvertiseOptions{
		IncludeServiceHeader: false,
	})
	lines := decodePktLines(t, out)
	if len(lines) != 4 {
		t.Fatalf("expected 4 pkt-lines (HEAD + 3 refs), got %d: %q", len(lines), lines)
	}

	// First line must be HEAD with NUL-separated capabilities including symref=HEAD:refs/heads/main.
	if !strings.HasPrefix(lines[0], oidMain+" HEAD\x00") {
		t.Fatalf("line 0 = %q, want prefix %q", lines[0], oidMain+" HEAD\x00")
	}
	if !strings.Contains(lines[0], "symref=HEAD:refs/heads/main") {
		t.Fatalf("line 0 missing symref capability: %q", lines[0])
	}

	// Subsequent lines sorted lexicographically with no NUL capability suffix.
	wantRest := []string{
		oidDev + " refs/heads/dev\n",
		oidMain + " refs/heads/main\n",
		oidTag + " refs/tags/v1.0.0\n",
	}
	for i, want := range wantRest {
		if lines[i+1] != want {
			t.Errorf("line %d = %q, want %q", i+1, lines[i+1], want)
		}
	}
}

func TestAdvertiseRefs_NoMatchingDefaultBranchOmitsHEAD(t *testing.T) {
	oidFeature := strings.Repeat("d", 40)
	refs := []ddb.RefState{
		{RefName: "refs/heads/feature", OID: oidFeature},
	}

	out := AdvertiseRefs(refs, "refs/heads/main", AdvertiseOptions{})
	lines := decodePktLines(t, out)
	if len(lines) != 1 {
		t.Fatalf("expected 1 pkt-line, got %d: %q", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], oidFeature+" refs/heads/feature\x00") {
		t.Fatalf("line 0 = %q, want capabilities attached to first ref", lines[0])
	}
	if strings.Contains(lines[0], "symref=HEAD:") {
		t.Fatalf("unexpected symref when default branch does not exist: %q", lines[0])
	}
}

func decodePktLines(t *testing.T, data []byte) []string {
	t.Helper()
	var lines []string
	i := 0
	for i < len(data) {
		if i+4 > len(data) {
			t.Fatalf("truncated pkt-line length at offset %d", i)
		}
		var n int
		for _, b := range data[i : i+4] {
			n <<= 4
			switch {
			case b >= '0' && b <= '9':
				n |= int(b - '0')
			case b >= 'a' && b <= 'f':
				n |= int(b-'a') + 10
			default:
				t.Fatalf("invalid hex length byte %q at offset %d", b, i)
			}
		}
		if n == 0 {
			i += 4
			continue
		}
		if i+n > len(data) {
			t.Fatalf("pkt-line length %d exceeds remaining %d bytes", n, len(data)-i)
		}
		lines = append(lines, string(data[i+4:i+n]))
		i += n
	}
	return lines
}
