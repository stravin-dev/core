package pack

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestIdxBuilderRoundTripAndGitVerifyPack(t *testing.T) {
	b1 := []byte("alpha blob")
	b2 := []byte("beta blob")
	b3 := []byte("gamma blob")
	packBytes := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjBlob, data: b1},
		{typ: ObjBlob, data: b2},
		{typ: ObjBlob, data: b3},
	})

	parsed, err := Parse(bytes.NewReader(packBytes), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	idxBytes, err := BuildIdx(parsed)
	if err != nil {
		t.Fatalf("BuildIdx failed: %v", err)
	}

	idx, err := ParseIdx(idxBytes)
	if err != nil {
		t.Fatalf("ParseIdx failed: %v", err)
	}
	if idx.Count() != 3 {
		t.Fatalf("got count %d, want 3", idx.Count())
	}
	if idx.PackChecksum() != parsed.PackChecksum {
		t.Fatalf("pack checksum mismatch: got %s, want %s", idx.PackChecksum(), parsed.PackChecksum)
	}

	for _, e := range parsed.Entries {
		off, crc, ok := idx.Lookup(e.OID)
		if !ok {
			t.Fatalf("Lookup(%s) not found", e.OID)
		}
		if off != e.Offset {
			t.Fatalf("Lookup(%s) offset=%d, want %d", e.OID, off, e.Offset)
		}
		if crc != e.CRC32 {
			t.Fatalf("Lookup(%s) crc=%#x, want %#x", e.OID, crc, e.CRC32)
		}
	}

	if _, _, ok := idx.Lookup(ComputeOID(ObjBlob, []byte("absent"))); ok {
		t.Fatal("expected absent OID lookup to return false")
	}

	if _, err := exec.LookPath("git"); err == nil {
		dir := t.TempDir()
		packPath := filepath.Join(dir, "pack-idx.pack")
		idxPath := filepath.Join(dir, "pack-idx.idx")
		if err := os.WriteFile(packPath, packBytes, 0o600); err != nil {
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

func TestIdxLarge64BitOffsets(t *testing.T) {
	packSum := OIDFromSHA1([20]byte{0xaa})

	oidSmall := ComputeOID(ObjBlob, []byte("small"))
	oidLarge := ComputeOID(ObjBlob, []byte("large"))
	const largeOff uint64 = (1 << 32) + 42

	idxBytes, err := NewIdxBuilder(packSum).
		Add(oidLarge, largeOff, 0x11223344).
		Add(oidSmall, 12, 0x55667788).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := ParseIdx(idxBytes)
	if err != nil {
		t.Fatalf("ParseIdx failed: %v", err)
	}
	off, crc, ok := idx.Lookup(oidLarge)
	if !ok || off != largeOff || crc != 0x11223344 {
		t.Fatalf("large offset lookup mismatch: ok=%v off=%d crc=%#x", ok, off, crc)
	}
	off, crc, ok = idx.Lookup(oidSmall)
	if !ok || off != 12 || crc != 0x55667788 {
		t.Fatalf("small offset lookup mismatch: ok=%v off=%d crc=%#x", ok, off, crc)
	}
}

func TestParseIdxRejectsCorrupt(t *testing.T) {
	var packSum OID
	idxBytes, err := NewIdxBuilder(packSum).Add(ComputeOID(ObjBlob, []byte("x")), 12, 1).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	badSum := append([]byte(nil), idxBytes...)
	badSum[len(badSum)-1] ^= 0x01
	if _, err := ParseIdx(badSum); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("corrupt checksum: got %v, want ErrChecksumMismatch", err)
	}

	badMagic := append([]byte(nil), idxBytes...)
	badMagic[0] = 0x00
	if _, err := ParseIdx(badMagic); !errors.Is(err, ErrInvalidIdx) {
		t.Fatalf("bad magic: got %v, want ErrInvalidIdx", err)
	}
}
