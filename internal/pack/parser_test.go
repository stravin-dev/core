package pack

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1" //nolint:gosec // Git packfile v2 uses SHA-1 per Git specification
	"encoding/binary"
	"errors"
	"testing"
)

// encodePackEntryHeader encodes a Git packfile v2 variable-length entry header.
func encodePackEntryHeader(typ ObjectType, size int64) []byte {
	var out []byte
	b := byte((uint8(typ)&0x07)<<4) | byte(size&0x0f)
	size >>= 4
	for size > 0 {
		out = append(out, b|0x80)
		b = byte(size & 0x7f)
		size >>= 7
	}
	out = append(out, b)
	return out
}

func zlibCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	return buf.Bytes()
}

func encodeOFSDeltaOffset(dist uint64) []byte {
	var rev []byte
	rev = append(rev, byte(dist&0x7f))
	dist >>= 7
	for dist > 0 {
		dist--
		rev = append(rev, byte(dist&0x7f)|0x80)
		dist >>= 7
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev
}

func encodeDeltaVarint(v uint64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if v == 0 {
			break
		}
	}
	return out
}

// buildDeltaCopyInsert builds a valid Git delta that copies [0:copyLen] from base and inserts suffix.
func buildDeltaCopyInsert(baseLen int, copyLen int, insert []byte) []byte {
	var d bytes.Buffer
	d.Write(encodeDeltaVarint(uint64(baseLen)))
	d.Write(encodeDeltaVarint(uint64(copyLen + len(insert))))

	if copyLen > 0 {
		var cmd byte = 0x80
		var sizeBytes []byte
		if copyLen != 0x10000 {
			if copyLen&0xff != 0 {
				cmd |= 0x10
				sizeBytes = append(sizeBytes, byte(copyLen&0xff))
			}
			if (copyLen>>8)&0xff != 0 {
				cmd |= 0x20
				sizeBytes = append(sizeBytes, byte((copyLen>>8)&0xff))
			}
			if (copyLen>>16)&0xff != 0 {
				cmd |= 0x40
				sizeBytes = append(sizeBytes, byte((copyLen>>16)&0xff))
			}
		}
		d.WriteByte(cmd)
		d.Write(sizeBytes)
	}

	for len(insert) > 0 {
		chunk := len(insert)
		if chunk > 127 {
			chunk = 127
		}
		d.WriteByte(byte(chunk))
		d.Write(insert[:chunk])
		insert = insert[chunk:]
	}
	return d.Bytes()
}

type rawPackEntry struct {
	typ     ObjectType
	data    []byte // uncompressed payload (or delta stream)
	ofsDist uint64 // for OBJ_OFS_DELTA
	refBase OID    // for OBJ_REF_DELTA
}

func buildRawPack(t *testing.T, version uint32, entries []rawPackEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, version)
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(entries)))

	for _, e := range entries {
		buf.Write(encodePackEntryHeader(e.typ, int64(len(e.data))))
		switch e.typ {
		case ObjOFSDelta:
			buf.Write(encodeOFSDeltaOffset(e.ofsDist))
		case ObjRefDelta:
			buf.Write(e.refBase[:])
		}
		buf.Write(zlibCompress(t, e.data))
	}

	sum := sha1.Sum(buf.Bytes()) //nolint:gosec
	buf.Write(sum[:])
	return buf.Bytes()
}

func TestParseStandardPack(t *testing.T) {
	blob1 := []byte("hello stravin packfile\n")
	blob2 := []byte("second blob content")
	packBytes := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjBlob, data: blob1},
		{typ: ObjBlob, data: blob2},
	})

	parsed, err := Parse(bytes.NewReader(packBytes), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(parsed.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(parsed.Entries))
	}

	oid1 := ComputeOID(ObjBlob, blob1)
	oid2 := ComputeOID(ObjBlob, blob2)
	obj1, ok := parsed.Objects[oid1]
	if !ok || !bytes.Equal(obj1.Data, blob1) || obj1.Type != ObjBlob {
		t.Fatalf("blob1 mismatch: got %+v, ok=%v", obj1, ok)
	}
	obj2, ok := parsed.Objects[oid2]
	if !ok || !bytes.Equal(obj2.Data, blob2) || obj2.Type != ObjBlob {
		t.Fatalf("blob2 mismatch: got %+v, ok=%v", obj2, ok)
	}
}

func TestParseOFSDeltaAndRefDelta(t *testing.T) {
	base := []byte("0123456789abcdef")
	baseOID := ComputeOID(ObjBlob, base)
	delta1 := buildDeltaCopyInsert(len(base), 10, []byte("-ofs"))
	want1 := []byte("0123456789-ofs")
	delta2 := buildDeltaCopyInsert(len(want1), len(want1), []byte("-ref"))
	want2 := []byte("0123456789-ofs-ref")

	// Construct pack manually so we know exact offset distance for OFS_DELTA.
	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(3))

	baseOffset := uint64(buf.Len())
	buf.Write(encodePackEntryHeader(ObjBlob, int64(len(base))))
	buf.Write(zlibCompress(t, base))

	ofsEntryOffset := uint64(buf.Len())
	buf.Write(encodePackEntryHeader(ObjOFSDelta, int64(len(delta1))))
	buf.Write(encodeOFSDeltaOffset(ofsEntryOffset - baseOffset))
	buf.Write(zlibCompress(t, delta1))

	oid1 := ComputeOID(ObjBlob, want1)
	buf.Write(encodePackEntryHeader(ObjRefDelta, int64(len(delta2))))
	buf.Write(oid1[:])
	buf.Write(zlibCompress(t, delta2))

	sum := sha1.Sum(buf.Bytes()) //nolint:gosec
	buf.Write(sum[:])

	parsed, err := Parse(bytes.NewReader(buf.Bytes()), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(parsed.Objects) != 3 {
		t.Fatalf("got %d objects, want 3", len(parsed.Objects))
	}
	if got := parsed.Objects[baseOID].Data; !bytes.Equal(got, base) {
		t.Fatalf("base mismatch: %q", got)
	}
	if got := parsed.Objects[oid1].Data; !bytes.Equal(got, want1) {
		t.Fatalf("ofs delta mismatch: got %q, want %q", got, want1)
	}
	oid2 := ComputeOID(ObjBlob, want2)
	if got := parsed.Objects[oid2].Data; !bytes.Equal(got, want2) {
		t.Fatalf("ref delta mismatch: got %q, want %q", got, want2)
	}
}

func TestParseRejectsMalformedPacks(t *testing.T) {
	validPack := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjBlob, data: []byte("abc")},
	})

	t.Run("bad magic", func(t *testing.T) {
		bad := append([]byte(nil), validPack...)
		bad[0] = 'X'
		_, err := Parse(bytes.NewReader(bad), ParseOptions{})
		if !errors.Is(err, ErrInvalidHeader) {
			t.Fatalf("got err=%v, want ErrInvalidHeader", err)
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		bad := buildRawPack(t, 3, []rawPackEntry{{typ: ObjBlob, data: []byte("abc")}})
		_, err := Parse(bytes.NewReader(bad), ParseOptions{})
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("got err=%v, want ErrUnsupportedVersion", err)
		}
	})

	t.Run("corrupt trailer checksum", func(t *testing.T) {
		bad := append([]byte(nil), validPack...)
		bad[len(bad)-1] ^= 0xff
		_, err := Parse(bytes.NewReader(bad), ParseOptions{})
		if !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("got err=%v, want ErrChecksumMismatch", err)
		}
	})

	t.Run("trailing garbage", func(t *testing.T) {
		bad := append(append([]byte(nil), validPack...), 0x00)
		_, err := Parse(bytes.NewReader(bad), ParseOptions{})
		if err == nil {
			t.Fatal("expected error for trailing bytes, got nil")
		}
	})

	t.Run("max object size limit", func(t *testing.T) {
		big := buildRawPack(t, 2, []rawPackEntry{{typ: ObjBlob, data: bytes.Repeat([]byte("a"), 256)}})
		_, err := Parse(bytes.NewReader(big), ParseOptions{
			Limits: Limits{MaxObjects: 10, MaxObjectSize: 64, MaxTotalUncompressed: 1024, MaxDeltaDepth: 10, MaxPackSize: 4096},
		})
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got err=%v, want ErrLimitExceeded", err)
		}
	})

	t.Run("delta depth bomb", func(t *testing.T) {
		base := []byte("seed")
		d1 := buildDeltaCopyInsert(len(base), len(base), []byte("1"))
		d2 := buildDeltaCopyInsert(len(base)+1, len(base)+1, []byte("2"))
		d3 := buildDeltaCopyInsert(len(base)+2, len(base)+2, []byte("3"))

		oid0 := ComputeOID(ObjBlob, base)
		oid1 := ComputeOID(ObjBlob, []byte("seed1"))
		oid2 := ComputeOID(ObjBlob, []byte("seed12"))

		pack := buildRawPack(t, 2, []rawPackEntry{
			{typ: ObjBlob, data: base},
			{typ: ObjRefDelta, refBase: oid0, data: d1},
			{typ: ObjRefDelta, refBase: oid1, data: d2},
			{typ: ObjRefDelta, refBase: oid2, data: d3},
		})

		_, err := Parse(bytes.NewReader(pack), ParseOptions{
			Limits: Limits{MaxObjects: 10, MaxObjectSize: 1024, MaxTotalUncompressed: 4096, MaxDeltaDepth: 2, MaxPackSize: 4096},
		})
		if !errors.Is(err, ErrDeltaBomb) {
			t.Fatalf("got err=%v, want ErrDeltaBomb", err)
		}
	})
}
