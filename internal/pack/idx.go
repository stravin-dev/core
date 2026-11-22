package pack

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // Git .idx v2 format requires SHA-1 checksums per Git specification
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	idxVersion2        = 2
	idxFanoutEntries   = 256
	idxHeaderSize      = 8
	idxFanoutSize      = idxFanoutEntries * 4
	idxMinSize         = idxHeaderSize + idxFanoutSize + 2*packTrailerSize
	idxLargeOffsetFlag = uint32(0x80000000)
)

var idxMagic = [4]byte{0xff, 't', 'O', 'c'}

type idxEntry struct {
	oid    OID
	offset uint64
	crc32  uint32
}

// IdxBuilder constructs a Git packfile .idx v2 binary file (GoF Builder pattern).
type IdxBuilder struct {
	packChecksum OID
	entries      []idxEntry
}

// NewIdxBuilder creates a new IdxBuilder for the given packfile SHA-1 checksum.
func NewIdxBuilder(packChecksum OID) *IdxBuilder {
	return &IdxBuilder{packChecksum: packChecksum}
}

// Add registers an object's OID, packfile byte offset, and raw entry CRC32.
func (b *IdxBuilder) Add(oid OID, offset uint64, crc32 uint32) *IdxBuilder {
	b.entries = append(b.entries, idxEntry{
		oid:    oid,
		offset: offset,
		crc32:  crc32,
	})
	return b
}

// Build validates, sorts, and serializes the .idx v2 byte representation.
func (b *IdxBuilder) Build() ([]byte, error) {
	sorted := make([]idxEntry, len(b.entries))
	copy(sorted, b.entries)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].oid[:], sorted[j].oid[:]) < 0
	})

	for i := 1; i < len(sorted); i++ {
		if sorted[i].oid == sorted[i-1].oid {
			return nil, fmt.Errorf("duplicate oid %s: %w", sorted[i].oid, ErrInvalidIdx)
		}
	}

	var fanout [idxFanoutEntries]uint32
	for _, e := range sorted {
		fanout[e.oid[0]]++
	}
	var running uint32
	for i := 0; i < idxFanoutEntries; i++ {
		running += fanout[i]
		fanout[i] = running
	}

	var largeOffsets []uint64
	smallOffsets := make([]uint32, len(sorted))
	for i, e := range sorted {
		if e.offset < uint64(idxLargeOffsetFlag) {
			smallOffsets[i] = uint32(e.offset)
		} else {
			smallOffsets[i] = idxLargeOffsetFlag | uint32(len(largeOffsets))
			largeOffsets = append(largeOffsets, e.offset)
		}
	}

	totalSize := idxHeaderSize + idxFanoutSize + len(sorted)*(20+4+4) + len(largeOffsets)*8 + 2*packTrailerSize
	buf := bytes.NewBuffer(make([]byte, 0, totalSize))

	buf.Write(idxMagic[:])
	_ = binary.Write(buf, binary.BigEndian, uint32(idxVersion2))

	for i := 0; i < idxFanoutEntries; i++ {
		_ = binary.Write(buf, binary.BigEndian, fanout[i])
	}
	for _, e := range sorted {
		buf.Write(e.oid[:])
	}
	for _, e := range sorted {
		_ = binary.Write(buf, binary.BigEndian, e.crc32)
	}
	for _, off := range smallOffsets {
		_ = binary.Write(buf, binary.BigEndian, off)
	}
	for _, loff := range largeOffsets {
		_ = binary.Write(buf, binary.BigEndian, loff)
	}

	buf.Write(b.packChecksum[:])
	idxSum := sha1.Sum(buf.Bytes()) //nolint:gosec // Git .idx v2 trailer requires SHA-1
	buf.Write(idxSum[:])
	return buf.Bytes(), nil
}

// BuildIdx is a convenience helper that builds a .idx v2 file directly from a ParsedPack.
func BuildIdx(p *ParsedPack) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("nil parsed pack: %w", ErrInvalidIdx)
	}
	builder := NewIdxBuilder(p.PackChecksum)
	for _, e := range p.Entries {
		builder.Add(e.OID, e.Offset, e.CRC32)
	}
	return builder.Build()
}

// Idx is a parsed, read-only Git .idx v2 index supporting O(log N) OID lookup.
type Idx struct {
	fanout       [idxFanoutEntries]uint32
	oids         []OID
	crcs         []uint32
	offsets      []uint64
	packChecksum OID
	idxChecksum  OID
}

// ParseIdx validates and parses a Git .idx v2 byte slice.
func ParseIdx(data []byte) (*Idx, error) {
	if len(data) < idxMinSize {
		return nil, fmt.Errorf("idx too short (%d bytes): %w", len(data), ErrInvalidIdx)
	}
	if !bytes.Equal(data[:4], idxMagic[:]) {
		return nil, fmt.Errorf("bad idx magic: %w", ErrInvalidIdx)
	}
	version := binary.BigEndian.Uint32(data[4:8])
	if version != idxVersion2 {
		return nil, fmt.Errorf("idx version %d: %w", version, ErrUnsupportedVersion)
	}

	// Verify trailing .idx SHA-1 checksum.
	body := data[:len(data)-packTrailerSize]
	expectedSum := sha1.Sum(body) //nolint:gosec // Git .idx v2 checksum requires SHA-1
	var actualSum OID
	copy(actualSum[:], data[len(data)-packTrailerSize:])
	if expectedSum != actualSum {
		return nil, fmt.Errorf("idx checksum %s != %s: %w", actualSum, expectedSum, ErrChecksumMismatch)
	}

	idx := &Idx{idxChecksum: actualSum}
	copy(idx.packChecksum[:], data[len(data)-2*packTrailerSize:len(data)-packTrailerSize])

	pos := idxHeaderSize
	var prev uint32
	for i := 0; i < idxFanoutEntries; i++ {
		v := binary.BigEndian.Uint32(data[pos : pos+4])
		if v < prev {
			return nil, fmt.Errorf("non-monotonic fanout at %d: %w", i, ErrInvalidIdx)
		}
		idx.fanout[i] = v
		prev = v
		pos += 4
	}

	count := int(idx.fanout[255])
	oidEnd := pos + count*20
	crcEnd := oidEnd + count*4
	offEnd := crcEnd + count*4
	trailerStart := len(data) - 2*packTrailerSize
	if offEnd > trailerStart {
		return nil, fmt.Errorf("idx truncated for %d entries: %w", count, ErrInvalidIdx)
	}

	largeBytes := trailerStart - offEnd
	if largeBytes%8 != 0 {
		return nil, fmt.Errorf("invalid large-offset table size %d: %w", largeBytes, ErrInvalidIdx)
	}
	largeCount := largeBytes / 8

	idx.oids = make([]OID, count)
	for i := 0; i < count; i++ {
		copy(idx.oids[i][:], data[pos+i*20:pos+(i+1)*20])
		if i > 0 && bytes.Compare(idx.oids[i-1][:], idx.oids[i][:]) >= 0 {
			return nil, fmt.Errorf("unsorted or duplicate OID at index %d: %w", i, ErrInvalidIdx)
		}
	}

	idx.crcs = make([]uint32, count)
	for i := 0; i < count; i++ {
		idx.crcs[i] = binary.BigEndian.Uint32(data[oidEnd+i*4 : oidEnd+(i+1)*4])
	}

	idx.offsets = make([]uint64, count)
	for i := 0; i < count; i++ {
		rawOff := binary.BigEndian.Uint32(data[crcEnd+i*4 : crcEnd+(i+1)*4])
		if rawOff&idxLargeOffsetFlag == 0 {
			idx.offsets[i] = uint64(rawOff)
		} else {
			largeIdx := int(rawOff &^ idxLargeOffsetFlag)
			if largeIdx >= largeCount {
				return nil, fmt.Errorf("large offset index %d out of range (%d): %w", largeIdx, largeCount, ErrInvalidIdx)
			}
			idx.offsets[i] = binary.BigEndian.Uint64(data[offEnd+largeIdx*8 : offEnd+(largeIdx+1)*8])
		}
	}

	return idx, nil
}

// Count returns the number of indexed objects.
func (x *Idx) Count() uint32 {
	return x.fanout[255]
}

// PackChecksum returns the SHA-1 checksum of the associated packfile.
func (x *Idx) PackChecksum() OID {
	return x.packChecksum
}

// IdxChecksum returns the SHA-1 checksum of the .idx file itself.
func (x *Idx) IdxChecksum() OID {
	return x.idxChecksum
}

// Lookup searches the index for oid and returns its packfile byte offset and entry CRC32.
func (x *Idx) Lookup(oid OID) (uint64, uint32, bool) {
	var lo int
	if oid[0] > 0 {
		lo = int(x.fanout[oid[0]-1])
	}
	hi := int(x.fanout[oid[0]])
	if lo >= hi {
		return 0, 0, false
	}

	rel := sort.Search(hi-lo, func(i int) bool {
		return bytes.Compare(x.oids[lo+i][:], oid[:]) >= 0
	})
	pos := lo + rel
	if pos < hi && x.oids[pos] == oid {
		return x.offsets[pos], x.crcs[pos], true
	}
	return 0, 0, false
}
