package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	idxVersion2        = 2
	idxFanoutEntries   = 256
	idxHeaderSize      = 8
	idxFanoutSize      = idxFanoutEntries * 4
	idxLargeOffsetFlag = uint32(0x80000000)
)

var idxMagic = [4]byte{0xff, 't', 'O', 'c'}

type idxEntry struct {
	oid    OID
	offset uint64
	crc32  uint32
}

// IdxBuilder constructs a Git packfile .idx v2 binary file (GoF Builder pattern)
// for either SHA-1 (20-byte) or SHA-256 (32-byte) packfiles.
type IdxBuilder struct {
	algo         HashAlgorithm
	packChecksum OID
	entries      []idxEntry
}

// NewIdxBuilder creates a new IdxBuilder for the given packfile checksum.
// The hash algorithm (SHA-1 or SHA-256) is inferred from packChecksum.
func NewIdxBuilder(packChecksum OID) *IdxBuilder {
	return &IdxBuilder{
		algo:         packChecksum.Algorithm(),
		packChecksum: packChecksum,
	}
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
	algo := DefaultHashAlgorithm(b.algo)
	oidSize := algo.Size()

	sorted := make([]idxEntry, len(b.entries))
	copy(sorted, b.entries)
	for _, e := range sorted {
		if e.oid.Len() != oidSize {
			return nil, fmt.Errorf("oid %s length %d does not match %s (%d): %w", e.oid, e.oid.Len(), algo.Name(), oidSize, ErrInvalidIdx)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].oid.Compare(sorted[j].oid) < 0
	})

	for i := 1; i < len(sorted); i++ {
		if sorted[i].oid == sorted[i-1].oid {
			return nil, fmt.Errorf("duplicate oid %s: %w", sorted[i].oid, ErrInvalidIdx)
		}
	}

	var fanout [idxFanoutEntries]uint32
	for _, e := range sorted {
		fanout[e.oid.FirstByte()]++
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

	totalSize := idxHeaderSize + idxFanoutSize + len(sorted)*(oidSize+4+4) + len(largeOffsets)*8 + 2*oidSize
	buf := bytes.NewBuffer(make([]byte, 0, totalSize))

	buf.Write(idxMagic[:])
	_ = binary.Write(buf, binary.BigEndian, uint32(idxVersion2))

	for i := 0; i < idxFanoutEntries; i++ {
		_ = binary.Write(buf, binary.BigEndian, fanout[i])
	}
	for _, e := range sorted {
		buf.Write(e.oid.Bytes())
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

	buf.Write(b.packChecksum.Bytes())
	idxSum := algo.Sum(buf.Bytes())
	buf.Write(idxSum.Bytes())
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
	algo         HashAlgorithm
	fanout       [idxFanoutEntries]uint32
	oids         []OID
	crcs         []uint32
	offsets      []uint64
	packChecksum OID
	idxChecksum  OID
}

// ParseIdx validates and parses a SHA-1 Git .idx v2 byte slice.
func ParseIdx(data []byte) (*Idx, error) {
	return ParseIdxWithAlgorithm(data, SHA1)
}

// ParseIdxWithAlgorithm validates and parses a Git .idx v2 byte slice using algo (default SHA-1).
func ParseIdxWithAlgorithm(data []byte, algo HashAlgorithm) (*Idx, error) {
	algo = DefaultHashAlgorithm(algo)
	oidSize := algo.Size()
	minSize := idxHeaderSize + idxFanoutSize + 2*oidSize

	if len(data) < minSize {
		return nil, fmt.Errorf("idx too short (%d bytes): %w", len(data), ErrInvalidIdx)
	}
	if !bytes.Equal(data[:4], idxMagic[:]) {
		return nil, fmt.Errorf("bad idx magic: %w", ErrInvalidIdx)
	}
	version := binary.BigEndian.Uint32(data[4:8])
	if version != idxVersion2 {
		return nil, fmt.Errorf("idx version %d: %w", version, ErrUnsupportedVersion)
	}

	// Verify trailing .idx checksum.
	body := data[:len(data)-oidSize]
	expectedSum := algo.Sum(body)
	actualSum, err := OIDFromBytes(data[len(data)-oidSize:])
	if err != nil {
		return nil, fmt.Errorf("decode idx checksum: %w", ErrChecksumMismatch)
	}
	if expectedSum != actualSum {
		return nil, fmt.Errorf("idx checksum %s != %s: %w", actualSum, expectedSum, ErrChecksumMismatch)
	}

	packChecksum, err := OIDFromBytes(data[len(data)-2*oidSize : len(data)-oidSize])
	if err != nil {
		return nil, fmt.Errorf("decode pack checksum: %w", ErrInvalidIdx)
	}
	idx := &Idx{
		algo:         algo,
		packChecksum: packChecksum,
		idxChecksum:  actualSum,
	}

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
	oidEnd := pos + count*oidSize
	crcEnd := oidEnd + count*4
	offEnd := crcEnd + count*4
	trailerStart := len(data) - 2*oidSize
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
		oid, err := OIDFromBytes(data[pos+i*oidSize : pos+(i+1)*oidSize])
		if err != nil {
			return nil, fmt.Errorf("invalid oid at index %d: %w", i, ErrInvalidIdx)
		}
		idx.oids[i] = oid
		if i > 0 && idx.oids[i-1].Compare(idx.oids[i]) >= 0 {
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

// Algorithm returns the HashAlgorithm used by this .idx file.
func (x *Idx) Algorithm() HashAlgorithm {
	return DefaultHashAlgorithm(x.algo)
}

// Count returns the number of indexed objects.
func (x *Idx) Count() uint32 {
	return x.fanout[255]
}

// PackChecksum returns the checksum of the associated packfile.
func (x *Idx) PackChecksum() OID {
	return x.packChecksum
}

// IdxChecksum returns the checksum of the .idx file itself.
func (x *Idx) IdxChecksum() OID {
	return x.idxChecksum
}

// Lookup searches the index for oid and returns its packfile byte offset and entry CRC32.
func (x *Idx) Lookup(oid OID) (uint64, uint32, bool) {
	if oid.Len() != x.Algorithm().Size() {
		return 0, 0, false
	}
	first := oid.FirstByte()
	var lo int
	if first > 0 {
		lo = int(x.fanout[first-1])
	}
	hi := int(x.fanout[first])
	if lo >= hi {
		return 0, 0, false
	}

	rel := sort.Search(hi-lo, func(i int) bool {
		return x.oids[lo+i].Compare(oid) >= 0
	})
	pos := lo + rel
	if pos < hi && x.oids[pos] == oid {
		return x.offsets[pos], x.crcs[pos], true
	}
	return 0, 0, false
}
