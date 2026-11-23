package pack

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

const (
	packHeaderSize = 12
	packVersion2   = 2
)

var packMagic = [4]byte{'P', 'A', 'C', 'K'}

// ParseOptions configures packfile v2 parsing and validation.
type ParseOptions struct {
	Limits        Limits
	BaseResolver  BaseResolver
	HashAlgorithm HashAlgorithm
}

// ParsedPack holds the validated entries, resolved objects, and trailer checksum of a packfile v2.
type ParsedPack struct {
	HashAlgorithm HashAlgorithm
	PackChecksum  OID
	Entries       []Entry
	Objects       map[OID]*Object
}

// packStreamReader tracks exact byte offsets, enforces MaxPackSize, updates the
// pack-level hash, and optionally updates a per-entry CRC32.
type packStreamReader struct {
	br       *bufio.Reader
	packHash hash.Hash
	entryCRC hash.Hash32
	offset   uint64
	maxBytes int64
}

func newPackStreamReader(r io.Reader, maxBytes int64, algo HashAlgorithm) *packStreamReader {
	algo = DefaultHashAlgorithm(algo)
	return &packStreamReader{
		br:       bufio.NewReaderSize(r, 32*1024),
		packHash: algo.NewHash(),
		maxBytes: maxBytes,
	}
}

func (p *packStreamReader) startEntryCRC() {
	p.entryCRC = crc32.NewIEEE()
}

func (p *packStreamReader) finishEntryCRC() uint32 {
	if p.entryCRC == nil {
		return 0
	}
	sum := p.entryCRC.Sum32()
	p.entryCRC = nil
	return sum
}

func (p *packStreamReader) account(b []byte) error {
	p.offset += uint64(len(b))
	if p.maxBytes > 0 && int64(p.offset) > p.maxBytes {
		return fmt.Errorf("pack size exceeds %d bytes: %w", p.maxBytes, ErrLimitExceeded)
	}
	_, _ = p.packHash.Write(b)
	if p.entryCRC != nil {
		_, _ = p.entryCRC.Write(b)
	}
	return nil
}

func (p *packStreamReader) Read(buf []byte) (int, error) {
	n, err := p.br.Read(buf)
	if n > 0 {
		if accErr := p.account(buf[:n]); accErr != nil {
			return n, accErr
		}
	}
	return n, err
}

func (p *packStreamReader) ReadByte() (byte, error) {
	b, err := p.br.ReadByte()
	if err != nil {
		return 0, err
	}
	var one [1]byte
	one[0] = b
	if accErr := p.account(one[:]); accErr != nil {
		return 0, accErr
	}
	return b, nil
}

func (p *packStreamReader) readFullHashed(buf []byte) error {
	if _, err := io.ReadFull(p.br, buf); err != nil {
		return err
	}
	return p.account(buf)
}

type rawParsedEntry struct {
	offset     uint64
	typ        ObjectType
	declaredSz int64
	crc32      uint32
	baseOffset uint64 // for ObjOFSDelta
	baseOID    OID    // for ObjRefDelta
	payload    []byte // decompressed bytes (raw object or delta stream)
}

// Parse streams, validates, and resolves a Git packfile v2 stream.
// It validates the PACK header, version 2, object count, zlib payloads, delta chains,
// bounded resource limits, and trailing hash checksum (SHA-1 by default, or opts.HashAlgorithm).
func Parse(r io.Reader, opts ParseOptions) (*ParsedPack, error) {
	limits := opts.Limits.withDefaults()
	algo := DefaultHashAlgorithm(opts.HashAlgorithm)
	rawEntries, packChecksum, err := parseRawEntries(r, limits, algo)
	if err != nil {
		return nil, err
	}
	return resolveEntries(rawEntries, packChecksum, limits, algo, opts.BaseResolver)
}

func parseRawEntries(r io.Reader, limits Limits, algo HashAlgorithm) ([]rawParsedEntry, OID, error) {
	algo = DefaultHashAlgorithm(algo)
	sr := newPackStreamReader(r, limits.MaxPackSize, algo)
	oidSize := algo.Size()
	oidBuf := make([]byte, oidSize)

	var hdr [packHeaderSize]byte
	if err := sr.readFullHashed(hdr[:]); err != nil {
		return nil, OID{}, fmt.Errorf("read header: %w", ErrInvalidHeader)
	}
	if !bytes.Equal(hdr[:4], packMagic[:]) {
		return nil, OID{}, fmt.Errorf("bad magic %q: %w", hdr[:4], ErrInvalidHeader)
	}
	version := binary.BigEndian.Uint32(hdr[4:8])
	if version != packVersion2 {
		return nil, OID{}, fmt.Errorf("version %d: %w", version, ErrUnsupportedVersion)
	}
	count := binary.BigEndian.Uint32(hdr[8:12])
	if count > limits.MaxObjects {
		return nil, OID{}, fmt.Errorf("object count %d > max %d: %w", count, limits.MaxObjects, ErrLimitExceeded)
	}

	entries := make([]rawParsedEntry, count)
	var totalUncompressed int64

	for i := uint32(0); i < count; i++ {
		entryOffset := sr.offset
		sr.startEntryCRC()

		typ, declaredSz, err := readEntryHeader(sr)
		if err != nil {
			return nil, OID{}, fmt.Errorf("entry %d header: %w", i, err)
		}
		if !typ.IsBase() && !typ.IsDelta() {
			return nil, OID{}, fmt.Errorf("entry %d type %d: %w", i, typ, ErrInvalidObjectType)
		}
		if declaredSz > limits.MaxObjectSize {
			return nil, OID{}, fmt.Errorf("entry %d size %d > max %d: %w", i, declaredSz, limits.MaxObjectSize, ErrLimitExceeded)
		}

		re := rawParsedEntry{
			offset:     entryOffset,
			typ:        typ,
			declaredSz: declaredSz,
		}

		switch typ {
		case ObjOFSDelta:
			dist, err := readOFSDeltaDistance(sr)
			if err != nil {
				return nil, OID{}, fmt.Errorf("entry %d ofs_delta: %w", i, err)
			}
			if dist == 0 || dist >= entryOffset || entryOffset-dist < packHeaderSize {
				return nil, OID{}, fmt.Errorf("entry %d invalid ofs_delta distance %d at offset %d: %w", i, dist, entryOffset, ErrCorruptEntry)
			}
			re.baseOffset = entryOffset - dist
		case ObjRefDelta:
			if err := sr.readFullHashed(oidBuf); err != nil {
				return nil, OID{}, fmt.Errorf("entry %d ref_delta base oid: %w", i, ErrCorruptEntry)
			}
			baseOID, err := OIDFromBytes(oidBuf)
			if err != nil {
				return nil, OID{}, fmt.Errorf("entry %d ref_delta base oid: %w", i, err)
			}
			re.baseOID = baseOID
		}

		payload, err := readZlibPayload(sr, declaredSz, limits.MaxObjectSize)
		if err != nil {
			return nil, OID{}, fmt.Errorf("entry %d payload: %w", i, err)
		}
		totalUncompressed += int64(len(payload))
		if totalUncompressed > limits.MaxTotalUncompressed {
			return nil, OID{}, fmt.Errorf("total uncompressed bytes %d > max %d: %w", totalUncompressed, limits.MaxTotalUncompressed, ErrLimitExceeded)
		}

		re.crc32 = sr.finishEntryCRC()
		re.payload = payload
		entries[i] = re
	}

	computedChecksum, err := OIDFromBytes(sr.packHash.Sum(nil))
	if err != nil {
		return nil, OID{}, fmt.Errorf("compute trailer checksum: %w", err)
	}

	trailerBuf := make([]byte, oidSize)
	if _, err := io.ReadFull(sr.br, trailerBuf); err != nil {
		return nil, OID{}, fmt.Errorf("read trailer checksum: %w", ErrChecksumMismatch)
	}
	trailer, err := OIDFromBytes(trailerBuf)
	if err != nil {
		return nil, OID{}, fmt.Errorf("decode trailer checksum: %w", ErrChecksumMismatch)
	}
	if trailer != computedChecksum {
		return nil, OID{}, fmt.Errorf("trailer %s != computed %s: %w", trailer, computedChecksum, ErrChecksumMismatch)
	}

	// Ensure no trailing garbage follows the hash trailer.
	var extra [1]byte
	n, err := sr.br.Read(extra[:])
	if n > 0 || (err != nil && !errors.Is(err, io.EOF)) {
		return nil, OID{}, fmt.Errorf("unexpected trailing bytes after pack checksum: %w", ErrCorruptEntry)
	}

	return entries, computedChecksum, nil
}

func readEntryHeader(r io.ByteReader) (ObjectType, int64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %w", ErrCorruptEntry, err)
	}
	typ := ObjectType((b >> 4) & 0x07)
	size := int64(b & 0x0f)
	shift := uint(4)
	for b&0x80 != 0 {
		if shift >= 63 {
			return 0, 0, fmt.Errorf("entry size varint overflow: %w", ErrCorruptEntry)
		}
		b, err = r.ReadByte()
		if err != nil {
			return 0, 0, fmt.Errorf("%w: %w", ErrCorruptEntry, err)
		}
		size |= int64(b&0x7f) << shift
		shift += 7
	}
	return typ, size, nil
}

func readOFSDeltaDistance(r io.ByteReader) (uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrCorruptEntry, err)
	}
	dist := uint64(b & 0x7f)
	for b&0x80 != 0 {
		b, err = r.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("%w: %w", ErrCorruptEntry, err)
		}
		if dist >= (1 << 56) {
			return 0, fmt.Errorf("ofs_delta offset overflow: %w", ErrCorruptEntry)
		}
		dist = ((dist + 1) << 7) | uint64(b&0x7f)
	}
	return dist, nil
}

func readZlibPayload(sr *packStreamReader, declaredSz, maxObjSz int64) ([]byte, error) {
	zr, err := zlib.NewReader(sr)
	if err != nil {
		return nil, fmt.Errorf("zlib open: %w", ErrCorruptEntry)
	}
	defer func() {
		_ = zr.Close()
	}()

	capLimit := declaredSz + 1
	if maxObjSz > 0 && capLimit > maxObjSz+1 {
		capLimit = maxObjSz + 1
	}
	data, err := io.ReadAll(io.LimitReader(zr, capLimit))
	if err != nil {
		return nil, fmt.Errorf("zlib inflate: %w", ErrCorruptEntry)
	}
	if int64(len(data)) != declaredSz {
		return nil, fmt.Errorf("inflated size %d != header size %d: %w", len(data), declaredSz, ErrCorruptEntry)
	}
	return data, nil
}

func resolveEntries(raw []rawParsedEntry, checksum OID, limits Limits, algo HashAlgorithm, resolver BaseResolver) (*ParsedPack, error) {
	algo = DefaultHashAlgorithm(algo)
	n := len(raw)
	entries := make([]Entry, n)
	objects := make(map[OID]*Object, n)
	resolved := make([]bool, n)
	depth := make([]int, n)
	offsetIndex := make(map[uint64]int, n)

	var totalExpanded int64

	// Pass 1: register all offsets and materialize all non-delta base entries.
	for i := range raw {
		offsetIndex[raw[i].offset] = i
		if raw[i].typ.IsBase() {
			oid := ComputeOIDWith(algo, raw[i].typ, raw[i].payload)
			obj := &Object{OID: oid, Type: raw[i].typ, Data: raw[i].payload}
			objects[oid] = obj
			entries[i] = Entry{
				OID:    oid,
				Type:   raw[i].typ,
				Offset: raw[i].offset,
				Size:   int64(len(raw[i].payload)),
				CRC32:  raw[i].crc32,
			}
			resolved[i] = true
			totalExpanded += int64(len(raw[i].payload))
		}
	}

	// Pass 2: iteratively resolve OFS_DELTA and REF_DELTA chains (supports forward REF_DELTA).
	remaining := 0
	for i := range resolved {
		if !resolved[i] {
			remaining++
		}
	}

	externalDepth := make(map[OID]int)

	for remaining > 0 {
		progressed := false
		for i := range raw {
			if resolved[i] {
				continue
			}
			re := &raw[i]
			var baseObj *Object
			var baseDepth int

			switch re.typ {
			case ObjOFSDelta:
				baseIdx, ok := offsetIndex[re.baseOffset]
				if !ok {
					return nil, fmt.Errorf("ofs_delta at offset %d references unknown offset %d: %w", re.offset, re.baseOffset, ErrCorruptEntry)
				}
				if !resolved[baseIdx] {
					continue
				}
				baseObj = objects[entries[baseIdx].OID]
				baseDepth = depth[baseIdx]

			case ObjRefDelta:
				if obj, ok := objects[re.baseOID]; ok {
					baseObj = obj
					baseDepth = findDepthForOID(entries, resolved, depth, re.baseOID)
				} else if resolver != nil {
					bType, bData, err := resolver.ResolveBase(re.baseOID)
					if err != nil {
						if errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrInvalidObjectType) {
							return nil, fmt.Errorf("resolve external base %s: %w", re.baseOID, err)
						}
						return nil, fmt.Errorf("resolve external base %s (%w): %w", re.baseOID, err, ErrMissingBase)
					}
					if !bType.IsBase() {
						return nil, fmt.Errorf("external base %s has non-base type %s: %w", re.baseOID, bType, ErrInvalidObjectType)
					}
					if ComputeOIDWith(algo, bType, bData) != re.baseOID {
						return nil, fmt.Errorf("external base %s checksum mismatch: %w", re.baseOID, ErrChecksumMismatch)
					}
					baseObj = &Object{OID: re.baseOID, Type: bType, Data: bData}
					baseDepth = externalDepth[re.baseOID]
				} else {
					continue
				}
			}

			nextDepth := baseDepth + 1
			if nextDepth > limits.MaxDeltaDepth {
				return nil, fmt.Errorf("delta depth %d > max %d: %w", nextDepth, limits.MaxDeltaDepth, ErrDeltaBomb)
			}

			target, err := ApplyDelta(baseObj.Data, re.payload, limits.MaxObjectSize)
			if err != nil {
				return nil, fmt.Errorf("apply delta at offset %d: %w", re.offset, err)
			}
			totalExpanded += int64(len(target))
			if totalExpanded > limits.MaxTotalUncompressed {
				return nil, fmt.Errorf("expanded pack bytes %d > max %d: %w", totalExpanded, limits.MaxTotalUncompressed, ErrLimitExceeded)
			}

			oid := ComputeOIDWith(algo, baseObj.Type, target)
			objects[oid] = &Object{OID: oid, Type: baseObj.Type, Data: target}
			entries[i] = Entry{
				OID:    oid,
				Type:   baseObj.Type,
				Offset: re.offset,
				Size:   int64(len(target)),
				CRC32:  re.crc32,
			}
			depth[i] = nextDepth
			resolved[i] = true
			remaining--
			progressed = true
		}

		if !progressed {
			// Identify whether unresolved entries are missing REF_DELTA bases or cyclic deltas.
			for i := range raw {
				if !resolved[i] && raw[i].typ == ObjRefDelta {
					return nil, fmt.Errorf("unresolved ref_delta base %s: %w", raw[i].baseOID, ErrMissingBase)
				}
			}
			return nil, fmt.Errorf("cyclic or unresolvable delta chain (%d entries remaining): %w", remaining, ErrCorruptEntry)
		}
	}

	return &ParsedPack{
		HashAlgorithm: algo,
		PackChecksum:  checksum,
		Entries:       entries,
		Objects:       objects,
	}, nil
}

func findDepthForOID(entries []Entry, resolved []bool, depth []int, oid OID) int {
	for i := range entries {
		if resolved[i] && entries[i].OID == oid {
			return depth[i]
		}
	}
	return 0
}
