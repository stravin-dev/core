package compaction

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // Git packfile v2 trailer requires SHA-1 per Git specification
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"stravin.dev/core/internal/pack"
)

// Tier0Strategy implements fast pure-Go LSM Tier-0 compaction: it downloads
// recent small ACTIVE packs from S3, parses and deduplicates their objects in
// memory, writes a consolidated self-contained Git packfile v2 + .idx v2
// without expensive delta recompression, and uploads both to S3.
type Tier0Strategy struct {
	store  PackObjectStore
	limits pack.Limits
}

// NewTier0Strategy constructs a Tier-0 streaming pack merge strategy.
func NewTier0Strategy(store PackObjectStore, limits pack.Limits) (*Tier0Strategy, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil object store", ErrInvalidConfig)
	}
	return &Tier0Strategy{
		store:  store,
		limits: limits,
	}, nil
}

// Tier returns Tier0.
func (s *Tier0Strategy) Tier() Tier {
	return Tier0
}

// Compact merges all input packs into a single consolidated .pack + .idx and
// uploads them to S3 before returning.
func (s *Tier0Strategy) Compact(ctx context.Context, req StrategyRequest) (StrategyResult, error) {
	if len(req.Inputs) == 0 {
		return StrategyResult{}, ErrNothingToCompact
	}

	objects := make(map[pack.OID]*pack.Object)
	for _, in := range req.Inputs {
		if err := ctx.Err(); err != nil {
			return StrategyResult{}, err
		}
		rawPack, err := s.store.GetPack(ctx, req.Repo, in.PackID)
		if err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier0: get pack %s: %w", in.PackID, err)
		}
		parsed, err := pack.Parse(bytes.NewReader(rawPack), pack.ParseOptions{
			Limits:       s.limits,
			BaseResolver: mapObjectResolver(objects),
		})
		if err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier0: parse pack %s: %w", in.PackID, err)
		}
		for oid, obj := range parsed.Objects {
			if _, exists := objects[oid]; !exists {
				objects[oid] = obj
			}
		}
	}

	mergedPack, err := buildConsolidatedPack(objects)
	if err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier0: build merged pack: %w", err)
	}

	parsedMerged, err := pack.Parse(bytes.NewReader(mergedPack), pack.ParseOptions{
		Limits: s.limits,
	})
	if err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier0: verify merged pack: %w", err)
	}

	idxBytes, err := pack.BuildIdx(parsedMerged)
	if err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier0: build idx: %w", err)
	}

	packSHA256 := sha256Hex(mergedPack)
	if err := s.store.PutPack(ctx, req.Repo, packSHA256, mergedPack, idxBytes); err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier0: upload merged pack %s: %w", packSHA256, err)
	}

	return StrategyResult{
		OutputPackID: packSHA256,
		OutputSize:   int64(len(mergedPack)),
	}, nil
}

type mapObjectResolver map[pack.OID]*pack.Object

func (m mapObjectResolver) ResolveBase(oid pack.OID) (pack.ObjectType, []byte, error) {
	obj, ok := m[oid]
	if !ok {
		return 0, nil, fmt.Errorf("%s: %w", oid, pack.ErrObjectNotFound)
	}
	return obj.Type, obj.Data, nil
}

// buildConsolidatedPack serializes a deterministic Git packfile v2 containing
// all deduplicated base objects sorted by OID.
func buildConsolidatedPack(objects map[pack.OID]*pack.Object) ([]byte, error) {
	oids := make([]pack.OID, 0, len(objects))
	for oid := range objects {
		oids = append(oids, oid)
	}
	sort.Slice(oids, func(i, j int) bool {
		return bytes.Compare(oids[i][:], oids[j][:]) < 0
	})

	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(oids)))

	for _, oid := range oids {
		obj := objects[oid]
		if err := writePackObjectEntry(&buf, obj.Type, obj.Data); err != nil {
			return nil, fmt.Errorf("write object %s: %w", oid, err)
		}
	}

	sum := sha1.Sum(buf.Bytes()) //nolint:gosec // Git packfile v2 trailer requires SHA-1
	buf.Write(sum[:])
	return buf.Bytes(), nil
}

func writePackObjectEntry(dst *bytes.Buffer, typ pack.ObjectType, data []byte) error {
	size := uint64(len(data))
	first := ((uint8(typ) & 0x07) << 4) | byte(size&0x0f)
	size >>= 4
	if size > 0 {
		first |= 0x80
	}
	dst.WriteByte(first)
	for size > 0 {
		b := byte(size & 0x7f)
		size >>= 7
		if size > 0 {
			b |= 0x80
		}
		dst.WriteByte(b)
	}

	zw := zlib.NewWriter(dst)
	if _, err := zw.Write(data); err != nil {
		_ = zw.Close()
		return err
	}
	return zw.Close()
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
