package pack

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
)

// ThinPackCompleter is a Strategy interface for completing thin packfiles
// (packs containing REF_DELTA entries whose base objects live outside the pack).
type ThinPackCompleter interface {
	Complete(ctx context.Context, thinPack []byte, resolver BaseResolver) ([]byte, error)
}

// AlgorithmAwareThinPackCompleter is an optional extension of ThinPackCompleter
// that accepts an explicit HashAlgorithm (SHA-1 or SHA-256).
type AlgorithmAwareThinPackCompleter interface {
	CompleteWithAlgorithm(ctx context.Context, thinPack []byte, resolver BaseResolver, algo HashAlgorithm) ([]byte, error)
}

// PureGoThinCompleter implements ThinPackCompleter and AlgorithmAwareThinPackCompleter
// in pure Go by resolving external REF_DELTA bases via BaseResolver and appending
// them as base entries before updating the object count header and recomputing
// the trailing pack checksum.
type PureGoThinCompleter struct {
	limits Limits
	algo   HashAlgorithm
}

var (
	_ ThinPackCompleter               = (*PureGoThinCompleter)(nil)
	_ AlgorithmAwareThinPackCompleter = (*PureGoThinCompleter)(nil)
)

// NewPureGoThinCompleter creates a PureGoThinCompleter with the given resource limits (defaulting to SHA-1).
func NewPureGoThinCompleter(limits Limits) *PureGoThinCompleter {
	return &PureGoThinCompleter{limits: limits.withDefaults(), algo: SHA1}
}

// NewPureGoThinCompleterWithAlgorithm creates a PureGoThinCompleter with explicit limits and HashAlgorithm.
func NewPureGoThinCompleterWithAlgorithm(limits Limits, algo HashAlgorithm) *PureGoThinCompleter {
	return &PureGoThinCompleter{limits: limits.withDefaults(), algo: DefaultHashAlgorithm(algo)}
}

// Complete validates thinPack, resolves any missing external REF_DELTA base objects
// using resolver, and returns a self-contained packfile v2.
func (c *PureGoThinCompleter) Complete(ctx context.Context, thinPack []byte, resolver BaseResolver) ([]byte, error) {
	return c.CompleteWithAlgorithm(ctx, thinPack, resolver, c.algo)
}

// CompleteWithAlgorithm validates thinPack using algo (defaulting to SHA-1), resolves
// any missing external REF_DELTA base objects via resolver, and returns a self-contained packfile v2.
func (c *PureGoThinCompleter) CompleteWithAlgorithm(ctx context.Context, thinPack []byte, resolver BaseResolver, algo HashAlgorithm) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return CompleteThinPackWithAlgorithm(thinPack, resolver, c.limits, algo)
}

type trackingBaseResolver struct {
	inner    BaseResolver
	algo     HashAlgorithm
	seen     map[OID]struct{}
	appended []Object
}

func (t *trackingBaseResolver) ResolveBase(oid OID) (ObjectType, []byte, error) {
	typ, data, err := t.inner.ResolveBase(oid)
	if err != nil {
		return 0, nil, err
	}
	if !typ.IsBase() {
		return 0, nil, fmt.Errorf("base %s has non-base type %s: %w", oid, typ, ErrInvalidObjectType)
	}
	if ComputeOIDWith(t.algo, typ, data) != oid {
		return 0, nil, fmt.Errorf("base %s checksum mismatch: %w", oid, ErrChecksumMismatch)
	}
	if _, ok := t.seen[oid]; !ok {
		t.seen[oid] = struct{}{}
		t.appended = append(t.appended, Object{
			OID:  oid,
			Type: typ,
			Data: append([]byte(nil), data...),
		})
	}
	return typ, data, nil
}

// CompleteThinPack validates thinPack (SHA-1), resolves any external REF_DELTA base objects
// via resolver, appends them to the pack, updates the object count header, and
// recomputes the trailing SHA-1 checksum so the resulting pack is self-contained.
func CompleteThinPack(thinPack []byte, resolver BaseResolver, limits Limits) ([]byte, error) {
	return CompleteThinPackWithAlgorithm(thinPack, resolver, limits, SHA1)
}

// CompleteThinPackWithAlgorithm validates thinPack using algo (SHA-1 or SHA-256),
// resolves external REF_DELTA bases via resolver, appends them, updates the header
// object count, and recomputes the trailing checksum.
func CompleteThinPackWithAlgorithm(thinPack []byte, resolver BaseResolver, limits Limits, algo HashAlgorithm) ([]byte, error) {
	limits = limits.withDefaults()
	algo = DefaultHashAlgorithm(algo)
	trailerSize := algo.Size()

	rawEntries, checksum, err := parseRawEntries(bytes.NewReader(thinPack), limits, algo)
	if err != nil {
		return nil, err
	}

	var tracker *trackingBaseResolver
	var activeResolver BaseResolver
	if resolver != nil {
		tracker = &trackingBaseResolver{
			inner: resolver,
			algo:  algo,
			seen:  make(map[OID]struct{}),
		}
		activeResolver = tracker
	}

	if _, err := resolveEntries(rawEntries, checksum, limits, algo, activeResolver); err != nil {
		return nil, err
	}

	if tracker == nil || len(tracker.appended) == 0 {
		out := make([]byte, len(thinPack))
		copy(out, thinPack)
		return out, nil
	}

	totalCount := uint64(len(rawEntries)) + uint64(len(tracker.appended))
	if totalCount > uint64(limits.MaxObjects) {
		return nil, fmt.Errorf("completed object count %d > max %d: %w", totalCount, limits.MaxObjects, ErrLimitExceeded)
	}

	bodyLen := len(thinPack) - trailerSize
	var buf bytes.Buffer
	buf.Grow(bodyLen + trailerSize + len(tracker.appended)*64)
	buf.Write(thinPack[:bodyLen])

	// Patch 4-byte big-endian object count in header.
	binary.BigEndian.PutUint32(buf.Bytes()[8:12], uint32(totalCount))

	for _, base := range tracker.appended {
		if err := writeBasePackEntry(&buf, base.Type, base.Data); err != nil {
			return nil, fmt.Errorf("append base %s: %w", base.OID, err)
		}
	}

	if limits.MaxPackSize > 0 && int64(buf.Len()+trailerSize) > limits.MaxPackSize {
		return nil, fmt.Errorf("completed pack size exceeds %d bytes: %w", limits.MaxPackSize, ErrLimitExceeded)
	}

	newSum := algo.Sum(buf.Bytes())
	buf.Write(newSum.Bytes())
	return buf.Bytes(), nil
}

func writeBasePackEntry(buf *bytes.Buffer, typ ObjectType, data []byte) error {
	size := int64(len(data))
	b := ((uint8(typ) & 0x07) << 4) | byte(size&0x0f)
	size >>= 4
	for size > 0 {
		buf.WriteByte(b | 0x80)
		b = byte(size & 0x7f)
		size >>= 7
	}
	buf.WriteByte(b)

	zw := zlib.NewWriter(buf)
	if _, err := zw.Write(data); err != nil {
		_ = zw.Close()
		return err
	}
	return zw.Close()
}
