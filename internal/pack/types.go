package pack

import (
	"crypto/sha1" //nolint:gosec // Git packfile v2 and object IDs require SHA-1 per Git specification
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// OID is a 20-byte Git SHA-1 object identifier.
type OID [20]byte

// String returns the lowercase 40-character hexadecimal representation of the OID.
func (o OID) String() string {
	return hex.EncodeToString(o[:])
}

// IsZero reports whether the OID is all zeros (Git's null OID).
func (o OID) IsZero() bool {
	return o == OID{}
}

// ParseOID parses a 40-character hexadecimal Git object ID.
func ParseOID(s string) (OID, error) {
	var oid OID
	if len(s) != 40 {
		return oid, fmt.Errorf("invalid oid length %d: %w", len(s), ErrCorruptEntry)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return oid, fmt.Errorf("decode oid %q: %w", s, err)
	}
	copy(oid[:], b)
	return oid, nil
}

// ObjectType identifies the Git packfile v2 object type (3-bit field in entry header).
type ObjectType uint8

const (
	ObjCommit   ObjectType = 1
	ObjTree     ObjectType = 2
	ObjBlob     ObjectType = 3
	ObjTag      ObjectType = 4
	ObjOFSDelta ObjectType = 6
	ObjRefDelta ObjectType = 7
)

// String returns the canonical Git object type name.
func (t ObjectType) String() string {
	switch t {
	case ObjCommit:
		return "commit"
	case ObjTree:
		return "tree"
	case ObjBlob:
		return "blob"
	case ObjTag:
		return "tag"
	case ObjOFSDelta:
		return "ofs_delta"
	case ObjRefDelta:
		return "ref_delta"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// IsBase reports whether the type is a non-delta Git object (commit, tree, blob, tag).
func (t ObjectType) IsBase() bool {
	return t >= ObjCommit && t <= ObjTag
}

// IsDelta reports whether the type is a delta object (OFS_DELTA or REF_DELTA).
func (t ObjectType) IsDelta() bool {
	return t == ObjOFSDelta || t == ObjRefDelta
}

// ComputeOID computes the standard Git SHA-1 OID for a base object ("<type> <size>\x00<data>").
func ComputeOID(typ ObjectType, data []byte) OID {
	h := sha1.New() //nolint:gosec // Git object IDs require SHA-1 per Git specification
	header := typ.String() + " " + strconv.Itoa(len(data)) + "\x00"
	_, _ = h.Write([]byte(header))
	_, _ = h.Write(data)
	var oid OID
	h.Sum(oid[:0])
	return oid
}

// Sentinel errors returned by packfile parsing, thin-pack completion, indexing, and object reads.
var (
	ErrInvalidHeader        = errors.New("pack: invalid header")
	ErrUnsupportedVersion   = errors.New("pack: unsupported version")
	ErrChecksumMismatch     = errors.New("pack: checksum mismatch")
	ErrCorruptEntry         = errors.New("pack: corrupt entry")
	ErrDeltaBomb            = errors.New("pack: delta depth or expansion limit exceeded")
	ErrMissingBase          = errors.New("pack: missing delta base object")
	ErrObjectNotFound       = errors.New("pack: object not found")
	ErrInvalidObjectType    = errors.New("pack: invalid object type")
	ErrLimitExceeded        = errors.New("pack: resource limit exceeded")
	ErrMaxTreeDepthExceeded = errors.New("pack: maximum tree recursion depth exceeded")
	ErrInvalidIdx           = errors.New("pack: invalid idx file")
)

// Limits bounds packfile parsing and delta expansion to resist delta bombs and memory exhaustion.
type Limits struct {
	MaxObjects           uint32
	MaxObjectSize        int64
	MaxTotalUncompressed int64
	MaxDeltaDepth        int
	MaxPackSize          int64
}

// DefaultLimits returns conservative production bounds for incoming push packfiles.
func DefaultLimits() Limits {
	return Limits{
		MaxObjects:           500_000,
		MaxObjectSize:        64 << 20,  // 64 MiB
		MaxTotalUncompressed: 512 << 20, // 512 MiB
		MaxDeltaDepth:        50,
		MaxPackSize:          512 << 20, // 512 MiB
	}
}

func (l Limits) withDefaults() Limits {
	def := DefaultLimits()
	if l.MaxObjects == 0 {
		l.MaxObjects = def.MaxObjects
	}
	if l.MaxObjectSize == 0 {
		l.MaxObjectSize = def.MaxObjectSize
	}
	if l.MaxTotalUncompressed == 0 {
		l.MaxTotalUncompressed = def.MaxTotalUncompressed
	}
	if l.MaxDeltaDepth == 0 {
		l.MaxDeltaDepth = def.MaxDeltaDepth
	}
	if l.MaxPackSize == 0 {
		l.MaxPackSize = def.MaxPackSize
	}
	return l
}

// BaseResolver is a Strategy interface for resolving external base objects referenced
// by REF_DELTA entries in thin packs or fallback reads for ObjectReader.
type BaseResolver interface {
	ResolveBase(oid OID) (ObjectType, []byte, error)
}

// MapBaseResolver is an in-memory BaseResolver backed by a map of OIDs to Objects.
type MapBaseResolver map[OID]Object

// ResolveBase looks up an object in the map.
func (m MapBaseResolver) ResolveBase(oid OID) (ObjectType, []byte, error) {
	obj, ok := m[oid]
	if !ok {
		return 0, nil, fmt.Errorf("%s: %w", oid, ErrObjectNotFound)
	}
	return obj.Type, obj.Data, nil
}

// FuncBaseResolver adapts a function to the BaseResolver interface.
type FuncBaseResolver func(oid OID) (ObjectType, []byte, error)

// ResolveBase invokes the underlying function.
func (f FuncBaseResolver) ResolveBase(oid OID) (ObjectType, []byte, error) {
	return f(oid)
}

// Entry holds the packfile offset, CRC32, and resolved metadata for a single packfile entry.
type Entry struct {
	OID    OID
	Type   ObjectType
	Offset uint64
	Size   int64
	CRC32  uint32
}

// Object is a fully resolved Git object held in memory.
type Object struct {
	OID  OID
	Type ObjectType
	Data []byte
}
