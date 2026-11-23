package pack

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // Git packfile v2 and object IDs require SHA-1 per Git specification
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
)

const (
	// SHA1Size is the byte length of a Git SHA-1 object ID.
	SHA1Size = 20
	// SHA256Size is the byte length of a Git SHA-256 object ID.
	SHA256Size = 32
	// MaxOIDSize is the maximum byte length of a supported Git object ID.
	MaxOIDSize = SHA256Size
	// SHA1HexSize is the hexadecimal character length of a Git SHA-1 object ID.
	SHA1HexSize = 40
	// SHA256HexSize is the hexadecimal character length of a Git SHA-256 object ID.
	SHA256HexSize = 64
)

// HashAlgorithm is the GoF Strategy interface encapsulating Git object-format
// hashing details (SHA-1 20-byte / 40-hex vs SHA-256 32-byte / 64-hex).
type HashAlgorithm interface {
	Name() string
	Size() int
	HexSize() int
	NewHash() hash.Hash
	Sum(data []byte) OID
	ZeroOID() OID
}

type sha1Algorithm struct{}

func (sha1Algorithm) Name() string       { return "sha1" }
func (sha1Algorithm) Size() int          { return SHA1Size }
func (sha1Algorithm) HexSize() int       { return SHA1HexSize }
func (sha1Algorithm) NewHash() hash.Hash { return sha1.New() } //nolint:gosec // Git SHA-1 object format requires SHA-1 per Git specification
func (sha1Algorithm) Sum(data []byte) OID {
	sum := sha1.Sum(data) //nolint:gosec // Git SHA-1 object format requires SHA-1 per Git specification
	var oid OID
	copy(oid.raw[:SHA1Size], sum[:])
	return oid
}
func (sha1Algorithm) ZeroOID() OID { return OID{} }

type sha256Algorithm struct{}

func (sha256Algorithm) Name() string       { return "sha256" }
func (sha256Algorithm) Size() int          { return SHA256Size }
func (sha256Algorithm) HexSize() int       { return SHA256HexSize }
func (sha256Algorithm) NewHash() hash.Hash { return sha256.New() }
func (sha256Algorithm) Sum(data []byte) OID {
	sum := sha256.Sum256(data)
	return OID{raw: sum, isSHA256: true}
}
func (sha256Algorithm) ZeroOID() OID { return OID{isSHA256: true} }

// SHA1 and SHA256 are the built-in Git object hash algorithm strategies.
var (
	SHA1   HashAlgorithm = sha1Algorithm{}
	SHA256 HashAlgorithm = sha256Algorithm{}
)

// DefaultHashAlgorithm returns SHA1 when algo is nil, otherwise algo.
func DefaultHashAlgorithm(algo HashAlgorithm) HashAlgorithm {
	if algo == nil {
		return SHA1
	}
	return algo
}

// HashAlgorithmForFormat resolves a Git objectFormat name ("", "sha1", "sha256")
// to its corresponding HashAlgorithm strategy.
func HashAlgorithmForFormat(format string) (HashAlgorithm, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "sha1":
		return SHA1, nil
	case "sha256":
		return SHA256, nil
	default:
		return nil, fmt.Errorf("object format %q: %w", format, ErrUnsupportedHashAlgorithm)
	}
}

// OID is an algorithm-agnostic, comparable Git object identifier supporting
// both 20-byte SHA-1 and 32-byte SHA-256 hashes.
// The zero value (OID{}) represents the 20-byte zero SHA-1 OID.
type OID struct {
	raw      [MaxOIDSize]byte
	isSHA256 bool
}

// OIDFromBytes constructs an OID from a 20-byte (SHA-1) or 32-byte (SHA-256) slice.
func OIDFromBytes(b []byte) (OID, error) {
	switch len(b) {
	case SHA1Size:
		var oid OID
		copy(oid.raw[:SHA1Size], b)
		return oid, nil
	case SHA256Size:
		oid := OID{isSHA256: true}
		copy(oid.raw[:SHA256Size], b)
		return oid, nil
	default:
		return OID{}, fmt.Errorf("invalid oid byte length %d: %w", len(b), ErrCorruptEntry)
	}
}

// OIDFromSHA1 converts a 20-byte SHA-1 array into an OID.
func OIDFromSHA1(b [SHA1Size]byte) OID {
	var oid OID
	copy(oid.raw[:SHA1Size], b[:])
	return oid
}

// OIDFromSHA256 converts a 32-byte SHA-256 array into an OID.
func OIDFromSHA256(b [SHA256Size]byte) OID {
	return OID{raw: b, isSHA256: true}
}

// Len returns the raw byte length of the OID (20 for SHA-1, 32 for SHA-256).
func (o OID) Len() int {
	if o.isSHA256 {
		return SHA256Size
	}
	return SHA1Size
}

// Algorithm returns the HashAlgorithm associated with this OID.
func (o OID) Algorithm() HashAlgorithm {
	if o.isSHA256 {
		return SHA256
	}
	return SHA1
}

// Bytes returns a copy of the raw OID bytes (20 bytes for SHA-1, 32 bytes for SHA-256).
func (o OID) Bytes() []byte {
	n := o.Len()
	out := make([]byte, n)
	copy(out, o.raw[:n])
	return out
}

// SHA1Bytes returns the 20-byte array and true if o is a SHA-1 OID.
func (o OID) SHA1Bytes() ([SHA1Size]byte, bool) {
	if o.isSHA256 {
		return [SHA1Size]byte{}, false
	}
	var out [SHA1Size]byte
	copy(out[:], o.raw[:SHA1Size])
	return out, true
}

// FirstByte returns the leading byte of the OID (used for .idx v2 fanout tables).
func (o OID) FirstByte() byte {
	return o.raw[0]
}

// Compare compares two OIDs lexicographically by their raw bytes.
func (o OID) Compare(other OID) int {
	return bytes.Compare(o.raw[:o.Len()], other.raw[:other.Len()])
}

// String returns the lowercase hexadecimal representation of the OID (40 or 64 chars).
func (o OID) String() string {
	return hex.EncodeToString(o.raw[:o.Len()])
}

// IsZero reports whether the OID is all zeros (Git's null OID for either SHA-1 or SHA-256).
func (o OID) IsZero() bool {
	return o.raw == [MaxOIDSize]byte{}
}

// ParseOID parses a 40-character (SHA-1) or 64-character (SHA-256) hexadecimal Git object ID.
func ParseOID(s string) (OID, error) {
	switch len(s) {
	case SHA1HexSize:
		return ParseOIDForAlgorithm(s, SHA1)
	case SHA256HexSize:
		return ParseOIDForAlgorithm(s, SHA256)
	default:
		return OID{}, fmt.Errorf("invalid oid length %d: %w", len(s), ErrCorruptEntry)
	}
}

// ParseOIDForAlgorithm parses a hexadecimal Git object ID strictly matching algo's hex length.
func ParseOIDForAlgorithm(s string, algo HashAlgorithm) (OID, error) {
	algo = DefaultHashAlgorithm(algo)
	if len(s) != algo.HexSize() {
		return OID{}, fmt.Errorf("invalid %s oid length %d (want %d): %w", algo.Name(), len(s), algo.HexSize(), ErrCorruptEntry)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return OID{}, fmt.Errorf("decode oid %q: %w", s, err)
	}
	return OIDFromBytes(b)
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
	return ComputeOIDWith(SHA1, typ, data)
}

// ComputeOIDWith computes the Git OID for a base object using the given HashAlgorithm strategy.
func ComputeOIDWith(algo HashAlgorithm, typ ObjectType, data []byte) OID {
	algo = DefaultHashAlgorithm(algo)
	h := algo.NewHash()
	header := typ.String() + " " + strconv.Itoa(len(data)) + "\x00"
	_, _ = h.Write([]byte(header))
	_, _ = h.Write(data)
	oid, _ := OIDFromBytes(h.Sum(nil))
	return oid
}

// Sentinel errors returned by packfile parsing, thin-pack completion, indexing, and object reads.
var (
	ErrInvalidHeader            = errors.New("pack: invalid header")
	ErrUnsupportedVersion       = errors.New("pack: unsupported version")
	ErrUnsupportedHashAlgorithm = errors.New("pack: unsupported hash algorithm")
	ErrChecksumMismatch         = errors.New("pack: checksum mismatch")
	ErrCorruptEntry             = errors.New("pack: corrupt entry")
	ErrDeltaBomb                = errors.New("pack: delta depth or expansion limit exceeded")
	ErrMissingBase              = errors.New("pack: missing delta base object")
	ErrObjectNotFound           = errors.New("pack: object not found")
	ErrInvalidObjectType        = errors.New("pack: invalid object type")
	ErrLimitExceeded            = errors.New("pack: resource limit exceeded")
	ErrMaxTreeDepthExceeded     = errors.New("pack: maximum tree recursion depth exceeded")
	ErrInvalidIdx               = errors.New("pack: invalid idx file")
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
