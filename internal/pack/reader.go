package pack

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	defaultCommitWalkLimit = 1000
	maxAncestorScanLimit   = 10_000
	treeDirMode            = 0o040000
)

// ObjectReader is the read-only Git object inspection contract consumed by HookHostABI (#9)
// and stravin-node (#11) without forking a git subprocess.
type ObjectReader interface {
	ReadObject(oid OID) (ObjectType, []byte, error)
	ListNewCommits(oldOID, newOID OID, limit uint32) ([]CommitMeta, error)
	DiffTree(oldTreeOID, newTreeOID OID, limit uint32) ([]TreeChange, error)
	ReadBlobChunk(blobOID OID, offset uint64, maxBytes uint32) ([]byte, error)
}

// Signature represents a parsed Git author or committer identity line.
type Signature struct {
	Name  string
	Email string
	When  time.Time
	Raw   string
}

// CommitMeta holds header fields and message parsed from a Git commit object.
type CommitMeta struct {
	OID       OID
	TreeOID   OID
	Parents   []OID
	Author    Signature
	Committer Signature
	Message   string
}

// TreeEntry represents a single entry inside a Git tree object.
type TreeEntry struct {
	Mode uint32
	Name string
	OID  OID
}

// IsDir reports whether the tree entry points to a subtree (mode 040000).
func (e TreeEntry) IsDir() bool {
	return e.Mode == treeDirMode
}

// ChangeKind classifies a file-level difference between two Git trees.
type ChangeKind string

const (
	ChangeAdd    ChangeKind = "add"
	ChangeModify ChangeKind = "modify"
	ChangeDelete ChangeKind = "delete"
)

// TreeChange describes a single changed path between two Git trees.
type TreeChange struct {
	Path    string
	Kind    ChangeKind
	OldMode uint32
	NewMode uint32
	OldOID  OID
	NewOID  OID
	OldSize int64
	NewSize int64
}

// PackReader implements ObjectReader over an in-memory ParsedPack with an optional
// BaseResolver fallback for objects residing in older repository packs.
type PackReader struct {
	pack     *ParsedPack
	fallback BaseResolver
}

// NewPackReader creates a read-only PackReader backed by pack and optional fallback.
func NewPackReader(pack *ParsedPack, fallback BaseResolver) *PackReader {
	return &PackReader{pack: pack, fallback: fallback}
}

// ReadObject retrieves a Git object by OID from the pack or fallback store.
func (r *PackReader) ReadObject(oid OID) (ObjectType, []byte, error) {
	if r.pack != nil {
		if obj, ok := r.pack.Objects[oid]; ok {
			return obj.Type, obj.Data, nil
		}
	}
	if r.fallback != nil {
		return r.fallback.ResolveBase(oid)
	}
	return 0, nil, fmt.Errorf("%s: %w", oid, ErrObjectNotFound)
}

// ListNewCommits walks commits reachable from newOID but not from oldOID, up to limit.
func (r *PackReader) ListNewCommits(oldOID, newOID OID, limit uint32) ([]CommitMeta, error) {
	if newOID.IsZero() || oldOID == newOID {
		return nil, nil
	}
	maxCommits := int(limit)
	if maxCommits == 0 {
		maxCommits = defaultCommitWalkLimit
	}

	stopSet := make(map[OID]struct{})
	if !oldOID.IsZero() {
		stopSet[oldOID] = struct{}{}
		r.collectAncestors(oldOID, stopSet, maxAncestorScanLimit)
	}

	visited := make(map[OID]struct{})
	queue := []OID{newOID}
	out := make([]CommitMeta, 0, min(maxCommits, 16))

	for len(queue) > 0 && len(out) < maxCommits {
		curr := queue[0]
		queue = queue[1:]

		if curr.IsZero() {
			continue
		}
		if _, stop := stopSet[curr]; stop {
			continue
		}
		if _, seen := visited[curr]; seen {
			continue
		}
		visited[curr] = struct{}{}

		typ, raw, err := r.ReadObject(curr)
		if err != nil {
			// If we hit a boundary parent that lives outside the incoming pack and no fallback is configured,
			// stop walking that branch once we have already resolved at least the tip commit.
			if len(out) > 0 && curr != newOID {
				continue
			}
			return nil, fmt.Errorf("read commit %s: %w", curr, err)
		}
		if typ != ObjCommit {
			return nil, fmt.Errorf("object %s is %s, want commit: %w", curr, typ, ErrInvalidObjectType)
		}

		meta, err := ParseCommit(curr, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
		queue = append(queue, meta.Parents...)
	}

	return out, nil
}

func (r *PackReader) collectAncestors(root OID, dst map[OID]struct{}, maxScan int) {
	queue := []OID{root}
	scanned := 0
	for len(queue) > 0 && scanned < maxScan {
		curr := queue[0]
		queue = queue[1:]
		typ, raw, err := r.ReadObject(curr)
		if err != nil || typ != ObjCommit {
			continue
		}
		meta, err := ParseCommit(curr, raw)
		if err != nil {
			continue
		}
		scanned++
		for _, p := range meta.Parents {
			if _, ok := dst[p]; !ok {
				dst[p] = struct{}{}
				queue = append(queue, p)
			}
		}
	}
}

// DiffTree recursively diffs two Git tree objects and returns up to limit file-level changes.
func (r *PackReader) DiffTree(oldTreeOID, newTreeOID OID, limit uint32) ([]TreeChange, error) {
	if oldTreeOID == newTreeOID {
		return nil, nil
	}
	var out []TreeChange
	if err := r.diffTreeRecursive("", oldTreeOID, newTreeOID, int(limit), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *PackReader) diffTreeRecursive(prefix string, oldTreeOID, newTreeOID OID, limit int, out *[]TreeChange) error {
	if limit > 0 && len(*out) >= limit {
		return nil
	}
	if oldTreeOID == newTreeOID {
		return nil
	}

	oldEntries, err := r.loadTreeEntries(oldTreeOID)
	if err != nil {
		return err
	}
	newEntries, err := r.loadTreeEntries(newTreeOID)
	if err != nil {
		return err
	}

	oldByName := make(map[string]TreeEntry, len(oldEntries))
	for _, e := range oldEntries {
		oldByName[e.Name] = e
	}
	newByName := make(map[string]TreeEntry, len(newEntries))
	for _, e := range newEntries {
		newByName[e.Name] = e
	}

	// Process entries present in newTree (additions and modifications).
	for _, ne := range newEntries {
		if limit > 0 && len(*out) >= limit {
			return nil
		}
		path := joinTreePath(prefix, ne.Name)
		oe, existed := oldByName[ne.Name]
		if !existed {
			if ne.IsDir() {
				if err := r.diffTreeRecursive(path, OID{}, ne.OID, limit, out); err != nil {
					return err
				}
				continue
			}
			*out = append(*out, TreeChange{
				Path:    path,
				Kind:    ChangeAdd,
				NewMode: ne.Mode,
				NewOID:  ne.OID,
				NewSize: r.blobSize(ne.OID),
			})
			continue
		}

		if oe.OID == ne.OID && oe.Mode == ne.Mode {
			continue
		}

		switch {
		case oe.IsDir() && ne.IsDir():
			if err := r.diffTreeRecursive(path, oe.OID, ne.OID, limit, out); err != nil {
				return err
			}
		case oe.IsDir() && !ne.IsDir():
			if err := r.diffTreeRecursive(path, oe.OID, OID{}, limit, out); err != nil {
				return err
			}
			if limit == 0 || len(*out) < limit {
				*out = append(*out, TreeChange{
					Path:    path,
					Kind:    ChangeAdd,
					NewMode: ne.Mode,
					NewOID:  ne.OID,
					NewSize: r.blobSize(ne.OID),
				})
			}
		case !oe.IsDir() && ne.IsDir():
			*out = append(*out, TreeChange{
				Path:    path,
				Kind:    ChangeDelete,
				OldMode: oe.Mode,
				OldOID:  oe.OID,
				OldSize: r.blobSize(oe.OID),
			})
			if err := r.diffTreeRecursive(path, OID{}, ne.OID, limit, out); err != nil {
				return err
			}
		default:
			*out = append(*out, TreeChange{
				Path:    path,
				Kind:    ChangeModify,
				OldMode: oe.Mode,
				NewMode: ne.Mode,
				OldOID:  oe.OID,
				NewOID:  ne.OID,
				OldSize: r.blobSize(oe.OID),
				NewSize: r.blobSize(ne.OID),
			})
		}
	}

	// Process entries deleted from oldTree.
	for _, oe := range oldEntries {
		if limit > 0 && len(*out) >= limit {
			return nil
		}
		if _, exists := newByName[oe.Name]; exists {
			continue
		}
		path := joinTreePath(prefix, oe.Name)
		if oe.IsDir() {
			if err := r.diffTreeRecursive(path, oe.OID, OID{}, limit, out); err != nil {
				return err
			}
			continue
		}
		*out = append(*out, TreeChange{
			Path:    path,
			Kind:    ChangeDelete,
			OldMode: oe.Mode,
			OldOID:  oe.OID,
			OldSize: r.blobSize(oe.OID),
		})
	}

	return nil
}

func (r *PackReader) loadTreeEntries(treeOID OID) ([]TreeEntry, error) {
	if treeOID.IsZero() {
		return nil, nil
	}
	typ, raw, err := r.ReadObject(treeOID)
	if err != nil {
		return nil, fmt.Errorf("read tree %s: %w", treeOID, err)
	}
	if typ != ObjTree {
		return nil, fmt.Errorf("object %s is %s, want tree: %w", treeOID, typ, ErrInvalidObjectType)
	}
	return ParseTree(raw)
}

func (r *PackReader) blobSize(oid OID) int64 {
	typ, data, err := r.ReadObject(oid)
	if err != nil || typ != ObjBlob {
		return 0
	}
	return int64(len(data))
}

func joinTreePath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// ReadBlobChunk returns up to maxBytes of blobOID starting at offset.
// It returns an empty slice when offset is at or past the end of the blob.
func (r *PackReader) ReadBlobChunk(blobOID OID, offset uint64, maxBytes uint32) ([]byte, error) {
	typ, data, err := r.ReadObject(blobOID)
	if err != nil {
		return nil, err
	}
	if typ != ObjBlob {
		return nil, fmt.Errorf("object %s is %s, want blob: %w", blobOID, typ, ErrInvalidObjectType)
	}
	if offset >= uint64(len(data)) || maxBytes == 0 {
		return []byte{}, nil
	}
	end := offset + uint64(maxBytes)
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	out := make([]byte, end-offset)
	copy(out, data[offset:end])
	return out, nil
}

// ParseCommit parses a raw Git commit object payload into CommitMeta.
func ParseCommit(oid OID, raw []byte) (CommitMeta, error) {
	meta := CommitMeta{OID: oid}
	sep := bytes.Index(raw, []byte("\n\n"))
	var headerBlock []byte
	if sep >= 0 {
		headerBlock = raw[:sep]
		meta.Message = string(raw[sep+2:])
	} else {
		headerBlock = raw
	}

	for _, line := range strings.Split(string(headerBlock), "\n") {
		if line == "" {
			continue
		}
		key, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		switch key {
		case "tree":
			tOID, err := ParseOID(strings.TrimSpace(val))
			if err != nil {
				return CommitMeta{}, fmt.Errorf("commit %s tree: %w", oid, err)
			}
			meta.TreeOID = tOID
		case "parent":
			pOID, err := ParseOID(strings.TrimSpace(val))
			if err != nil {
				return CommitMeta{}, fmt.Errorf("commit %s parent: %w", oid, err)
			}
			meta.Parents = append(meta.Parents, pOID)
		case "author":
			meta.Author = parseSignature(val)
		case "committer":
			meta.Committer = parseSignature(val)
		}
	}

	if meta.TreeOID.IsZero() {
		return CommitMeta{}, fmt.Errorf("commit %s missing tree header: %w", oid, ErrCorruptEntry)
	}
	return meta, nil
}

func parseSignature(raw string) Signature {
	sig := Signature{Raw: raw}
	lt := strings.IndexByte(raw, '<')
	gt := strings.IndexByte(raw, '>')
	if lt >= 0 && gt > lt {
		sig.Name = strings.TrimSpace(raw[:lt])
		sig.Email = strings.TrimSpace(raw[lt+1 : gt])
		rest := strings.Fields(strings.TrimSpace(raw[gt+1:]))
		if len(rest) >= 1 {
			if sec, err := strconv.ParseInt(rest[0], 10, 64); err == nil {
				loc := time.UTC
				if len(rest) >= 2 && len(rest[1]) == 5 {
					if tz, ok := parseTZOffset(rest[1]); ok {
						loc = tz
					}
				}
				sig.When = time.Unix(sec, 0).In(loc)
			}
		}
	}
	return sig
}

func parseTZOffset(s string) (*time.Location, bool) {
	var sign int
	switch s[0] {
	case '+':
		sign = 1
	case '-':
		sign = -1
	default:
		return nil, false
	}
	hours, err1 := strconv.Atoi(s[1:3])
	mins, err2 := strconv.Atoi(s[3:5])
	if err1 != nil || err2 != nil {
		return nil, false
	}
	return time.FixedZone(s, sign*(hours*3600+mins*60)), true
}

// ParseTree parses a raw Git binary tree object payload into TreeEntry records.
func ParseTree(raw []byte) ([]TreeEntry, error) {
	var entries []TreeEntry
	pos := 0
	for pos < len(raw) {
		space := bytes.IndexByte(raw[pos:], ' ')
		if space <= 0 {
			return nil, fmt.Errorf("tree entry missing mode separator: %w", ErrCorruptEntry)
		}
		modeVal, err := strconv.ParseUint(string(raw[pos:pos+space]), 8, 32)
		if err != nil {
			return nil, fmt.Errorf("tree entry invalid mode: %w", ErrCorruptEntry)
		}
		pos += space + 1

		nul := bytes.IndexByte(raw[pos:], 0)
		if nul <= 0 {
			return nil, fmt.Errorf("tree entry missing name terminator: %w", ErrCorruptEntry)
		}
		name := string(raw[pos : pos+nul])
		pos += nul + 1

		if pos+20 > len(raw) {
			return nil, fmt.Errorf("tree entry truncated OID: %w", ErrCorruptEntry)
		}
		var oid OID
		copy(oid[:], raw[pos:pos+20])
		pos += 20

		entries = append(entries, TreeEntry{
			Mode: uint32(modeVal),
			Name: name,
			OID:  oid,
		})
	}
	return entries, nil
}
