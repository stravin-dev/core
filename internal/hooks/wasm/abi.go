package wasm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const (
	hostModuleName         = "stravin"
	abiErrGeneric          = int32(-1)
	abiErrBufTooSmall      = int32(-2)
	abiErrBudgetExceeded   = int32(-3)
	verdictDecisionAllow   = uint32(0)
	verdictDecisionReject  = uint32(1)
	verdictDecisionJSON    = uint32(2)
	maxVerdictMessageBytes = 64 << 10
)

type invocationStateKey struct{}

// budgetedObjectReader is a GoF Proxy around ObjectReader that enforces per-invocation
// host-call, commit-walk, and blob-read budgets.
type budgetedObjectReader struct {
	inner ObjectReader

	mu            sync.Mutex
	commitsWalked uint32
	blobBytesRead uint64
	hostCalls     uint32

	maxCommits   uint32
	maxBlobBytes uint64
	maxHostCalls uint32
}

func newBudgetedObjectReader(inner ObjectReader, maxCommits uint32, maxBlobBytes uint64, maxHostCalls uint32) *budgetedObjectReader {
	return &budgetedObjectReader{
		inner:        inner,
		maxCommits:   maxCommits,
		maxBlobBytes: maxBlobBytes,
		maxHostCalls: maxHostCalls,
	}
}

func (r *budgetedObjectReader) chargeHostCall() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hostCalls >= r.maxHostCalls {
		return ErrBudgetExceeded
	}
	r.hostCalls++
	return nil
}

func (r *budgetedObjectReader) ListNewCommits(oldOID, newOID [20]byte, limit uint32) ([]CommitMeta, error) {
	if err := r.chargeHostCall(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.commitsWalked >= r.maxCommits {
		r.mu.Unlock()
		return nil, ErrBudgetExceeded
	}
	remaining := r.maxCommits - r.commitsWalked
	r.mu.Unlock()

	if r.inner == nil {
		return nil, nil
	}
	effectiveLimit := limit
	if effectiveLimit == 0 || effectiveLimit > remaining {
		effectiveLimit = remaining
	}
	commits, err := r.inner.ListNewCommits(oldOID, newOID, effectiveLimit)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.commitsWalked += uint32(len(commits))
	r.mu.Unlock()
	return commits, nil
}

func clampDiffTreeLimit(limit uint32) uint32 {
	if limit == 0 {
		return DefaultDiffTreeLimit
	}
	if limit > MaxDiffTreeLimit {
		return MaxDiffTreeLimit
	}
	return limit
}

func (r *budgetedObjectReader) DiffTree(ctx context.Context, oldTreeOID, newTreeOID [20]byte, limit uint32) ([]TreeChange, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.chargeHostCall(); err != nil {
		return nil, err
	}
	if r.inner == nil {
		return nil, nil
	}
	effectiveLimit := clampDiffTreeLimit(limit)
	if ctxReader, ok := r.inner.(ContextDiffTreeReader); ok {
		return ctxReader.DiffTreeContext(ctx, oldTreeOID, newTreeOID, effectiveLimit)
	}
	return r.inner.DiffTree(oldTreeOID, newTreeOID, effectiveLimit)
}

func (r *budgetedObjectReader) ReadBlobChunk(blobOID [20]byte, offset uint64, maxBytes uint32) ([]byte, error) {
	if err := r.chargeHostCall(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.blobBytesRead >= r.maxBlobBytes {
		r.mu.Unlock()
		return nil, ErrBudgetExceeded
	}
	remaining := r.maxBlobBytes - r.blobBytesRead
	r.mu.Unlock()

	if r.inner == nil {
		return []byte{}, nil
	}
	effectiveMax := maxBytes
	if uint64(effectiveMax) > remaining {
		effectiveMax = uint32(remaining)
	}
	chunk, err := r.inner.ReadBlobChunk(blobOID, offset, effectiveMax)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.blobBytesRead += uint64(len(chunk))
	r.mu.Unlock()
	return chunk, nil
}

func (r *budgetedObjectReader) ListNewCommitsByHex(oldOIDHex, newOIDHex string, limit uint32) ([]CommitMeta, error) {
	if err := r.chargeHostCall(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.commitsWalked >= r.maxCommits {
		r.mu.Unlock()
		return nil, ErrBudgetExceeded
	}
	remaining := r.maxCommits - r.commitsWalked
	r.mu.Unlock()

	if r.inner == nil {
		return nil, nil
	}
	effectiveLimit := limit
	if effectiveLimit == 0 || effectiveLimit > remaining {
		effectiveLimit = remaining
	}

	var (
		commits []CommitMeta
		err     error
	)
	if dual, ok := r.inner.(DualHashObjectReader); ok {
		commits, err = dual.ListNewCommitsByHex(oldOIDHex, newOIDHex, effectiveLimit)
	} else {
		oldB, err1 := decodeSHA1Hex(oldOIDHex)
		newB, err2 := decodeSHA1Hex(newOIDHex)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("hooks/wasm: reader does not support non-sha1 oids")
		}
		commits, err = r.inner.ListNewCommits(oldB, newB, effectiveLimit)
	}
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.commitsWalked += uint32(len(commits))
	r.mu.Unlock()
	return commits, nil
}

func (r *budgetedObjectReader) DiffTreeByHex(ctx context.Context, oldTreeOIDHex, newTreeOIDHex string, limit uint32) ([]TreeChange, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.chargeHostCall(); err != nil {
		return nil, err
	}
	if r.inner == nil {
		return nil, nil
	}
	effectiveLimit := clampDiffTreeLimit(limit)
	if dual, ok := r.inner.(DualHashObjectReader); ok {
		return dual.DiffTreeByHex(ctx, oldTreeOIDHex, newTreeOIDHex, effectiveLimit)
	}
	oldB, err1 := decodeSHA1Hex(oldTreeOIDHex)
	newB, err2 := decodeSHA1Hex(newTreeOIDHex)
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("hooks/wasm: reader does not support non-sha1 oids")
	}
	if ctxReader, ok := r.inner.(ContextDiffTreeReader); ok {
		return ctxReader.DiffTreeContext(ctx, oldB, newB, effectiveLimit)
	}
	return r.inner.DiffTree(oldB, newB, effectiveLimit)
}

func (r *budgetedObjectReader) ReadBlobChunkByHex(blobOIDHex string, offset uint64, maxBytes uint32) ([]byte, error) {
	if err := r.chargeHostCall(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.blobBytesRead >= r.maxBlobBytes {
		r.mu.Unlock()
		return nil, ErrBudgetExceeded
	}
	remaining := r.maxBlobBytes - r.blobBytesRead
	r.mu.Unlock()

	if r.inner == nil {
		return []byte{}, nil
	}
	effectiveMax := maxBytes
	if uint64(effectiveMax) > remaining {
		effectiveMax = uint32(remaining)
	}

	var (
		chunk []byte
		err   error
	)
	if dual, ok := r.inner.(DualHashObjectReader); ok {
		chunk, err = dual.ReadBlobChunkByHex(blobOIDHex, offset, effectiveMax)
	} else {
		blobB, decErr := decodeSHA1Hex(blobOIDHex)
		if decErr != nil {
			return nil, fmt.Errorf("hooks/wasm: reader does not support non-sha1 oids")
		}
		chunk, err = r.inner.ReadBlobChunk(blobB, offset, effectiveMax)
	}
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.blobBytesRead += uint64(len(chunk))
	r.mu.Unlock()
	return chunk, nil
}

func decodeSHA1Hex(s string) ([20]byte, error) {
	var out [20]byte
	if len(s) != 40 {
		return out, errors.New("not a 40-char sha1 hex oid")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

type invocationState struct {
	pushCtx PushContext
	reader  *budgetedObjectReader

	mu         sync.Mutex
	verdictSet bool
	verdict    HookVerdict
}

func (s *invocationState) setVerdict(v HookVerdict) {
	s.mu.Lock()
	s.verdictSet = true
	s.verdict = v
	s.mu.Unlock()
}

func (s *invocationState) getVerdict() (HookVerdict, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.verdict, s.verdictSet
}

// Wire JSON DTOs exposed to Wasm guests through HookHostABI.
type wireRefUpdate struct {
	Ref    string `json:"ref"`
	OldOID string `json:"old_oid"`
	NewOID string `json:"new_oid"`
}

type wirePushContext struct {
	TenantID     string          `json:"tenant_id"`
	RepoID       string          `json:"repo_id"`
	OrgID        string          `json:"org_id,omitempty"`
	ActorID      string          `json:"actor_id,omitempty"`
	ObjectFormat string          `json:"object_format,omitempty"`
	Updates      []wireRefUpdate `json:"updates"`
}

type wireCommitMeta struct {
	OID       string    `json:"oid"`
	TreeOID   string    `json:"tree_oid"`
	Parents   []string  `json:"parents,omitempty"`
	Author    Signature `json:"author"`
	Committer Signature `json:"committer"`
	Message   string    `json:"message"`
}

type wireTreeChange struct {
	Path    string     `json:"path"`
	Kind    ChangeKind `json:"kind"`
	OldMode uint32     `json:"old_mode,omitempty"`
	NewMode uint32     `json:"new_mode,omitempty"`
	OldOID  string     `json:"old_oid,omitempty"`
	NewOID  string     `json:"new_oid,omitempty"`
	OldSize int64      `json:"old_size,omitempty"`
	NewSize int64      `json:"new_size,omitempty"`
}

// registerHostABI exposes the read-only HookHostABI Facade under the "stravin" Wasm host module.
// Both v1 (20-byte SHA-1 pointer) and v2 (length-prefixed SHA-1/SHA-256 raw or hex OID) exports are registered.
func registerHostABI(ctx context.Context, rt wazero.Runtime) error {
	_, err := rt.NewHostModuleBuilder(hostModuleName).
		NewFunctionBuilder().
		WithFunc(hostGetPushContext).
		Export("get_push_context").
		NewFunctionBuilder().
		WithFunc(hostListNewCommits).
		Export("list_new_commits").
		NewFunctionBuilder().
		WithFunc(hostDiffTree).
		Export("diff_tree").
		NewFunctionBuilder().
		WithFunc(hostReadBlobChunk).
		Export("read_blob_chunk").
		NewFunctionBuilder().
		WithFunc(hostListNewCommitsV2).
		Export("list_new_commits_v2").
		NewFunctionBuilder().
		WithFunc(hostDiffTreeV2).
		Export("diff_tree_v2").
		NewFunctionBuilder().
		WithFunc(hostReadBlobChunkV2).
		Export("read_blob_chunk_v2").
		NewFunctionBuilder().
		WithFunc(hostSetVerdict).
		Export("set_verdict").
		Instantiate(ctx)
	if err != nil {
		return fmt.Errorf("hooks/wasm: instantiate host module: %w", err)
	}
	return nil
}

func stateFromContext(ctx context.Context) (*invocationState, bool) {
	st, ok := ctx.Value(invocationStateKey{}).(*invocationState)
	return st, ok && st != nil
}

func readOID(mem api.Memory, ptr uint32) ([20]byte, bool) {
	var oid [20]byte
	if mem == nil {
		return oid, false
	}
	raw, ok := mem.Read(ptr, 20)
	if !ok {
		return oid, false
	}
	copy(oid[:], raw)
	return oid, true
}

// readFlexOID reads an algorithm-agnostic OID from guest linear memory.
// It accepts either raw hash bytes (20B for sha1, 32B for sha256) or lowercase
// hex strings (40B for sha1, 64B for sha256) and validates against expectedFormat.
func readFlexOID(mem api.Memory, ptr, length uint32, expectedFormat string) (string, bool) {
	if mem == nil {
		return "", false
	}
	wantRawLen := uint32(20)
	wantHexLen := uint32(40)
	if expectedFormat == ObjectFormatSHA256 {
		wantRawLen = 32
		wantHexLen = 64
	}
	switch length {
	case wantRawLen:
		raw, ok := mem.Read(ptr, length)
		if !ok {
			return "", false
		}
		return hex.EncodeToString(raw), true
	case wantHexLen:
		raw, ok := mem.Read(ptr, length)
		if !ok {
			return "", false
		}
		for _, c := range raw {
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return "", false
			}
		}
		return string(raw), true
	default:
		return "", false
	}
}

func writeGuestBytes(mem api.Memory, outPtr, outCap uint32, payload []byte) int32 {
	if mem == nil {
		return abiErrGeneric
	}
	if uint64(len(payload)) > uint64(outCap) {
		return abiErrBufTooSmall
	}
	if !mem.Write(outPtr, payload) {
		return abiErrGeneric
	}
	return int32(len(payload))
}

func hostGetPushContext(ctx context.Context, mod api.Module, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok {
		return abiErrGeneric
	}
	if err := st.reader.chargeHostCall(); err != nil {
		return abiErrBudgetExceeded
	}

	wire := wirePushContext{
		TenantID:     st.pushCtx.TenantID,
		RepoID:       st.pushCtx.RepoID,
		OrgID:        st.pushCtx.OrgID,
		ActorID:      st.pushCtx.ActorID,
		ObjectFormat: st.pushCtx.EffectiveObjectFormat(),
		Updates:      make([]wireRefUpdate, 0, len(st.pushCtx.Updates)),
	}
	for _, u := range st.pushCtx.Updates {
		wire.Updates = append(wire.Updates, wireRefUpdate{
			Ref:    u.Ref,
			OldOID: u.EffectiveOldOIDHex(),
			NewOID: u.EffectiveNewOIDHex(),
		})
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return abiErrGeneric
	}
	return writeGuestBytes(mod.Memory(), outPtr, outCap, raw)
}

func marshalCommitsWire(commits []CommitMeta) ([]byte, error) {
	wire := make([]wireCommitMeta, 0, len(commits))
	for _, c := range commits {
		wire = append(wire, wireCommitMeta{
			OID:       c.EffectiveOIDHex(),
			TreeOID:   c.EffectiveTreeOIDHex(),
			Parents:   c.EffectiveParentHexes(),
			Author:    c.Author,
			Committer: c.Committer,
			Message:   c.Message,
		})
	}
	return json.Marshal(wire)
}

func marshalTreeChangesWire(changes []TreeChange) ([]byte, error) {
	wire := make([]wireTreeChange, 0, len(changes))
	for _, ch := range changes {
		wire = append(wire, wireTreeChange{
			Path:    ch.Path,
			Kind:    ch.Kind,
			OldMode: ch.OldMode,
			NewMode: ch.NewMode,
			OldOID:  ch.EffectiveOldOIDHex(),
			NewOID:  ch.EffectiveNewOIDHex(),
			OldSize: ch.OldSize,
			NewSize: ch.NewSize,
		})
	}
	return json.Marshal(wire)
}

func hostListNewCommits(ctx context.Context, mod api.Module, oldOIDPtr, newOIDPtr, limit, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok || st.pushCtx.EffectiveObjectFormat() == ObjectFormatSHA256 {
		return abiErrGeneric
	}
	mem := mod.Memory()
	oldOID, ok := readOID(mem, oldOIDPtr)
	if !ok {
		return abiErrGeneric
	}
	newOID, ok := readOID(mem, newOIDPtr)
	if !ok {
		return abiErrGeneric
	}

	commits, err := st.reader.ListNewCommits(oldOID, newOID, limit)
	if err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return abiErrBudgetExceeded
		}
		return abiErrGeneric
	}

	raw, err := marshalCommitsWire(commits)
	if err != nil {
		return abiErrGeneric
	}
	return writeGuestBytes(mem, outPtr, outCap, raw)
}

func hostListNewCommitsV2(ctx context.Context, mod api.Module, oldOIDPtr, oldOIDLen, newOIDPtr, newOIDLen, limit, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok {
		return abiErrGeneric
	}
	mem := mod.Memory()
	objFmt := st.pushCtx.EffectiveObjectFormat()
	oldHex, ok := readFlexOID(mem, oldOIDPtr, oldOIDLen, objFmt)
	if !ok {
		return abiErrGeneric
	}
	newHex, ok := readFlexOID(mem, newOIDPtr, newOIDLen, objFmt)
	if !ok {
		return abiErrGeneric
	}

	commits, err := st.reader.ListNewCommitsByHex(oldHex, newHex, limit)
	if err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return abiErrBudgetExceeded
		}
		return abiErrGeneric
	}

	raw, err := marshalCommitsWire(commits)
	if err != nil {
		return abiErrGeneric
	}
	return writeGuestBytes(mem, outPtr, outCap, raw)
}

func hostDiffTree(ctx context.Context, mod api.Module, oldTreePtr, newTreePtr, limit, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok || st.pushCtx.EffectiveObjectFormat() == ObjectFormatSHA256 {
		return abiErrGeneric
	}
	mem := mod.Memory()
	oldTree, ok := readOID(mem, oldTreePtr)
	if !ok {
		return abiErrGeneric
	}
	newTree, ok := readOID(mem, newTreePtr)
	if !ok {
		return abiErrGeneric
	}

	changes, err := st.reader.DiffTree(ctx, oldTree, newTree, limit)
	if err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return abiErrBudgetExceeded
		}
		return abiErrGeneric
	}

	raw, err := marshalTreeChangesWire(changes)
	if err != nil {
		return abiErrGeneric
	}
	return writeGuestBytes(mem, outPtr, outCap, raw)
}

func hostDiffTreeV2(ctx context.Context, mod api.Module, oldTreePtr, oldTreeLen, newTreePtr, newTreeLen, limit, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok {
		return abiErrGeneric
	}
	mem := mod.Memory()
	objFmt := st.pushCtx.EffectiveObjectFormat()
	oldHex, ok := readFlexOID(mem, oldTreePtr, oldTreeLen, objFmt)
	if !ok {
		return abiErrGeneric
	}
	newHex, ok := readFlexOID(mem, newTreePtr, newTreeLen, objFmt)
	if !ok {
		return abiErrGeneric
	}

	changes, err := st.reader.DiffTreeByHex(ctx, oldHex, newHex, limit)
	if err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return abiErrBudgetExceeded
		}
		return abiErrGeneric
	}

	raw, err := marshalTreeChangesWire(changes)
	if err != nil {
		return abiErrGeneric
	}
	return writeGuestBytes(mem, outPtr, outCap, raw)
}

func hostReadBlobChunk(ctx context.Context, mod api.Module, blobOIDPtr uint32, offset uint64, maxBytes, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok || st.pushCtx.EffectiveObjectFormat() == ObjectFormatSHA256 {
		return abiErrGeneric
	}
	mem := mod.Memory()
	blobOID, ok := readOID(mem, blobOIDPtr)
	if !ok {
		return abiErrGeneric
	}
	reqBytes := min(maxBytes, outCap)
	chunk, err := st.reader.ReadBlobChunk(blobOID, offset, reqBytes)
	if err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return abiErrBudgetExceeded
		}
		return abiErrGeneric
	}
	return writeGuestBytes(mem, outPtr, outCap, chunk)
}

func hostReadBlobChunkV2(ctx context.Context, mod api.Module, blobOIDPtr, blobOIDLen uint32, offset uint64, maxBytes, outPtr, outCap uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok {
		return abiErrGeneric
	}
	mem := mod.Memory()
	blobHex, ok := readFlexOID(mem, blobOIDPtr, blobOIDLen, st.pushCtx.EffectiveObjectFormat())
	if !ok {
		return abiErrGeneric
	}
	reqBytes := min(maxBytes, outCap)
	chunk, err := st.reader.ReadBlobChunkByHex(blobHex, offset, reqBytes)
	if err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return abiErrBudgetExceeded
		}
		return abiErrGeneric
	}
	return writeGuestBytes(mem, outPtr, outCap, chunk)
}

func hostSetVerdict(ctx context.Context, mod api.Module, decision, msgPtr, msgLen uint32) int32 {
	st, ok := stateFromContext(ctx)
	if !ok {
		return abiErrGeneric
	}
	if msgLen > maxVerdictMessageBytes {
		return abiErrGeneric
	}
	var msg string
	if msgLen > 0 {
		mem := mod.Memory()
		if mem == nil {
			return abiErrGeneric
		}
		raw, ok := mem.Read(msgPtr, msgLen)
		if !ok {
			return abiErrGeneric
		}
		msg = string(raw)
	}

	switch decision {
	case verdictDecisionAllow:
		st.setVerdict(HookVerdict{Decision: VerdictAllow, Message: msg})
		return 0
	case verdictDecisionReject:
		st.setVerdict(HookVerdict{Decision: VerdictReject, Message: msg})
		return 0
	case verdictDecisionJSON:
		var parsed HookVerdict
		if err := json.Unmarshal([]byte(msg), &parsed); err != nil {
			return abiErrGeneric
		}
		if parsed.Decision != VerdictAllow && parsed.Decision != VerdictReject {
			return abiErrGeneric
		}
		st.setVerdict(parsed)
		return 0
	default:
		return abiErrGeneric
	}
}
