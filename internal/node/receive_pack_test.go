package node

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // Git packfile v2 test fixtures require SHA-1
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type fakePushStore struct {
	*fakeMetadataStore
	commitMu    sync.Mutex
	commitCalls []ddb.CommitPushInput
	commitErr   error
	hooks       map[tenant.RepoKey][]ddb.HookConfig
}

func newFakePushStore() *fakePushStore {
	return &fakePushStore{
		fakeMetadataStore: newFakeMetadataStore(),
		hooks:             make(map[tenant.RepoKey][]ddb.HookConfig),
	}
}

func (f *fakePushStore) CommitPush(_ context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error) {
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	f.commitCalls = append(f.commitCalls, in)
	if f.commitErr != nil {
		return ddb.CommitPushResult{}, f.commitErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	byRef := make(map[string]ddb.RefState)
	for _, r := range f.refs[in.Repo] {
		byRef[r.RefName] = r
	}
	packID := ""
	if in.Pack != nil {
		packID = in.Pack.PackID
	}
	for _, u := range in.Updates {
		if u.IsDelete() {
			delete(byRef, u.RefName)
			continue
		}
		byRef[u.RefName] = ddb.RefState{
			Repo:         in.Repo,
			RefName:      u.RefName,
			OID:          u.NewOID,
			PrevOID:      u.OldOID,
			PackID:       packID,
			Epoch:        in.Epoch,
			OriginRegion: in.OriginRegion,
		}
	}
	next := make([]ddb.RefState, 0, len(byRef))
	for _, r := range byRef {
		next = append(next, r)
	}
	f.refs[in.Repo] = next

	if in.Pack != nil {
		if f.manifestsByShard[in.Repo] == nil {
			f.manifestsByShard[in.Repo] = make(map[int][]ddb.PackManifest)
		}
		shard, _ := ddb.ManifestShard(in.Pack.PackID, in.ManifestShards)
		ulid := fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G5%03d", len(f.commitCalls))
		f.manifestsByShard[in.Repo][shard] = append(f.manifestsByShard[in.Repo][shard], ddb.PackManifest{
			Repo:         in.Repo,
			ULID:         ulid,
			PackID:       in.Pack.PackID,
			SizeBytes:    in.Pack.SizeBytes,
			OriginRegion: in.Pack.OriginRegion,
			Status:       ddb.PackStatusActive,
		})
	}
	if m, ok := f.metas[in.Repo]; ok {
		m.RefVersion++
		if in.Pack != nil {
			m.ManifestVersion++
		}
		f.metas[in.Repo] = m
	}

	return ddb.CommitPushResult{
		ULID:        fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G5%03d", len(f.commitCalls)),
		CommittedAt: time.Now().UTC(),
		RefCount:    len(in.Updates),
	}, nil
}

func (f *fakePushStore) GetEffectiveHooks(_ context.Context, repo tenant.RepoKey, _ string) ([]ddb.HookConfig, error) {
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	return append([]ddb.HookConfig(nil), f.hooks[repo]...), nil
}

func (f *fakePushStore) SetHooks(repo tenant.RepoKey, hooks []ddb.HookConfig) {
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	f.hooks[repo] = append([]ddb.HookConfig(nil), hooks...)
}

func (f *fakePushStore) CommitCount() int {
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	return len(f.commitCalls)
}

func (f *fakePushStore) LastCommit() ddb.CommitPushInput {
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	return f.commitCalls[len(f.commitCalls)-1]
}

type fakeS3WriteStore struct {
	*fakePackFetcher
	writeMu     sync.Mutex
	putCount    int
	lastPackID  string
	lastPack    []byte
	lastIdx     []byte
	putErr      error
	putHookFn   func(ctx context.Context) error
	wasmModules map[string][]byte
}

func newFakeS3WriteStore() *fakeS3WriteStore {
	return &fakeS3WriteStore{
		fakePackFetcher: newFakePackFetcher(),
		wasmModules:     make(map[string][]byte),
	}
}

func (s *fakeS3WriteStore) PutPack(ctx context.Context, _ tenant.RepoKey, packSHA256 string, packData, idxData []byte) error {
	s.writeMu.Lock()
	fn := s.putHookFn
	err := s.putErr
	s.writeMu.Unlock()

	if fn != nil {
		if fnErr := fn(ctx); fnErr != nil {
			return fnErr
		}
	}
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	s.putCount++
	s.lastPackID = packSHA256
	s.lastPack = append([]byte(nil), packData...)
	s.lastIdx = append([]byte(nil), idxData...)
	s.writeMu.Unlock()
	s.Put(packSHA256, packData, idxData)
	return nil
}

func (s *fakeS3WriteStore) GetWasmHook(_ context.Context, wasmSHA256 string) ([]byte, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b, ok := s.wasmModules[strings.ToLower(wasmSHA256)]
	if !ok {
		return nil, s3.ErrNotFound
	}
	return append([]byte(nil), b...), nil
}

func (s *fakeS3WriteStore) PutWasmFixture(wasmBytes []byte) string {
	sum := sha256.Sum256(wasmBytes)
	sha := hex.EncodeToString(sum[:])
	s.writeMu.Lock()
	s.wasmModules[sha] = append([]byte(nil), wasmBytes...)
	s.writeMu.Unlock()
	return sha
}

func (s *fakeS3WriteStore) PutCount() int {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.putCount
}

type testReceivePackEnv struct {
	rk        tenant.RepoKey
	ddbStore  *fakePushStore
	s3Store   *fakeS3WriteStore
	nvmeCache *cache.Cache
	mat       *Materializer
	refresher *LeaseRefresher
	leaseBox  *fakeRegionalLeaseStore
	tp        *telemetry.Provider
	client    rpc.GitWireClient
}

func newTestReceivePackEnv(t *testing.T, repoName string) *testReceivePackEnv {
	t.Helper()
	rk, err := tenant.NewRepoKey("acme", repoName)
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	ddbStore := newFakePushStore()
	ddbStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	s3Store := newFakeS3WriteStore()
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	leaseBox := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region:    "us-east-1",
			Epoch:     1,
			Mode:      lease.ModeActive,
			HolderID:  "ctrl-1",
			ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		},
	}
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{
		Store:  leaseBox,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      ddbStore,
		Packs:     s3Store,
		Cache:     nvmeCache,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	hookEngine := wasm.NewEngine(
		NewS3WasmLoader(s3Store),
		NewDDBHookConfigStore(ddbStore),
		wasm.EngineConfig{},
	)
	t.Cleanup(func() { _ = hookEngine.Close(context.Background()) })

	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer:   mat,
		Cache:          nvmeCache,
		Packs:          s3Store,
		Committer:      ddbStore,
		LeaseRefresher: refresher,
		HookEngine:     hookEngine,
		Region:         "us-east-1",
		Telemetry:      tp,
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		Materializer:   mat,
		GitExecutor:    gitexec.NewSubprocessExecutor(gitexec.Config{}),
		WriteHandler:   wh,
		LeaseRefresher: refresher,
		Telemetry:      tp,
		Region:         "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	return &testReceivePackEnv{
		rk:        rk,
		ddbStore:  ddbStore,
		s3Store:   s3Store,
		nvmeCache: nvmeCache,
		mat:       mat,
		refresher: refresher,
		leaseBox:  leaseBox,
		tp:        tp,
		client:    startBufconnNode(t, srv),
	}
}

func sendReceivePack(t *testing.T, ctx context.Context, client rpc.GitWireClient, rk tenant.RepoKey, traceParent string, payload []byte) ([]byte, int32, error) {
	t.Helper()
	outCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
	stream, err := client.OpenStream(outCtx, rpc.RequestMeta{
		TenantID:    rk.TenantID,
		RepoID:      rk.RepoID,
		Operation:   rpc.OpReceivePack,
		TraceParent: traceParent,
	})
	if err != nil {
		return nil, 0, err
	}
	if err := stream.Send(&rpc.Frame{Data: payload, EOF: true}); err != nil {
		return nil, 0, err
	}

	var resp bytes.Buffer
	var exitCode int32
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return resp.Bytes(), exitCode, err
		}
		resp.Write(f.Data)
		if f.EOF {
			exitCode = f.ExitCode
			break
		}
	}
	return resp.Bytes(), exitCode, nil
}

func buildPushWirePayload(cmds []ddb.RefUpdate, caps string, packBytes []byte) []byte {
	var buf bytes.Buffer
	for i, c := range cmds {
		oldOID := c.OldOID
		if oldOID == "" {
			oldOID = ddb.ZeroOID
		}
		line := fmt.Sprintf("%s %s %s", oldOID, c.NewOID, c.RefName)
		if i == 0 && caps != "" {
			line += "\x00" + caps
		}
		line += "\n"
		buf.Write(EncodePktLine([]byte(line)))
	}
	buf.Write(FlushPkt)
	if len(packBytes) > 0 {
		buf.Write(packBytes)
	}
	return buf.Bytes()
}

func splitSideBandResponse(t *testing.T, data []byte) (band1, band2 []byte) {
	t.Helper()
	lines := decodePktLines(t, data)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		switch line[0] {
		case SideBandData:
			band1 = append(band1, []byte(line[1:])...)
		case SideBandStderr:
			band2 = append(band2, []byte(line[1:])...)
		}
	}
	return band1, band2
}

// buildMinimalWasmVerdictModule builds a valid WebAssembly 1.0 module calling stravin.set_verdict(decision, ptr, len).
func buildMinimalWasmVerdictModule(decision int32, msg string) []byte {
	encodeULEB := func(v uint32) []byte {
		var out []byte
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if v != 0 {
				b |= 0x80
			}
			out = append(out, b)
			if v == 0 {
				break
			}
		}
		return out
	}
	encodeSLEB := func(v int64) []byte {
		var out []byte
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if (v == 0 && (b&0x40) == 0) || (v == -1 && (b&0x40) != 0) {
				out = append(out, b)
				break
			}
			out = append(out, b|0x80)
		}
		return out
	}
	encodeSec := func(id byte, payload []byte) []byte {
		var out []byte
		out = append(out, id)
		out = append(out, encodeULEB(uint32(len(payload)))...)
		out = append(out, payload...)
		return out
	}
	encodeStr := func(s string) []byte {
		return append(encodeULEB(uint32(len(s))), []byte(s)...)
	}

	const msgOffset = 1024
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00})

	// Type section: type 0 = (i32,i32,i32)->i32, type 1 = ()->i32
	var typeSec bytes.Buffer
	typeSec.Write(encodeULEB(2))
	typeSec.Write([]byte{0x60, 3, 0x7f, 0x7f, 0x7f, 1, 0x7f})
	typeSec.Write([]byte{0x60, 0, 1, 0x7f})
	buf.Write(encodeSec(1, typeSec.Bytes()))

	// Import section: stravin.set_verdict (func type 0)
	var impSec bytes.Buffer
	impSec.Write(encodeULEB(1))
	impSec.Write(encodeStr("stravin"))
	impSec.Write(encodeStr("set_verdict"))
	impSec.Write([]byte{0x00, 0x00})
	buf.Write(encodeSec(2, impSec.Bytes()))

	// Function section: 1 func of type 1
	buf.Write(encodeSec(3, []byte{1, 1}))

	// Memory section: 1 page min
	buf.Write(encodeSec(5, []byte{1, 0, 1}))

	// Export section: "memory" + "run" (func idx 1)
	var expSec bytes.Buffer
	expSec.Write(encodeULEB(2))
	expSec.Write(encodeStr("memory"))
	expSec.Write([]byte{0x02, 0x00})
	expSec.Write(encodeStr("run"))
	expSec.Write([]byte{0x00, 0x01})
	buf.Write(encodeSec(7, expSec.Bytes()))

	// Code section
	var fnBody bytes.Buffer
	fnBody.WriteByte(0x00) // 0 locals
	fnBody.WriteByte(0x41)
	fnBody.Write(encodeSLEB(int64(decision)))
	fnBody.WriteByte(0x41)
	fnBody.Write(encodeSLEB(msgOffset))
	fnBody.WriteByte(0x41)
	fnBody.Write(encodeSLEB(int64(len(msg))))
	fnBody.Write([]byte{0x10, 0x00, 0x1a}) // call 0; drop
	fnBody.Write([]byte{0x41, 0x00, 0x0b}) // i32.const 0; end
	var codeSec bytes.Buffer
	codeSec.Write(encodeULEB(1))
	codeSec.Write(encodeULEB(uint32(fnBody.Len())))
	codeSec.Write(fnBody.Bytes())
	buf.Write(encodeSec(10, codeSec.Bytes()))

	// Data section
	var dataSec bytes.Buffer
	dataSec.Write(encodeULEB(1))
	dataSec.WriteByte(0x00)
	dataSec.WriteByte(0x41)
	dataSec.Write(encodeSLEB(msgOffset))
	dataSec.WriteByte(0x0b)
	dataSec.Write(encodeULEB(uint32(len(msg))))
	dataSec.Write([]byte(msg))
	buf.Write(encodeSec(11, dataSec.Bytes()))

	return buf.Bytes()
}

// buildSyntheticPack builds a valid self-contained Git packfile v2 containing a single commit+tree+blob.
func buildSyntheticCommitPack(t *testing.T, blobContent, commitMsg string, parentOID *pack.OID) (commitOID pack.OID, blobOID pack.OID, packBytes []byte) {
	t.Helper()
	blobData := []byte(blobContent)
	blobOID = pack.ComputeOID(pack.ObjBlob, blobData)

	var treeBuf bytes.Buffer
	treeBuf.WriteString("100644 file.txt\x00")
	treeBuf.Write(blobOID.Bytes())
	treeData := treeBuf.Bytes()
	treeOID := pack.ComputeOID(pack.ObjTree, treeData)

	var commitBuf bytes.Buffer
	fmt.Fprintf(&commitBuf, "tree %s\n", treeOID)
	if parentOID != nil {
		fmt.Fprintf(&commitBuf, "parent %s\n", *parentOID)
	}
	commitBuf.WriteString("author Test <test@stravin.dev> 1700000000 +0000\n")
	commitBuf.WriteString("committer Test <test@stravin.dev> 1700000000 +0000\n\n")
	commitBuf.WriteString(commitMsg)
	commitData := commitBuf.Bytes()
	commitOID = pack.ComputeOID(pack.ObjCommit, commitData)

	packBytes = encodeRawPackfile(t, []rawPackObj{
		{typ: pack.ObjBlob, data: blobData},
		{typ: pack.ObjTree, data: treeData},
		{typ: pack.ObjCommit, data: commitData},
	})
	return commitOID, blobOID, packBytes
}

// buildSyntheticThinPack builds a thin packfile v2 whose new blob is a REF_DELTA against baseBlobOID.
func buildSyntheticThinPack(t *testing.T, baseBlobData []byte, baseBlobOID, parentCommitOID pack.OID) (newCommitOID pack.OID, thinPackBytes []byte) {
	t.Helper()
	suffix := []byte(" + incremental delta line\n")
	targetBlob := append(append([]byte(nil), baseBlobData...), suffix...)
	newBlobOID := pack.ComputeOID(pack.ObjBlob, targetBlob)

	// Git delta encoding: varint(baseSize), varint(targetSize), copy(baseSize from 0), insert(suffix).
	var delta bytes.Buffer
	writeDeltaVarint(&delta, uint64(len(baseBlobData)))
	writeDeltaVarint(&delta, uint64(len(targetBlob)))
	// Copy instruction: 0x80 | 0x10 (size byte 0) -> copy len(baseBlobData) from offset 0
	delta.WriteByte(0x80 | 0x10)
	delta.WriteByte(byte(len(baseBlobData)))
	// Insert instruction: len(suffix) bytes
	delta.WriteByte(byte(len(suffix)))
	delta.Write(suffix)

	var treeBuf bytes.Buffer
	treeBuf.WriteString("100644 file.txt\x00")
	treeBuf.Write(newBlobOID.Bytes())
	treeData := treeBuf.Bytes()
	treeOID := pack.ComputeOID(pack.ObjTree, treeData)

	var commitBuf bytes.Buffer
	fmt.Fprintf(&commitBuf, "tree %s\nparent %s\n", treeOID, parentCommitOID)
	commitBuf.WriteString("author Test <test@stravin.dev> 1700000001 +0000\n")
	commitBuf.WriteString("committer Test <test@stravin.dev> 1700000001 +0000\n\nsecond commit\n")
	commitData := commitBuf.Bytes()
	newCommitOID = pack.ComputeOID(pack.ObjCommit, commitData)

	thinPackBytes = encodeRawPackfile(t, []rawPackObj{
		{typ: pack.ObjRefDelta, refBase: baseBlobOID, data: delta.Bytes()},
		{typ: pack.ObjTree, data: treeData},
		{typ: pack.ObjCommit, data: commitData},
	})
	return newCommitOID, thinPackBytes
}

type rawPackObj struct {
	typ     pack.ObjectType
	refBase pack.OID
	data    []byte
}

func encodeRawPackfile(t *testing.T, objs []rawPackObj) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write([]byte{'P', 'A', 'C', 'K'})
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(objs)))

	for _, o := range objs {
		size := int64(len(o.data))
		b := ((uint8(o.typ) & 0x07) << 4) | byte(size&0x0f)
		size >>= 4
		for size > 0 {
			buf.WriteByte(b | 0x80)
			b = byte(size & 0x7f)
			size >>= 7
		}
		buf.WriteByte(b)
		if o.typ == pack.ObjRefDelta {
			buf.Write(o.refBase.Bytes())
		}
		zw := zlib.NewWriter(&buf)
		if _, err := zw.Write(o.data); err != nil {
			t.Fatalf("zlib write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zlib close: %v", err)
		}
	}

	sum := sha1.Sum(buf.Bytes()) //nolint:gosec // Git packfile v2 trailer requires SHA-1
	buf.Write(sum[:])
	return buf.Bytes()
}

func writeDeltaVarint(buf *bytes.Buffer, v uint64) {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		buf.WriteByte(b)
		if v == 0 {
			break
		}
	}
}

func TestReceivePack_AdvertiseRefsFromDynamoDB(t *testing.T) {
	env := newTestReceivePackEnv(t, "adv-receive")
	mainOID := strings.Repeat("a", 40)
	env.ddbStore.SetRefs(env.rk, []ddb.RefState{
		{Repo: env.rk, RefName: "refs/heads/main", OID: mainOID},
	})

	ctx := metadata.AppendToOutgoingContext(context.Background(), HeaderAdvertiseRefs, "true")
	stream, err := env.client.OpenStream(ctx, rpc.RequestMeta{
		TenantID:  env.rk.TenantID,
		RepoID:    env.rk.RepoID,
		Operation: rpc.OpReceivePack,
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	var buf bytes.Buffer
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		buf.Write(f.Data)
		if f.EOF {
			break
		}
	}

	got := buf.String()
	if !strings.Contains(got, mainOID+" refs/heads/main\x00") {
		t.Fatalf("missing advertised ref: %q", got)
	}
	for _, capName := range []string{"report-status", "delete-refs", "side-band-64k", "ofs-delta", "atomic"} {
		if !strings.Contains(got, capName) {
			t.Errorf("missing receive-pack capability %q in %q", capName, got)
		}
	}
}

func TestReceivePack_InitialPushAndWarmCacheAndOTelSpans(t *testing.T) {
	env := newTestReceivePackEnv(t, "initial-push")
	commitOID, _, packBytes := buildSyntheticCommitPack(t, "hello stravin\n", "initial commit\n", nil)

	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	tpHeader := "00-" + traceID + "-00f067aa0ba902b7-01"
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
		"report-status side-band-64k atomic",
		packBytes,
	)

	resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, tpHeader, wire)
	if err != nil {
		t.Fatalf("sendReceivePack: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}

	band1, _ := splitSideBandResponse(t, resp)
	statusLines := decodePktLines(t, band1)
	if len(statusLines) != 2 || statusLines[0] != "unpack ok\n" || statusLines[1] != "ok refs/heads/main\n" {
		t.Fatalf("unexpected report-status lines: %q", statusLines)
	}

	if env.s3Store.PutCount() != 1 {
		t.Fatalf("S3 PutPack count = %d, want 1", env.s3Store.PutCount())
	}
	if env.ddbStore.CommitCount() != 1 {
		t.Fatalf("DDB CommitPush count = %d, want 1", env.ddbStore.CommitCount())
	}
	lastCommit := env.ddbStore.LastCommit()
	if !strings.Contains(lastCommit.TraceParent, traceID) {
		t.Errorf("DDB CommitPush TraceParent = %q, want trace ID %s", lastCommit.TraceParent, traceID)
	}
	if lastCommit.Pack == nil || !env.nvmeCache.HasPack(env.rk, lastCommit.Pack.PackID) {
		t.Fatalf("expected committed pack %v installed in warm NVMe cache", lastCommit.Pack)
	}

	wantSpans := map[string]bool{
		"node.pack.index_validate": false,
		"s3.put_pack":              false,
		"hook.wasm.exec":           false,
		"ddb.transact_write":       false,
	}
	for _, sp := range env.tp.SpanExporter.GetSpans() {
		if _, ok := wantSpans[sp.Name]; ok {
			wantSpans[sp.Name] = true
			if got := sp.SpanContext.TraceID().String(); got != traceID {
				t.Errorf("span %s trace_id = %s, want %s", sp.Name, got, traceID)
			}
		}
	}
	for name, seen := range wantSpans {
		if !seen {
			t.Errorf("missing expected OTel span %q", name)
		}
	}
}

func TestReceivePack_IncrementalThinPackPush(t *testing.T) {
	env := newTestReceivePackEnv(t, "thin-push")

	// 1. Initial push seeds base blob and commit.
	baseBlobContent := "hello thin pack base\n"
	commit1, blob1, pack1 := buildSyntheticCommitPack(t, baseBlobContent, "base commit\n", nil)
	wire1 := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commit1.String()}},
		"report-status",
		pack1,
	)
	if _, _, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire1); err != nil {
		t.Fatalf("initial push: %v", err)
	}

	// 2. Incremental thin-pack push referencing blob1 via REF_DELTA (not present in thinPack2).
	commit2, thinPack2 := buildSyntheticThinPack(t, []byte(baseBlobContent), blob1, commit1)
	wire2 := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: commit1.String(), NewOID: commit2.String()}},
		"report-status side-band-64k",
		thinPack2,
	)
	resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire2)
	if err != nil {
		t.Fatalf("thin push: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}

	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) != 2 || lines[0] != "unpack ok\n" || lines[1] != "ok refs/heads/main\n" {
		t.Fatalf("unexpected thin push status: %q", lines)
	}

	// Verify the pack uploaded to S3 is self-contained (parseable with nil BaseResolver).
	env.s3Store.writeMu.Lock()
	uploadedPack := append([]byte(nil), env.s3Store.lastPack...)
	env.s3Store.writeMu.Unlock()
	if _, err := pack.Parse(bytes.NewReader(uploadedPack), pack.ParseOptions{}); err != nil {
		t.Fatalf("uploaded completed pack is not self-contained: %v", err)
	}
}

func TestReceivePack_PureRefDeletion(t *testing.T) {
	env := newTestReceivePackEnv(t, "ref-delete")
	existingOID := strings.Repeat("b", 40)
	env.ddbStore.SetRefs(env.rk, []ddb.RefState{
		{Repo: env.rk, RefName: "refs/heads/feature", OID: existingOID},
	})

	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/feature", OldOID: existingOID, NewOID: ddb.ZeroOID}},
		"report-status delete-refs",
		nil,
	)
	resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
	if err != nil {
		t.Fatalf("delete push: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	lines := decodePktLines(t, resp)
	if len(lines) != 2 || lines[0] != "unpack ok\n" || lines[1] != "ok refs/heads/feature\n" {
		t.Fatalf("unexpected delete status: %q", lines)
	}
	if env.s3Store.PutCount() != 0 {
		t.Fatalf("pure ref deletion should not call S3 PutPack, got %d", env.s3Store.PutCount())
	}
	if env.ddbStore.CommitCount() != 1 || env.ddbStore.LastCommit().Pack != nil {
		t.Fatalf("expected 1 DDB commit with Pack=nil, got count=%d pack=%v", env.ddbStore.CommitCount(), env.ddbStore.LastCommit().Pack)
	}
}

func TestReceivePack_StaleOldOIDFastReject(t *testing.T) {
	env := newTestReceivePackEnv(t, "stale-ref")
	currentOID := strings.Repeat("c", 40)
	staleOID := strings.Repeat("d", 40)
	newOID := strings.Repeat("e", 40)
	env.ddbStore.SetRefs(env.rk, []ddb.RefState{
		{Repo: env.rk, RefName: "refs/heads/main", OID: currentOID},
	})

	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: staleOID, NewOID: newOID}},
		"report-status",
		nil,
	)
	resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
	if err != nil {
		t.Fatalf("stale push: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	lines := decodePktLines(t, resp)
	if len(lines) < 2 || !strings.Contains(lines[1], "ng refs/heads/main stale old_oid") {
		t.Fatalf("expected stale old_oid rejection, got %q", lines)
	}
	if env.s3Store.PutCount() != 0 {
		t.Fatalf("stale push must not upload to S3, got %d", env.s3Store.PutCount())
	}
	if env.ddbStore.CommitCount() != 0 {
		t.Fatalf("stale push must not commit to DDB, got %d", env.ddbStore.CommitCount())
	}
}

func TestReceivePack_TooManyRefsRejected(t *testing.T) {
	env := newTestReceivePackEnv(t, "too-many-refs")
	updates := make([]ddb.RefUpdate, 0, ddb.MaxAtomicPushRefs+1)
	newOID := strings.Repeat("f", 40)
	for i := 0; i <= ddb.MaxAtomicPushRefs; i++ {
		updates = append(updates, ddb.RefUpdate{
			RefName: fmt.Sprintf("refs/heads/b-%02d", i),
			OldOID:  ddb.ZeroOID,
			NewOID:  newOID,
		})
	}

	wire := buildPushWirePayload(updates, "report-status side-band-64k", nil)
	resp, _, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
	if err != nil {
		t.Fatalf("too-many-refs push: %v", err)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) < 2 || !strings.Contains(lines[1], "ng refs/heads/b-00") {
		t.Fatalf("expected >45 ref rejection, got %q", lines)
	}
	if env.s3Store.PutCount() != 0 || env.ddbStore.CommitCount() != 0 {
		t.Fatalf("expected zero S3/DDB calls, got s3=%d ddb=%d", env.s3Store.PutCount(), env.ddbStore.CommitCount())
	}
}

func TestReceivePack_TooManyRefsWithoutFlushRejected(t *testing.T) {
	env := newTestReceivePackEnv(t, "too-many-refs-no-flush")
	newOID := strings.Repeat("f", 40)
	var wire bytes.Buffer
	for i := 0; i < ddb.MaxAtomicPushRefs+5; i++ {
		line := fmt.Sprintf("%s %s refs/heads/b-%02d", ddb.ZeroOID, newOID, i)
		if i == 0 {
			line += "\x00report-status side-band-64k"
		}
		wire.Write(EncodePktLine([]byte(line + "\n")))
	}
	// Intentionally omit FlushPkt ("0000") to simulate an unterminated command flood.

	resp, _, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire.Bytes())
	if err != nil {
		t.Fatalf("unterminated too-many-refs push: %v", err)
	}
	band1, band2 := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) < 2 || !strings.Contains(lines[1], "ng refs/heads/b-00") {
		t.Fatalf("expected >45 ref rejection, got %q", lines)
	}
	if !strings.Contains(string(band2), "push exceeds max atomic ref limit") {
		t.Fatalf("expected side-band stderr limit message, got %q", string(band2))
	}
	if env.s3Store.PutCount() != 0 || env.ddbStore.CommitCount() != 0 {
		t.Fatalf("expected zero S3/DDB calls, got s3=%d ddb=%d", env.s3Store.PutCount(), env.ddbStore.CommitCount())
	}
}

func TestReceivePack_LeaseExpiredFailsClosed(t *testing.T) {
	env := newTestReceivePackEnv(t, "lease-expired")
	env.leaseBox.Set(ddb.RegionalWriteLease{
		Region:    "us-east-1",
		Epoch:     1,
		Mode:      lease.ModeActive,
		HolderID:  "ctrl-1",
		ExpiresAt: time.Now().UTC().Add(-10 * time.Second),
	}, nil)
	if err := env.refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: strings.Repeat("a", 40)}},
		"report-status",
		nil,
	)
	_, _, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expired lease error code = %v (err=%v), want FailedPrecondition", status.Code(err), err)
	}
	if env.s3Store.PutCount() != 0 || env.ddbStore.CommitCount() != 0 {
		t.Fatalf("expired lease must not touch S3 or DDB")
	}
}

func TestReceivePack_ConcurrentWasmAllowAndReject(t *testing.T) {
	t.Run("WasmAllow", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "wasm-allow")
		allowSHA := env.s3Store.PutWasmFixture(buildMinimalWasmVerdictModule(0, ""))
		env.ddbStore.SetHooks(env.rk, []ddb.HookConfig{
			{
				HookID:        "allow-hook",
				WasmSHA256:    allowSHA,
				RefGlob:       "refs/heads/*",
				TimeoutMS:     100,
				MemoryLimitMB: 16,
				FailMode:      ddb.FailModeClosed,
			},
		})

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "clean content\n", "good commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
			"report-status side-band-64k",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("WasmAllow push: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) != 2 || lines[1] != "ok refs/heads/main\n" {
			t.Fatalf("unexpected WasmAllow status: %q", lines)
		}
		if env.s3Store.PutCount() != 1 || env.ddbStore.CommitCount() != 1 {
			t.Fatalf("expected 1 S3 put and 1 DDB commit, got s3=%d ddb=%d", env.s3Store.PutCount(), env.ddbStore.CommitCount())
		}
	})

	t.Run("WasmRejectCancelsS3AndStreamsSideBandAndSkipsDDB", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "wasm-reject")
		rejectMsg := "secret token detected in commit"
		rejectSHA := env.s3Store.PutWasmFixture(buildMinimalWasmVerdictModule(1, rejectMsg))
		env.ddbStore.SetHooks(env.rk, []ddb.HookConfig{
			{
				HookID:        "secret-scan",
				WasmSHA256:    rejectSHA,
				RefGlob:       "refs/heads/*",
				TimeoutMS:     200,
				MemoryLimitMB: 16,
				FailMode:      ddb.FailModeClosed,
			},
		})

		s3Canceled := make(chan struct{}, 1)
		env.s3Store.putHookFn = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				s3Canceled <- struct{}{}
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return errors.New("s3 upload was not canceled by hook rejection")
			}
		}

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "AKIASECRET123456\n", "bad commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
			"report-status side-band-64k",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("WasmReject push: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}

		select {
		case <-s3Canceled:
		case <-time.After(2 * time.Second):
			t.Fatal("expected S3 PutPack context to be immediately canceled on Wasm REJECT")
		}

		band1, band2 := splitSideBandResponse(t, resp)
		if !strings.Contains(string(band2), rejectMsg) {
			t.Fatalf("expected hook stderr %q on side-band channel 2, got %q", rejectMsg, string(band2))
		}
		lines := decodePktLines(t, band1)
		if len(lines) < 2 || !strings.HasPrefix(lines[1], "ng refs/heads/main ") {
			t.Fatalf("expected ng rejection in report-status, got %q", lines)
		}
		if env.ddbStore.CommitCount() != 0 {
			t.Fatalf("Wasm REJECT must NEVER touch DynamoDB, got %d commits", env.ddbStore.CommitCount())
		}
	})
}

func TestReceivePack_RefCASConflict(t *testing.T) {
	env := newTestReceivePackEnv(t, "cas-conflict")
	env.ddbStore.commitErr = &ddb.RefConflictError{
		ConflictingRefs: []string{"refs/heads/main"},
	}

	commitOID, _, packBytes := buildSyntheticCommitPack(t, "race content\n", "race commit\n", nil)
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
		"report-status side-band-64k",
		packBytes,
	)
	resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
	if err != nil {
		t.Fatalf("CAS conflict push: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) < 2 || !strings.Contains(lines[1], "ng refs/heads/main ref CAS conflict") {
		t.Fatalf("expected ref CAS conflict in report-status, got %q", lines)
	}
}

func TestPackObjectReaderAdapter_CommitsDiffAndBlob(t *testing.T) {
	commit1, blob1, pack1 := buildSyntheticCommitPack(t, "hello adapter\n", "adapter commit\n", nil)
	parsed, err := pack.Parse(bytes.NewReader(pack1), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("pack.Parse: %v", err)
	}
	adapter := NewPackObjectReaderAdapter(pack.NewPackReader(parsed, nil))

	c1Raw, _ := commit1.SHA1Bytes()
	commits, err := adapter.ListNewCommits([20]byte{}, c1Raw, 10)
	if err != nil {
		t.Fatalf("ListNewCommits: %v", err)
	}
	if len(commits) != 1 || commits[0].Message != "adapter commit\n" {
		t.Fatalf("unexpected commits: %+v", commits)
	}

	changes, err := adapter.DiffTree([20]byte{}, commits[0].TreeOID, 10)
	if err != nil {
		t.Fatalf("DiffTree: %v", err)
	}
	if len(changes) != 1 || changes[0].Path != "file.txt" || changes[0].Kind != wasm.ChangeAdd {
		t.Fatalf("unexpected tree changes: %+v", changes)
	}

	b1Raw, _ := blob1.SHA1Bytes()
	chunk, err := adapter.ReadBlobChunk(b1Raw, 0, 64)
	if err != nil {
		t.Fatalf("ReadBlobChunk: %v", err)
	}
	if string(chunk) != "hello adapter\n" {
		t.Fatalf("ReadBlobChunk = %q, want %q", string(chunk), "hello adapter\n")
	}

	ctxReader, ok := adapter.(wasm.ContextDiffTreeReader)
	if !ok {
		t.Fatalf("adapter %T does not implement wasm.ContextDiffTreeReader", adapter)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ctxReader.DiffTreeContext(canceledCtx, [20]byte{}, commits[0].TreeOID, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("DiffTreeContext(canceled) err = %v, want context.Canceled", err)
	}
}

func TestReceivePack_RefDirectoryCollisions(t *testing.T) {
	t.Run("PushChildWhenParentExists", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "collide-child")
		existingOID := strings.Repeat("a", 40)
		env.ddbStore.SetRefs(env.rk, []ddb.RefState{
			{Repo: env.rk, RefName: "refs/heads/a", OID: existingOID},
		})

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "child\n", "child commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{{RefName: "refs/heads/a/b", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
			"report-status side-band-64k",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("sendReceivePack: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) < 2 || !strings.HasPrefix(lines[1], "ng refs/heads/a/b ") || !strings.Contains(lines[1], "collision") {
			t.Fatalf("expected ref collision rejection, got %q", lines)
		}
		if env.s3Store.PutCount() != 0 || env.ddbStore.CommitCount() != 0 {
			t.Fatalf("expected zero S3/DDB writes on collision, got s3=%d ddb=%d", env.s3Store.PutCount(), env.ddbStore.CommitCount())
		}
	})

	t.Run("PushParentWhenChildExists", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "collide-parent")
		existingOID := strings.Repeat("b", 40)
		env.ddbStore.SetRefs(env.rk, []ddb.RefState{
			{Repo: env.rk, RefName: "refs/heads/a/b", OID: existingOID},
		})

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "parent\n", "parent commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{{RefName: "refs/heads/a", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
			"report-status side-band-64k",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("sendReceivePack: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) < 2 || !strings.HasPrefix(lines[1], "ng refs/heads/a ") || !strings.Contains(lines[1], "collision") {
			t.Fatalf("expected ref collision rejection, got %q", lines)
		}
		if env.s3Store.PutCount() != 0 || env.ddbStore.CommitCount() != 0 {
			t.Fatalf("expected zero S3/DDB writes on collision, got s3=%d ddb=%d", env.s3Store.PutCount(), env.ddbStore.CommitCount())
		}
	})

	t.Run("PushParentAndChildInSameBatch", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "collide-batch")
		commitOID, _, packBytes := buildSyntheticCommitPack(t, "batch\n", "batch commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{
				{RefName: "refs/heads/a", OldOID: ddb.ZeroOID, NewOID: commitOID.String()},
				{RefName: "refs/heads/a/b", OldOID: ddb.ZeroOID, NewOID: commitOID.String()},
				{RefName: "refs/heads/ok", OldOID: ddb.ZeroOID, NewOID: commitOID.String()},
			},
			"report-status side-band-64k atomic",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("sendReceivePack: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) != 4 {
			t.Fatalf("expected 4 status lines, got %q", lines)
		}
		if !strings.HasPrefix(lines[1], "ng refs/heads/a ") || !strings.Contains(lines[1], "collision") {
			t.Fatalf("expected collision on refs/heads/a, got %q", lines[1])
		}
		if !strings.HasPrefix(lines[2], "ng refs/heads/a/b ") || !strings.Contains(lines[2], "collision") {
			t.Fatalf("expected collision on refs/heads/a/b, got %q", lines[2])
		}
		if !strings.HasPrefix(lines[3], "ng refs/heads/ok ") {
			t.Fatalf("expected atomic rejection on refs/heads/ok, got %q", lines[3])
		}
		if env.s3Store.PutCount() != 0 || env.ddbStore.CommitCount() != 0 {
			t.Fatalf("expected zero S3/DDB writes on collision, got s3=%d ddb=%d", env.s3Store.PutCount(), env.ddbStore.CommitCount())
		}
	})

	seedExistingRef := func(t *testing.T, env *testReceivePackEnv, refName, content string) string {
		t.Helper()
		oid, _, packBytes := buildSyntheticCommitPack(t, content, "seed "+refName+"\n", nil)
		parsed, err := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{})
		if err != nil {
			t.Fatalf("parse seed pack: %v", err)
		}
		idxBytes, err := pack.BuildIdx(parsed)
		if err != nil {
			t.Fatalf("build seed idx: %v", err)
		}
		sum := sha256.Sum256(packBytes)
		packID := hex.EncodeToString(sum[:])
		if err := env.s3Store.PutPack(context.Background(), env.rk, packID, packBytes, idxBytes); err != nil {
			t.Fatalf("put seed pack: %v", err)
		}
		env.ddbStore.AddManifest(env.rk, 0, ddb.PackManifest{
			Repo:         env.rk,
			ULID:         "01ARZ3NDEKTSV4RRFFQ69G5FZ0",
			PackID:       packID,
			SizeBytes:    int64(len(packBytes)),
			OriginRegion: "us-east-1",
			Status:       ddb.PackStatusActive,
		})
		env.ddbStore.SetRefs(env.rk, []ddb.RefState{
			{Repo: env.rk, RefName: refName, OID: oid.String(), PackID: packID},
		})
		return oid.String()
	}

	t.Run("DeleteParentAndCreateChildInSameBatchAllowed", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "replace-parent-with-child")
		existingOID := seedExistingRef(t, env, "refs/heads/a", "seed-a\n")

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "replace\n", "replace commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{
				{RefName: "refs/heads/a", OldOID: existingOID, NewOID: ddb.ZeroOID},
				{RefName: "refs/heads/a/b", OldOID: ddb.ZeroOID, NewOID: commitOID.String()},
			},
			"report-status side-band-64k delete-refs atomic",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("sendReceivePack: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) != 3 || lines[1] != "ok refs/heads/a\n" || lines[2] != "ok refs/heads/a/b\n" {
			t.Fatalf("expected delete+create to succeed, got %q", lines)
		}
		if env.ddbStore.CommitCount() != 1 {
			t.Fatalf("expected 1 DDB commit, got %d", env.ddbStore.CommitCount())
		}
	})

	t.Run("DeleteChildAndCreateParentInSameBatchAllowed", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "replace-child-with-parent")
		existingOID := seedExistingRef(t, env, "refs/heads/a/b", "seed-ab\n")

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "replace2\n", "replace2 commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{
				{RefName: "refs/heads/a/b", OldOID: existingOID, NewOID: ddb.ZeroOID},
				{RefName: "refs/heads/a", OldOID: ddb.ZeroOID, NewOID: commitOID.String()},
			},
			"report-status side-band-64k delete-refs atomic",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("sendReceivePack: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) != 3 || lines[1] != "ok refs/heads/a/b\n" || lines[2] != "ok refs/heads/a\n" {
			t.Fatalf("expected delete+create to succeed, got %q", lines)
		}
		if env.ddbStore.CommitCount() != 1 {
			t.Fatalf("expected 1 DDB commit, got %d", env.ddbStore.CommitCount())
		}
	})

	t.Run("SiblingNamePrefixAllowed", func(t *testing.T) {
		env := newTestReceivePackEnv(t, "sibling-prefix")
		_ = seedExistingRef(t, env, "refs/heads/a", "seed-sibling\n")

		commitOID, _, packBytes := buildSyntheticCommitPack(t, "sibling\n", "sibling commit\n", nil)
		wire := buildPushWirePayload(
			[]ddb.RefUpdate{{RefName: "refs/heads/ab", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
			"report-status side-band-64k",
			packBytes,
		)
		resp, exitCode, err := sendReceivePack(t, context.Background(), env.client, env.rk, "", wire)
		if err != nil {
			t.Fatalf("sendReceivePack: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		band1, _ := splitSideBandResponse(t, resp)
		lines := decodePktLines(t, band1)
		if len(lines) != 2 || lines[1] != "ok refs/heads/ab\n" {
			t.Fatalf("expected sibling ref push to succeed, got %q", lines)
		}
	})
}

func TestReceivePack_SingleListRefsAndNoReftableMaterialization(t *testing.T) {
	rk, err := tenant.NewRepoKey("acme", "single-listrefs")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	ddbStore := newFakePushStore()
	ddbStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	s3Store := newFakeS3WriteStore()
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	leaseBox := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region:    "us-east-1",
			Epoch:     1,
			Mode:      lease.ModeActive,
			HolderID:  "ctrl-1",
			ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		},
	}
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{Store: leaseBox, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	spyExec := newSpyBareRepoExecutor()
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        ddbStore,
		Packs:       s3Store,
		Cache:       nvmeCache,
		GitExecutor: spyExec,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer:   mat,
		Cache:          nvmeCache,
		Packs:          s3Store,
		Committer:      ddbStore,
		LeaseRefresher: refresher,
		Region:         "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer:   mat,
		GitExecutor:    spyExec,
		WriteHandler:   wh,
		LeaseRefresher: refresher,
		Region:         "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	commitOID, _, packBytes := buildSyntheticCommitPack(t, "push-content\n", "push commit\n", nil)
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commitOID.String()}},
		"report-status side-band-64k",
		packBytes,
	)
	outCtx := metadata.AppendToOutgoingContext(context.Background(), HeaderStatelessRPC, "true")
	resp, exitCode, err := sendReceivePack(t, outCtx, client, rk, "", wire)
	if err != nil {
		t.Fatalf("sendReceivePack: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) != 2 || lines[1] != "ok refs/heads/main\n" {
		t.Fatalf("unexpected report-status lines: %q", lines)
	}
	if ddbStore.ListRefsCalls() != 1 {
		t.Errorf("ListRefsCalls = %d, want 1 (must not call ListRefs twice during push)", ddbStore.ListRefsCalls())
	}
	if spyExec.initBareCalls.Load() != 0 || spyExec.forEachRefCalls.Load() != 0 || spyExec.updateRefCalls.Load() != 0 || spyExec.symbolicRefCalls.Load() != 0 {
		t.Errorf("expected 0 git ref subprocesses during push, got init:%d foreach:%d update:%d symref:%d",
			spyExec.initBareCalls.Load(), spyExec.forEachRefCalls.Load(), spyExec.updateRefCalls.Load(), spyExec.symbolicRefCalls.Load())
	}
}

func TestReceivePackHandler_UpdateFastPathSkipsListRefsAndSyncAndPin(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "push-fastpath")
	ddbStore := newFakePushStore()
	ddbStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 4, ManifestShards: 2, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})

	baseCommitOID, baseBlobOID, basePackBytes := buildSyntheticCommitPack(t, "base-content\n", "base commit\n", nil)
	baseParsed, err := pack.Parse(bytes.NewReader(basePackBytes), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("parse base pack: %v", err)
	}
	baseIdxBytes, err := pack.BuildIdx(baseParsed)
	if err != nil {
		t.Fatalf("build base idx: %v", err)
	}
	basePackSum := sha256.Sum256(basePackBytes)
	basePackID := hex.EncodeToString(basePackSum[:])

	ddbStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: baseCommitOID.String(), PackID: basePackID},
	})
	s3Store := newFakeS3WriteStore()
	s3Store.Put(basePackID, basePackBytes, baseIdxBytes)

	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	// Pre-warm local NVMe cache with base pack.
	if err := nvmeCache.InstallPack(rk, basePackID, basePackBytes, baseIdxBytes); err != nil {
		t.Fatalf("InstallPack: %v", err)
	}

	leaseBox := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region: "us-east-1", Epoch: 1, Mode: lease.ModeActive,
			HolderID: "ctrl-1", ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		},
	}
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{Store: leaseBox, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	spyExec := newSpyBareRepoExecutor()
	mat, err := NewMaterializer(MaterializerConfig{
		Meta: ddbStore, Packs: s3Store, Cache: nvmeCache, GitExecutor: spyExec,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer: mat, Cache: nvmeCache, Packs: s3Store,
		Committer: ddbStore, LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer: mat, GitExecutor: spyExec, WriteHandler: wh,
		LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	// Build a thin pack delta referencing baseBlobOID (which is in warm local cache).
	newCommitOID, thinPackBytes := buildSyntheticThinPack(t, []byte("base-content\n"), baseBlobOID, baseCommitOID)
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: baseCommitOID.String(), NewOID: newCommitOID.String()}},
		"report-status side-band-64k",
		thinPackBytes,
	)
	outCtx := metadata.AppendToOutgoingContext(context.Background(), HeaderStatelessRPC, "true")
	resp, exitCode, err := sendReceivePack(t, outCtx, client, rk, "", wire)
	if err != nil {
		t.Fatalf("sendReceivePack: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) != 2 || lines[1] != "ok refs/heads/main\n" {
		t.Fatalf("unexpected report-status lines: %q", lines)
	}
	if got := ddbStore.ListRefsCalls(); got != 0 {
		t.Errorf("ListRefsCalls = %d, want 0 (update push must use GetRef)", got)
	}
	if got := ddbStore.GetRefCalls(); got != 1 {
		t.Errorf("GetRefCalls = %d, want 1", got)
	}
	if got := ddbStore.QueryManifestCalls(); got != 0 {
		t.Errorf("QueryManifestCalls = %d, want 0 (warm local pin must skip SyncAndPin)", got)
	}

	// Verify SyncAndPin immediately after push sees the newly committed SHA (0 stale reads).
	syncRes, err := mat.SyncAndPin(context.Background(), rk)
	if err != nil {
		t.Fatalf("SyncAndPin after push: %v", err)
	}
	defer syncRes.Handle.Release()
	if len(syncRes.Refs) != 1 || syncRes.Refs[0].OID != newCommitOID.String() {
		t.Fatalf("SyncAndPin refs = %+v, want %s", syncRes.Refs, newCommitOID.String())
	}
}

func TestReceivePackHandler_UpdateMissingBaseFallsBackToSyncAndPin(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "push-fallback")
	ddbStore := newFakePushStore()
	ddbStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 2, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})

	baseCommitOID, baseBlobOID, basePackBytes := buildSyntheticCommitPack(t, "base-content\n", "base commit\n", nil)
	baseParsed, err := pack.Parse(bytes.NewReader(basePackBytes), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("parse base pack: %v", err)
	}
	baseIdxBytes, err := pack.BuildIdx(baseParsed)
	if err != nil {
		t.Fatalf("build base idx: %v", err)
	}
	basePackSum := sha256.Sum256(basePackBytes)
	basePackID := hex.EncodeToString(basePackSum[:])

	ddbStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: baseCommitOID.String(), PackID: basePackID},
	})
	ddbStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FD1", PackID: basePackID, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	s3Store := newFakeS3WriteStore()
	s3Store.Put(basePackID, basePackBytes, baseIdxBytes)

	// Cold cache: base pack is ONLY in S3/manifest, NOT in local NVMe cache.
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	leaseBox := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region: "us-east-1", Epoch: 1, Mode: lease.ModeActive,
			HolderID: "ctrl-1", ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		},
	}
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{Store: leaseBox, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta: ddbStore, Packs: s3Store, Cache: nvmeCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer: mat, Cache: nvmeCache, Packs: s3Store,
		Committer: ddbStore, LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer: mat, GitExecutor: newSpyBareRepoExecutor(), WriteHandler: wh,
		LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	newCommitOID, thinPackBytes := buildSyntheticThinPack(t, []byte("base-content\n"), baseBlobOID, baseCommitOID)
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: baseCommitOID.String(), NewOID: newCommitOID.String()}},
		"report-status side-band-64k",
		thinPackBytes,
	)
	outCtx := metadata.AppendToOutgoingContext(context.Background(), HeaderStatelessRPC, "true")
	resp, exitCode, err := sendReceivePack(t, outCtx, client, rk, "", wire)
	if err != nil {
		t.Fatalf("sendReceivePack: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) != 2 || lines[1] != "ok refs/heads/main\n" {
		t.Fatalf("unexpected report-status lines: %q", lines)
	}
	if got := ddbStore.QueryManifestCalls(); got == 0 {
		t.Errorf("expected SyncAndPin fallback to query manifest shards, got 0")
	}
}

func TestReceivePackHandler_UpdateNonExistentRefViaGetRefRejectedAsStale(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "push-getref-notfound")
	ddbStore := newFakePushStore()
	ddbStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 2, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})
	s3Store := newFakeS3WriteStore()
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	leaseBox := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region: "us-east-1", Epoch: 1, Mode: lease.ModeActive,
			HolderID: "ctrl-1", ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		},
	}
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{Store: leaseBox, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta: ddbStore, Packs: s3Store, Cache: nvmeCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)
	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer: mat, Cache: nvmeCache, Packs: s3Store,
		Committer: ddbStore, LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer: mat, GitExecutor: newSpyBareRepoExecutor(), WriteHandler: wh,
		LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	oldOID := strings.Repeat("a", 40)
	// Delete of a non-existent ref (no create, no pack): must use GetRef -> ErrNotFound -> stale old_oid.
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/missing", OldOID: oldOID, NewOID: ddb.ZeroOID}},
		"report-status side-band-64k",
		nil,
	)
	outCtx := metadata.AppendToOutgoingContext(context.Background(), HeaderStatelessRPC, "true")
	resp, exitCode, err := sendReceivePack(t, outCtx, client, rk, "", wire)
	if err != nil {
		t.Fatalf("sendReceivePack: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) != 2 || lines[1] != "ng refs/heads/missing stale old_oid\n" {
		t.Fatalf("unexpected report-status lines: %q", lines)
	}
	if got := ddbStore.ListRefsCalls(); got != 0 {
		t.Errorf("ListRefsCalls = %d, want 0", got)
	}
	if got := ddbStore.GetRefCalls(); got != 1 {
		t.Errorf("GetRefCalls = %d, want 1", got)
	}
}

func TestReceivePack_SameNodePushUpdatesVersionedCache(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "same-node-cache")

	commit1, pack1, idx1 := buildSyntheticCommitPackForSync(t, pack.SHA1, "base\n", "base commit\n")
	commit2, pack2, _ := buildSyntheticCommitPackForSync(t, pack.SHA1, "next\n", "next commit\n")
	packID1 := strings.Repeat("b", 64)

	ddbStore := newFakePushStore()
	ddbStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 2, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
		RefVersion: 1, ManifestVersion: 1,
	})
	ddbStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: commit1},
	})
	ddbStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5F01", PackID: packID1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})

	s3Store := newFakeS3WriteStore()
	s3Store.Put(packID1, pack1, idx1)
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	leaseBox := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region: "us-east-1", Epoch: 1, Mode: lease.ModeActive,
			HolderID: "ctrl-1", ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		},
	}
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{Store: leaseBox, Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(ctx); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                   ddbStore,
		Packs:                  s3Store,
		Cache:                  nvmeCache,
		PruneReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)
	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer: mat, Cache: nvmeCache, Packs: s3Store,
		Committer: ddbStore, LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer: mat, GitExecutor: newSpyBareRepoExecutor(), WriteHandler: wh,
		LeaseRefresher: refresher, Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	// Warm both refCache (v1) and syncedManifestVersion (v1) via SyncAndPin.
	warm, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(warm): %v", err)
	}
	warm.Handle.Release()
	listBeforePush := ddbStore.ListRefsCalls()
	manifestBeforePush := ddbStore.QueryManifestCalls()

	// Same-node push updating refs/heads/main and creating refs/heads/feat with pack2.
	wire := buildPushWirePayload(
		[]ddb.RefUpdate{
			{RefName: "refs/heads/main", OldOID: commit1, NewOID: commit2},
			{RefName: "refs/heads/feat", OldOID: ddb.ZeroOID, NewOID: commit2},
		},
		"report-status side-band-64k",
		pack2,
	)
	outCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
	resp, exitCode, err := sendReceivePack(t, outCtx, client, rk, "", wire)
	if err != nil {
		t.Fatalf("sendReceivePack: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	band1, _ := splitSideBandResponse(t, resp)
	lines := decodePktLines(t, band1)
	if len(lines) != 3 || lines[0] != "unpack ok\n" || lines[1] != "ok refs/heads/main\n" || lines[2] != "ok refs/heads/feat\n" {
		t.Fatalf("unexpected report-status lines: %q", lines)
	}

	// Subsequent LoadAuthoritativeRefs sees v2 from GetMeta and serves updated refs with 0 ListRefs calls!
	gotMeta, gotRefs, err := mat.LoadAuthoritativeRefs(ctx, rk)
	if err != nil {
		t.Fatalf("LoadAuthoritativeRefs(after push): %v", err)
	}
	if gotMeta.RefVersion != 2 || gotMeta.ManifestVersion != 2 {
		t.Fatalf("got versions = (%d, %d), want (2, 2)", gotMeta.RefVersion, gotMeta.ManifestVersion)
	}
	if ddbStore.ListRefsCalls() != listBeforePush {
		t.Fatalf("ListRefsCalls = %d, want %d (0 extra ListRefs after same-node push)", ddbStore.ListRefsCalls(), listBeforePush)
	}
	if len(gotRefs) != 2 || gotRefs[0].RefName != "refs/heads/feat" || gotRefs[0].OID != commit2 ||
		gotRefs[1].RefName != "refs/heads/main" || gotRefs[1].OID != commit2 {
		t.Fatalf("unexpected cached refs after push: %+v", gotRefs)
	}

	// Next SyncAndPin updates on-disk reftable with 0 ListRefs and 0 QueryManifestShard calls!
	synced, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(after push): %v", err)
	}
	mainOut, err := exec.Command("git", "-C", synced.Handle.Path(), "rev-parse", "refs/heads/main").CombinedOutput()
	featOut, featErr := exec.Command("git", "-C", synced.Handle.Path(), "rev-parse", "refs/heads/feat").CombinedOutput()
	synced.Handle.Release()
	if err != nil || strings.TrimSpace(string(mainOut)) != commit2 {
		t.Fatalf("reftable refs/heads/main = %q (err=%v), want %q", strings.TrimSpace(string(mainOut)), err, commit2)
	}
	if featErr != nil || strings.TrimSpace(string(featOut)) != commit2 {
		t.Fatalf("reftable refs/heads/feat = %q (err=%v), want %q", strings.TrimSpace(string(featOut)), featErr, commit2)
	}
	if ddbStore.ListRefsCalls() != listBeforePush {
		t.Fatalf("ListRefsCalls after SyncAndPin = %d, want %d", ddbStore.ListRefsCalls(), listBeforePush)
	}
	if ddbStore.QueryManifestCalls() != manifestBeforePush {
		t.Fatalf("QueryManifestCalls after SyncAndPin = %d, want %d", ddbStore.QueryManifestCalls(), manifestBeforePush)
	}

	// Simulate a racing push on another node (RefVersion jumps 2 -> 4): next LoadAuthoritativeRefs must reload via ListRefs.
	racedMeta := gotMeta
	racedMeta.RefVersion = 4
	ddbStore.PutMeta(racedMeta)
	if _, _, err := mat.LoadAuthoritativeRefs(ctx, rk); err != nil {
		t.Fatalf("LoadAuthoritativeRefs(after race): %v", err)
	}
	if ddbStore.ListRefsCalls() != listBeforePush+1 {
		t.Fatalf("ListRefsCalls after external race = %d, want %d", ddbStore.ListRefsCalls(), listBeforePush+1)
	}
}
