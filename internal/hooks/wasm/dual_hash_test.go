package wasm

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type dualHashStubReader struct {
	stubObjectReader
	commitsByHex []CommitMeta
	changesByHex []TreeChange
	blobsByHex   map[string][]byte
	lastOldHex   string
	lastNewHex   string
	lastBlobHex  string
}

var _ DualHashObjectReader = (*dualHashStubReader)(nil)

func (r *dualHashStubReader) ListNewCommitsByHex(oldOIDHex, newOIDHex string, limit uint32) ([]CommitMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitCalls++
	r.lastOldHex = oldOIDHex
	r.lastNewHex = newOIDHex
	if limit > 0 && int(limit) < len(r.commitsByHex) {
		return r.commitsByHex[:limit], nil
	}
	return r.commitsByHex, nil
}

func (r *dualHashStubReader) DiffTreeByHex(_ context.Context, oldTreeOIDHex, newTreeOIDHex string, limit uint32) ([]TreeChange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diffCalls++
	r.lastOldHex = oldTreeOIDHex
	r.lastNewHex = newTreeOIDHex
	if limit > 0 && int(limit) < len(r.changesByHex) {
		return r.changesByHex[:limit], nil
	}
	return r.changesByHex, nil
}

func (r *dualHashStubReader) ReadBlobChunkByHex(blobOIDHex string, offset uint64, maxBytes uint32) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blobCalls++
	r.lastBlobHex = blobOIDHex
	data, ok := r.blobsByHex[blobOIDHex]
	if !ok {
		return nil, errors.New("blob not found")
	}
	if offset >= uint64(len(data)) {
		return []byte{}, nil
	}
	end := offset + uint64(maxBytes)
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return data[offset:end], nil
}

// wasmV2HostCallsFixture builds a Wasm module that calls get_push_context,
// list_new_commits_v2, diff_tree_v2, read_blob_chunk_v2, and emits the
// get_push_context JSON payload as the verdict message.
func wasmV2HostCallsFixture(oldOID, newOID, oldTree, newTree, blobOID []byte) []byte {
	const (
		offOldOID  = 0
		offNewOID  = 80
		offOldTree = 160
		offNewTree = 240
		offBlobOID = 320
		offOutBuf  = 1024
		outCap     = 4096
	)

	var body bytes.Buffer
	// 1. list_new_commits_v2(oldOIDPtr, oldOIDLen, newOIDPtr, newOIDLen, 10, outBuf, outCap) -> drop
	body.Write(i32Const(offOldOID))
	body.Write(i32Const(int32(len(oldOID))))
	body.Write(i32Const(offNewOID))
	body.Write(i32Const(int32(len(newOID))))
	body.Write(i32Const(10))
	body.Write(i32Const(offOutBuf))
	body.Write(i32Const(outCap))
	body.Write(callOp(1))
	body.WriteByte(0x1a) // drop

	// 2. diff_tree_v2(oldTreePtr, oldTreeLen, newTreePtr, newTreeLen, 10, outBuf, outCap) -> drop
	body.Write(i32Const(offOldTree))
	body.Write(i32Const(int32(len(oldTree))))
	body.Write(i32Const(offNewTree))
	body.Write(i32Const(int32(len(newTree))))
	body.Write(i32Const(10))
	body.Write(i32Const(offOutBuf))
	body.Write(i32Const(outCap))
	body.Write(callOp(2))
	body.WriteByte(0x1a) // drop

	// 3. read_blob_chunk_v2(blobOIDPtr, blobOIDLen, 0, 64, outBuf, outCap) -> drop
	body.Write(i32Const(offBlobOID))
	body.Write(i32Const(int32(len(blobOID))))
	body.Write(i64Const(0))
	body.Write(i32Const(64))
	body.Write(i32Const(offOutBuf))
	body.Write(i32Const(outCap))
	body.Write(callOp(3))
	body.WriteByte(0x1a) // drop

	// 4. n = get_push_context(outBuf, outCap); set_verdict(0, outBuf, n)
	body.Write(i32Const(0)) // decision = ALLOW
	body.Write(i32Const(offOutBuf))
	body.Write(i32Const(offOutBuf))
	body.Write(i32Const(outCap))
	body.Write(callOp(0))
	body.Write(callOp(4))
	body.WriteByte(0x1a) // drop

	return buildWasmModule(wasmModuleSpec{
		types: []wasmFuncType{
			{params: []byte{wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI32, wasmValI32, wasmValI32, wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI32, wasmValI64, wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: nil, results: nil},
		},
		imports: []wasmImport{
			{module: "stravin", name: "get_push_context", typeIdx: 0},
			{module: "stravin", name: "list_new_commits_v2", typeIdx: 1},
			{module: "stravin", name: "diff_tree_v2", typeIdx: 1},
			{module: "stravin", name: "read_blob_chunk_v2", typeIdx: 2},
			{module: "stravin", name: "set_verdict", typeIdx: 3},
		},
		funcTypes: []uint32{4},
		exportIdx: 5,
		bodies:    [][]byte{body.Bytes()},
		dataSegs: []wasmDataSeg{
			{offset: offOldOID, data: oldOID},
			{offset: offNewOID, data: newOID},
			{offset: offOldTree, data: oldTree},
			{offset: offNewTree, data: newTree},
			{offset: offBlobOID, data: blobOID},
		},
	})
}

func TestWasmABIV2DualHashSHA1AndSHA256(t *testing.T) {
	sha256Zero := strings.Repeat("0", 64)
	sha256Commit := strings.Repeat("a", 64)
	sha256Tree := strings.Repeat("b", 64)
	sha256Blob := strings.Repeat("c", 64)
	sha256BlobRaw, _ := hex.DecodeString(sha256Blob)

	sha1Zero := strings.Repeat("0", 40)
	sha1Commit := strings.Repeat("d", 40)
	sha1Tree := strings.Repeat("e", 40)
	sha1Blob := strings.Repeat("f", 40)

	cases := []struct {
		name         string
		objectFormat string
		oldOIDWire   []byte
		newOIDWire   []byte
		oldTreeWire  []byte
		newTreeWire  []byte
		blobWire     []byte
		wantOldHex   string
		wantNewHex   string
		wantBlobHex  string
	}{
		{
			name:         "SHA256_64Hex_And_32Raw",
			objectFormat: ObjectFormatSHA256,
			oldOIDWire:   []byte(sha256Zero),
			newOIDWire:   []byte(sha256Commit),
			oldTreeWire:  []byte(sha256Zero),
			newTreeWire:  []byte(sha256Tree),
			blobWire:     sha256BlobRaw,
			wantOldHex:   sha256Zero,
			wantNewHex:   sha256Commit,
			wantBlobHex:  sha256Blob,
		},
		{
			name:         "SHA1_40Hex",
			objectFormat: ObjectFormatSHA1,
			oldOIDWire:   []byte(sha1Zero),
			newOIDWire:   []byte(sha1Commit),
			oldTreeWire:  []byte(sha1Zero),
			newTreeWire:  []byte(sha1Tree),
			blobWire:     []byte(sha1Blob),
			wantOldHex:   sha1Zero,
			wantNewHex:   sha1Commit,
			wantBlobHex:  sha1Blob,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			loader := newMemWasmLoader()
			wasmBytes := wasmV2HostCallsFixture(tc.oldOIDWire, tc.newOIDWire, tc.oldTreeWire, tc.newTreeWire, tc.blobWire)
			wasmSHA := loader.Put(wasmBytes)

			store := &staticConfigStore{
				hooks: []HookConfig{
					{
						HookID:     "dual-hash-hook",
						WasmSHA256: wasmSHA,
						RefGlob:    "refs/heads/*",
						TimeoutMS:  200,
						FailMode:   FailModeClosed,
					},
				},
			}
			eng := NewEngine(loader, store, EngineConfig{})
			defer func() { _ = eng.Close(context.Background()) }()

			reader := &dualHashStubReader{
				commitsByHex: []CommitMeta{
					{OIDHex: tc.wantNewHex, TreeOIDHex: tc.wantNewHex, Message: "dual-hash commit"},
				},
				changesByHex: []TreeChange{
					{Path: "README.md", Kind: ChangeAdd, NewOIDHex: tc.wantBlobHex, NewSize: 12},
				},
				blobsByHex: map[string][]byte{
					tc.wantBlobHex: []byte("hello world\n"),
				},
			}

			res, err := eng.EvaluatePush(context.Background(), PushContext{
				TenantID:     "acme",
				RepoID:       "repo-" + tc.objectFormat,
				ObjectFormat: tc.objectFormat,
				Updates: []RefUpdate{
					{
						Ref:       "refs/heads/main",
						OldOIDHex: tc.wantOldHex,
						NewOIDHex: tc.wantNewHex,
					},
				},
			}, reader)
			if err != nil {
				t.Fatalf("EvaluatePush(%s): %v", tc.name, err)
			}
			if !res.Allowed {
				t.Fatalf("expected push allowed, got rejected: %+v", res)
			}
			if reader.commitCalls != 1 || reader.diffCalls != 1 || reader.blobCalls != 1 {
				t.Fatalf("expected 1 call each (commit=%d diff=%d blob=%d)", reader.commitCalls, reader.diffCalls, reader.blobCalls)
			}
			if reader.lastBlobHex != tc.wantBlobHex {
				t.Fatalf("lastBlobHex = %q, want %q", reader.lastBlobHex, tc.wantBlobHex)
			}

			// Verify get_push_context returned object_format and exact hex OIDs.
			var wireCtx wirePushContext
			if err := json.Unmarshal([]byte(res.HookOutcomes[0].Message), &wireCtx); err != nil {
				t.Fatalf("unmarshal push context: %v (raw=%q)", err, res.HookOutcomes[0].Message)
			}
			if wireCtx.ObjectFormat != tc.objectFormat {
				t.Fatalf("wireCtx.ObjectFormat = %q, want %q", wireCtx.ObjectFormat, tc.objectFormat)
			}
			if len(wireCtx.Updates) != 1 || wireCtx.Updates[0].OldOID != tc.wantOldHex || wireCtx.Updates[0].NewOID != tc.wantNewHex {
				t.Fatalf("unexpected wireCtx.Updates: %+v", wireCtx.Updates)
			}
		})
	}
}
