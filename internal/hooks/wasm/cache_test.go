package wasm

import (
	"context"
	"strings"
	"testing"

	"github.com/tetratelabs/wazero"
)

func TestModuleCacheLRUEviction(t *testing.T) {
	ctx := context.Background()
	rt := wazero.NewRuntime(ctx)
	defer func() { _ = rt.Close(ctx) }()

	cache := newModuleCache(2)
	defer cache.Close(ctx)

	compile := func(msg string) (moduleCacheKey, wazero.CompiledModule) {
		b := wasmVerdictFixture(0, msg)
		cm, err := rt.CompileModule(ctx, b)
		if err != nil {
			t.Fatalf("CompileModule(%q): %v", msg, err)
		}
		return moduleCacheKey{sha256: sha256Hex(b), memoryLimitPages: 256}, cm
	}

	k1, cm1 := compile("m1")
	k2, cm2 := compile("m2")
	k3, cm3 := compile("m3")

	h1 := cache.Put(ctx, k1, cm1)
	h1.release(ctx)
	h2 := cache.Put(ctx, k2, cm2)
	h2.release(ctx)

	// Touch k1 so k2 becomes LRU.
	if hit, ok := cache.Get(k1); !ok {
		t.Fatal("expected k1 hit")
	} else {
		hit.release(ctx)
	}

	// Inserting k3 should evict k2.
	h3 := cache.Put(ctx, k3, cm3)
	h3.release(ctx)

	if cache.Len() != 2 {
		t.Fatalf("cache.Len() = %d, want 2", cache.Len())
	}
	if _, ok := cache.Get(k2); ok {
		t.Fatal("expected k2 to be evicted")
	}
	if hit, ok := cache.Get(k1); !ok {
		t.Fatal("expected k1 to remain cached")
	} else {
		hit.release(ctx)
	}
	if hit, ok := cache.Get(k3); !ok {
		t.Fatal("expected k3 to remain cached")
	} else {
		hit.release(ctx)
	}
}

func TestEngineRejectsCorruptWasmChecksum(t *testing.T) {
	loader := newMemWasmLoader()
	goodBytes := wasmVerdictFixture(0, "ok")
	wrongSHA := strings.Repeat("b", 64)
	loader.modules[wrongSHA] = goodBytes

	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "bad-sha", WasmSHA256: wrongSHA, RefGlob: "*", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{})
	defer func() { _ = eng.Close(context.Background()) }()

	res, err := eng.EvaluatePush(context.Background(), PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)}},
	}, &stubObjectReader{})
	if err != nil {
		t.Fatalf("EvaluatePush error: %v", err)
	}
	if res.Allowed {
		t.Fatal("expected checksum mismatch to reject under fail_mode=closed")
	}
	if !strings.Contains(res.Message, ErrChecksumMismatch.Error()) {
		t.Fatalf("expected checksum mismatch error in message, got %q", res.Message)
	}
}
