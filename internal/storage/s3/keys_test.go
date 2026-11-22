package s3

import (
	"errors"
	"strings"
	"testing"

	"stravin.dev/core/internal/tenant"
)

const (
	testPackSHA = "a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0"
	testWasmSHA = "ff00112233445566778899aabbccddeeff00112233445566778899aabbccdde0"
)

func mustRepoKey(t *testing.T, tenantID, repoID string) tenant.RepoKey {
	t.Helper()
	rk, err := tenant.NewRepoKey(tenantID, repoID)
	if err != nil {
		t.Fatalf("NewRepoKey(%q, %q): %v", tenantID, repoID, err)
	}
	return rk
}

func TestKeyLayouts(t *testing.T) {
	rk := mustRepoKey(t, "acme", "core")

	gotPack, err := PackKey(rk, strings.ToUpper(testPackSHA))
	if err != nil {
		t.Fatalf("PackKey: %v", err)
	}
	wantPack := "a1/acme/core/packs/" + testPackSHA + ".pack"
	if gotPack != wantPack {
		t.Errorf("PackKey = %q, want %q", gotPack, wantPack)
	}

	gotIdx, err := IndexKey(rk, testPackSHA)
	if err != nil {
		t.Fatalf("IndexKey: %v", err)
	}
	wantIdx := "a1/acme/core/packs/" + testPackSHA + ".idx"
	if gotIdx != wantIdx {
		t.Errorf("IndexKey = %q, want %q", gotIdx, wantIdx)
	}

	gotMIDX, err := AuxMIDXKey(rk, testPackSHA, "epoch-42")
	if err != nil {
		t.Fatalf("AuxMIDXKey (full sha): %v", err)
	}
	wantMIDX := "a1/acme/core/aux/epoch-42.midx"
	if gotMIDX != wantMIDX {
		t.Errorf("AuxMIDXKey = %q, want %q", gotMIDX, wantMIDX)
	}

	gotMIDXShort, err := AuxMIDXKey(rk, "A1", "epoch-42")
	if err != nil {
		t.Fatalf("AuxMIDXKey (2-hex prefix): %v", err)
	}
	if gotMIDXShort != wantMIDX {
		t.Errorf("AuxMIDXKey(2-hex) = %q, want %q", gotMIDXShort, wantMIDX)
	}

	gotGraph, err := AuxGraphKey(rk, testPackSHA, "epoch-42")
	if err != nil {
		t.Fatalf("AuxGraphKey: %v", err)
	}
	wantGraph := "a1/acme/core/aux/epoch-42.graph"
	if gotGraph != wantGraph {
		t.Errorf("AuxGraphKey = %q, want %q", gotGraph, wantGraph)
	}

	gotWasm, err := WasmHookKey(strings.ToUpper(testWasmSHA))
	if err != nil {
		t.Fatalf("WasmHookKey: %v", err)
	}
	wantWasm := "hooks/" + testWasmSHA + ".wasm"
	if gotWasm != wantWasm {
		t.Errorf("WasmHookKey = %q, want %q", gotWasm, wantWasm)
	}
}

func TestKeyValidationErrors(t *testing.T) {
	rk := mustRepoKey(t, "acme", "core")
	badRepo := tenant.RepoKey{TenantID: "", RepoID: "core"}

	if _, err := PackKey(badRepo, testPackSHA); err == nil {
		t.Error("expected invalid repo error for PackKey")
	}
	for _, badSHA := range []string{"", "abc", strings.Repeat("z", 64), testPackSHA + "00"} {
		if _, err := PackKey(rk, badSHA); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("PackKey(%q) err = %v, want ErrInvalidInput", badSHA, err)
		}
		if _, err := IndexKey(rk, badSHA); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("IndexKey(%q) err = %v, want ErrInvalidInput", badSHA, err)
		}
		if _, err := WasmHookKey(badSHA); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("WasmHookKey(%q) err = %v, want ErrInvalidInput", badSHA, err)
		}
	}

	for _, badPrefix := range []string{"", "a", "abc", "zz"} {
		if _, err := AuxMIDXKey(rk, badPrefix, "epoch-1"); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("AuxMIDXKey prefix=%q err = %v, want ErrInvalidInput", badPrefix, err)
		}
	}

	for _, badEpoch := range []string{"", "../escape", "a/b", "has space"} {
		if _, err := AuxGraphKey(rk, "a1", badEpoch); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("AuxGraphKey epoch=%q err = %v, want ErrInvalidInput", badEpoch, err)
		}
	}
}
