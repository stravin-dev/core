package wasm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// Minimal WebAssembly 1.0 binary encoder for deterministic test fixtures.
// Produces valid, inspectable .wasm modules with zero external toolchain dependencies.

const (
	wasmValI32 = 0x7f
	wasmValI64 = 0x7e

	wasmSecType   = 1
	wasmSecImport = 2
	wasmSecFunc   = 3
	wasmSecMemory = 5
	wasmSecExport = 7
	wasmSecCode   = 10
	wasmSecData   = 11
)

type wasmFuncType struct {
	params  []byte
	results []byte
}

type wasmImport struct {
	module  string
	name    string
	typeIdx uint32
}

type wasmDataSeg struct {
	offset uint32
	data   []byte
}

type wasmModuleSpec struct {
	types      []wasmFuncType
	imports    []wasmImport
	funcTypes  []uint32
	memMin     uint32
	memMax     uint32 // 0 means no max
	exportName string
	exportIdx  uint32
	bodies     [][]byte // raw instruction bytes (without local decls or trailing 0x0b)
	dataSegs   []wasmDataSeg
}

func encodeULEB128(v uint32) []byte {
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

func encodeSLEB128(v int64) []byte {
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

func encodeName(s string) []byte {
	out := encodeULEB128(uint32(len(s)))
	return append(out, []byte(s)...)
}

func encodeSection(id byte, payload []byte) []byte {
	var out []byte
	out = append(out, id)
	out = append(out, encodeULEB128(uint32(len(payload)))...)
	out = append(out, payload...)
	return out
}

func i32Const(v int32) []byte {
	return append([]byte{0x41}, encodeSLEB128(int64(v))...)
}

func i64Const(v int64) []byte {
	return append([]byte{0x42}, encodeSLEB128(v)...)
}

func callOp(idx uint32) []byte {
	return append([]byte{0x10}, encodeULEB128(idx)...)
}

func buildWasmModule(spec wasmModuleSpec) []byte {
	var buf bytes.Buffer
	// Magic + version 1
	buf.Write([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00})

	// Type section
	if len(spec.types) > 0 {
		var sec bytes.Buffer
		sec.Write(encodeULEB128(uint32(len(spec.types))))
		for _, ft := range spec.types {
			sec.WriteByte(0x60)
			sec.Write(encodeULEB128(uint32(len(ft.params))))
			sec.Write(ft.params)
			sec.Write(encodeULEB128(uint32(len(ft.results))))
			sec.Write(ft.results)
		}
		buf.Write(encodeSection(wasmSecType, sec.Bytes()))
	}

	// Import section
	if len(spec.imports) > 0 {
		var sec bytes.Buffer
		sec.Write(encodeULEB128(uint32(len(spec.imports))))
		for _, imp := range spec.imports {
			sec.Write(encodeName(imp.module))
			sec.Write(encodeName(imp.name))
			sec.WriteByte(0x00) // func import
			sec.Write(encodeULEB128(imp.typeIdx))
		}
		buf.Write(encodeSection(wasmSecImport, sec.Bytes()))
	}

	// Function section
	if len(spec.funcTypes) > 0 {
		var sec bytes.Buffer
		sec.Write(encodeULEB128(uint32(len(spec.funcTypes))))
		for _, ti := range spec.funcTypes {
			sec.Write(encodeULEB128(ti))
		}
		buf.Write(encodeSection(wasmSecFunc, sec.Bytes()))
	}

	// Memory section
	{
		minPages := spec.memMin
		if minPages == 0 {
			minPages = 1
		}
		var sec bytes.Buffer
		sec.Write(encodeULEB128(1))
		if spec.memMax > 0 {
			sec.WriteByte(0x01)
			sec.Write(encodeULEB128(minPages))
			sec.Write(encodeULEB128(spec.memMax))
		} else {
			sec.WriteByte(0x00)
			sec.Write(encodeULEB128(minPages))
		}
		buf.Write(encodeSection(wasmSecMemory, sec.Bytes()))
	}

	// Export section: export "memory" + spec.exportName
	{
		exportName := spec.exportName
		if exportName == "" {
			exportName = "run"
		}
		var sec bytes.Buffer
		sec.Write(encodeULEB128(2))
		sec.Write(encodeName("memory"))
		sec.WriteByte(0x02) // memory export
		sec.Write(encodeULEB128(0))

		sec.Write(encodeName(exportName))
		sec.WriteByte(0x00) // func export
		sec.Write(encodeULEB128(spec.exportIdx))
		buf.Write(encodeSection(wasmSecExport, sec.Bytes()))
	}

	// Code section
	if len(spec.bodies) > 0 {
		var sec bytes.Buffer
		sec.Write(encodeULEB128(uint32(len(spec.bodies))))
		for _, body := range spec.bodies {
			var fn bytes.Buffer
			fn.WriteByte(0x00) // 0 local decls
			fn.Write(body)
			fn.WriteByte(0x0b) // end
			sec.Write(encodeULEB128(uint32(fn.Len())))
			sec.Write(fn.Bytes())
		}
		buf.Write(encodeSection(wasmSecCode, sec.Bytes()))
	}

	// Data section
	if len(spec.dataSegs) > 0 {
		var sec bytes.Buffer
		sec.Write(encodeULEB128(uint32(len(spec.dataSegs))))
		for _, ds := range spec.dataSegs {
			sec.WriteByte(0x00) // active memory 0
			sec.Write(i32Const(int32(ds.offset)))
			sec.WriteByte(0x0b) // end offset expr
			sec.Write(encodeULEB128(uint32(len(ds.data))))
			sec.Write(ds.data)
		}
		buf.Write(encodeSection(wasmSecData, sec.Bytes()))
	}

	return buf.Bytes()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// wasmVerdictFixture builds a Wasm hook calling stravin.set_verdict(decision, msgPtr, msgLen).
func wasmVerdictFixture(decision int32, msg string) []byte {
	const msgOffset = 1024
	var body bytes.Buffer
	body.Write(i32Const(decision))
	body.Write(i32Const(msgOffset))
	body.Write(i32Const(int32(len(msg))))
	body.Write(callOp(0)) // stravin.set_verdict
	body.WriteByte(0x1a)  // drop
	body.Write(i32Const(0))

	return buildWasmModule(wasmModuleSpec{
		types: []wasmFuncType{
			{params: []byte{wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}}, // type 0: set_verdict
			{params: nil, results: []byte{wasmValI32}},                                        // type 1: run
		},
		imports: []wasmImport{
			{module: "stravin", name: "set_verdict", typeIdx: 0},
		},
		funcTypes: []uint32{1},
		memMin:    1,
		exportIdx: 1, // func 0 is import, func 1 is run
		bodies:    [][]byte{body.Bytes()},
		dataSegs:  []wasmDataSeg{{offset: msgOffset, data: []byte(msg)}},
	})
}

// wasmInfiniteLoopFixture builds a Wasm hook that loops forever until context cancellation.
func wasmInfiniteLoopFixture() []byte {
	// loop (br 0) end; i32.const 0
	body := []byte{
		0x03, 0x40, // loop (void)
		0x0c, 0x00, // br 0
		0x0b,       // end
		0x41, 0x00, // i32.const 0
	}
	return buildWasmModule(wasmModuleSpec{
		types:     []wasmFuncType{{params: nil, results: []byte{wasmValI32}}},
		funcTypes: []uint32{0},
		memMin:    1,
		exportIdx: 0,
		bodies:    [][]byte{body},
	})
}

// wasmTrapFixture builds a Wasm hook that traps immediately via unreachable.
func wasmTrapFixture() []byte {
	return buildWasmModule(wasmModuleSpec{
		types:     []wasmFuncType{{params: nil, results: []byte{wasmValI32}}},
		funcTypes: []uint32{0},
		memMin:    1,
		exportIdx: 0,
		bodies:    [][]byte{{0x00}}, // unreachable
	})
}

// wasmMemoryExceedFixture declares initial memory larger than the configured limit.
func wasmMemoryExceedFixture(minPages uint32) []byte {
	return buildWasmModule(wasmModuleSpec{
		types:     []wasmFuncType{{params: nil, results: []byte{wasmValI32}}},
		funcTypes: []uint32{0},
		memMin:    minPages,
		exportIdx: 0,
		bodies:    [][]byte{i32Const(0)},
	})
}

// wasmABIExerciserFixture invokes all 5 stravin host ABI functions and emits ALLOW if all succeed,
// or REJECT if any host call returns a negative status code.
func wasmABIExerciserFixture(oldOID, newOID, oldTreeOID, newTreeOID, blobOID [20]byte) []byte {
	const (
		offOldOID  = 100
		offNewOID  = 120
		offOldTree = 140
		offNewTree = 160
		offBlobOID = 180
		offOKMsg   = 200
		offErrMsg  = 220
		offBuf     = 4096
		bufCap     = 4096
	)
	okMsg := "abi-ok"
	errMsg := "abi-fail"

	// Imports:
	// 0: get_push_context(outPtr, outCap) -> i32            [type 0]
	// 1: list_new_commits(old, new, limit, out, cap) -> i32 [type 1]
	// 2: diff_tree(old, new, limit, out, cap) -> i32        [type 1]
	// 3: read_blob_chunk(oid, offset64, max, out, cap) -> i32 [type 2]
	// 4: set_verdict(decision, ptr, len) -> i32             [type 3]
	// Local func 5: run() -> i32                            [type 4]

	var body bytes.Buffer
	// helper: if result < 0, jump to fail block
	// block $fail (void)
	//   call get_push_context -> i32.const 0; i32.lt_s; br_if 0
	//   ...
	//   call set_verdict(0, offOKMsg, len(okMsg)); drop; i32.const 0; return
	// end
	// call set_verdict(1, offErrMsg, len(errMsg)); drop; i32.const 0
	body.Write([]byte{0x02, 0x40}) // block (void)

	// 1. get_push_context(offBuf, bufCap)
	body.Write(i32Const(offBuf))
	body.Write(i32Const(bufCap))
	body.Write(callOp(0))
	body.Write(i32Const(0))
	body.WriteByte(0x48)        // i32.lt_s
	body.Write([]byte{0x0d, 0}) // br_if 0

	// 2. list_new_commits(offOldOID, offNewOID, 10, offBuf, bufCap)
	body.Write(i32Const(offOldOID))
	body.Write(i32Const(offNewOID))
	body.Write(i32Const(10))
	body.Write(i32Const(offBuf))
	body.Write(i32Const(bufCap))
	body.Write(callOp(1))
	body.Write(i32Const(0))
	body.WriteByte(0x48)
	body.Write([]byte{0x0d, 0})

	// 3. diff_tree(offOldTree, offNewTree, 10, offBuf, bufCap)
	body.Write(i32Const(offOldTree))
	body.Write(i32Const(offNewTree))
	body.Write(i32Const(10))
	body.Write(i32Const(offBuf))
	body.Write(i32Const(bufCap))
	body.Write(callOp(2))
	body.Write(i32Const(0))
	body.WriteByte(0x48)
	body.Write([]byte{0x0d, 0})

	// 4. read_blob_chunk(offBlobOID, 0, 64, offBuf, bufCap)
	body.Write(i32Const(offBlobOID))
	body.Write(i64Const(0))
	body.Write(i32Const(64))
	body.Write(i32Const(offBuf))
	body.Write(i32Const(bufCap))
	body.Write(callOp(3))
	body.Write(i32Const(0))
	body.WriteByte(0x48)
	body.Write([]byte{0x0d, 0})

	// All succeeded -> set_verdict(ALLOW, "abi-ok")
	body.Write(i32Const(0))
	body.Write(i32Const(offOKMsg))
	body.Write(i32Const(int32(len(okMsg))))
	body.Write(callOp(4))
	body.WriteByte(0x1a) // drop
	body.Write(i32Const(0))
	body.WriteByte(0x0f) // return

	body.WriteByte(0x0b) // end block

	// Failure path -> set_verdict(REJECT, "abi-fail")
	body.Write(i32Const(1))
	body.Write(i32Const(offErrMsg))
	body.Write(i32Const(int32(len(errMsg))))
	body.Write(callOp(4))
	body.WriteByte(0x1a) // drop
	body.Write(i32Const(0))

	return buildWasmModule(wasmModuleSpec{
		types: []wasmFuncType{
			{params: []byte{wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI32, wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI64, wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: nil, results: []byte{wasmValI32}},
		},
		imports: []wasmImport{
			{module: "stravin", name: "get_push_context", typeIdx: 0},
			{module: "stravin", name: "list_new_commits", typeIdx: 1},
			{module: "stravin", name: "diff_tree", typeIdx: 1},
			{module: "stravin", name: "read_blob_chunk", typeIdx: 2},
			{module: "stravin", name: "set_verdict", typeIdx: 3},
		},
		funcTypes: []uint32{4},
		memMin:    1,
		exportIdx: 5,
		bodies:    [][]byte{body.Bytes()},
		dataSegs: []wasmDataSeg{
			{offset: offOldOID, data: oldOID[:]},
			{offset: offNewOID, data: newOID[:]},
			{offset: offOldTree, data: oldTreeOID[:]},
			{offset: offNewTree, data: newTreeOID[:]},
			{offset: offBlobOID, data: blobOID[:]},
			{offset: offOKMsg, data: []byte(okMsg)},
			{offset: offErrMsg, data: []byte(errMsg)},
		},
	})
}

// wasmBlobBudgetSpammerFixture repeatedly calls read_blob_chunk until it receives a negative error code,
// then sets a REJECT verdict with "budget-hit".
func wasmBlobBudgetSpammerFixture(blobOID [20]byte, chunkBytes int32) []byte {
	const (
		offBlobOID = 100
		offMsg     = 200
		offBuf     = 4096
		bufCap     = 4096
	)
	msg := "budget-hit"

	// Imports:
	// 0: read_blob_chunk(oid, offset64, max, out, cap) -> i32 [type 0]
	// 1: set_verdict(decision, ptr, len) -> i32               [type 1]
	// Func 2: run() -> i32                                    [type 2]
	var body bytes.Buffer
	body.Write([]byte{0x03, 0x40}) // loop
	body.Write(i32Const(offBlobOID))
	body.Write(i64Const(0))
	body.Write(i32Const(chunkBytes))
	body.Write(i32Const(offBuf))
	body.Write(i32Const(bufCap))
	body.Write(callOp(0))
	body.Write(i32Const(0))
	body.WriteByte(0x4e)        // i32.ge_s (if >= 0 keep looping)
	body.Write([]byte{0x0d, 0}) // br_if 0
	body.WriteByte(0x0b)        // end loop

	body.Write(i32Const(1))
	body.Write(i32Const(offMsg))
	body.Write(i32Const(int32(len(msg))))
	body.Write(callOp(1))
	body.WriteByte(0x1a) // drop
	body.Write(i32Const(0))

	return buildWasmModule(wasmModuleSpec{
		types: []wasmFuncType{
			{params: []byte{wasmValI32, wasmValI64, wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: []byte{wasmValI32, wasmValI32, wasmValI32}, results: []byte{wasmValI32}},
			{params: nil, results: []byte{wasmValI32}},
		},
		imports: []wasmImport{
			{module: "stravin", name: "read_blob_chunk", typeIdx: 0},
			{module: "stravin", name: "set_verdict", typeIdx: 1},
		},
		funcTypes: []uint32{2},
		memMin:    1,
		exportIdx: 2,
		bodies:    [][]byte{body.Bytes()},
		dataSegs: []wasmDataSeg{
			{offset: offBlobOID, data: blobOID[:]},
			{offset: offMsg, data: []byte(msg)},
		},
	})
}
