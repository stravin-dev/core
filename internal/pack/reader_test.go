package pack

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func buildTreePayload(entries []TreeEntry) []byte {
	var buf bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&buf, "%o %s\x00", e.Mode, e.Name)
		buf.Write(e.OID.Bytes())
	}
	return buf.Bytes()
}

func buildCommitPayload(treeOID OID, parents []OID, author, committer, msg string) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "tree %s\n", treeOID)
	for _, p := range parents {
		fmt.Fprintf(&buf, "parent %s\n", p)
	}
	fmt.Fprintf(&buf, "author %s\n", author)
	fmt.Fprintf(&buf, "committer %s\n\n%s", committer, msg)
	return buf.Bytes()
}

func TestPackReaderCommitsTreesAndBlobs(t *testing.T) {
	blobA1 := []byte("hello world v1")
	blobA2 := []byte("hello world v2 with more bytes")
	blobB := []byte("new nested file")
	blobC := []byte("file to delete")

	oidA1 := ComputeOID(ObjBlob, blobA1)
	oidA2 := ComputeOID(ObjBlob, blobA2)
	oidB := ComputeOID(ObjBlob, blobB)
	oidC := ComputeOID(ObjBlob, blobC)

	tree1Data := buildTreePayload([]TreeEntry{
		{Mode: 0o100644, Name: "a.txt", OID: oidA1},
		{Mode: 0o100644, Name: "c.txt", OID: oidC},
	})
	tree1OID := ComputeOID(ObjTree, tree1Data)

	subTree2Data := buildTreePayload([]TreeEntry{
		{Mode: 0o100644, Name: "b.txt", OID: oidB},
	})
	subTree2OID := ComputeOID(ObjTree, subTree2Data)

	tree2Data := buildTreePayload([]TreeEntry{
		{Mode: 0o100644, Name: "a.txt", OID: oidA2},
		{Mode: 0o040000, Name: "dir", OID: subTree2OID},
	})
	tree2OID := ComputeOID(ObjTree, tree2Data)

	c1Data := buildCommitPayload(tree1OID, nil, "Alice <alice@stravin.dev> 1700000000 +0000", "Alice <alice@stravin.dev> 1700000000 +0000", "initial commit\n")
	c1OID := ComputeOID(ObjCommit, c1Data)

	c2Data := buildCommitPayload(tree2OID, []OID{c1OID}, "Bob <bob@stravin.dev> 1700000100 -0800", "Bob <bob@stravin.dev> 1700000100 -0800", "update a, add dir/b, delete c\n")
	c2OID := ComputeOID(ObjCommit, c2Data)

	packBytes := buildRawPack(t, 2, []rawPackEntry{
		{typ: ObjBlob, data: blobA1},
		{typ: ObjBlob, data: blobA2},
		{typ: ObjBlob, data: blobB},
		{typ: ObjBlob, data: blobC},
		{typ: ObjTree, data: tree1Data},
		{typ: ObjTree, data: subTree2Data},
		{typ: ObjTree, data: tree2Data},
		{typ: ObjCommit, data: c1Data},
		{typ: ObjCommit, data: c2Data},
	})

	parsed, err := Parse(bytes.NewReader(packBytes), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	reader := NewPackReader(parsed, nil)

	t.Run("ListNewCommits incremental", func(t *testing.T) {
		commits, err := reader.ListNewCommits(c1OID, c2OID, 10)
		if err != nil {
			t.Fatalf("ListNewCommits: %v", err)
		}
		if len(commits) != 1 {
			t.Fatalf("got %d commits, want 1", len(commits))
		}
		if commits[0].OID != c2OID || commits[0].TreeOID != tree2OID || commits[0].Author.Name != "Bob" || commits[0].Author.Email != "bob@stravin.dev" {
			t.Fatalf("unexpected commit meta: %+v", commits[0])
		}
		if commits[0].Message != "update a, add dir/b, delete c\n" {
			t.Fatalf("unexpected message: %q", commits[0].Message)
		}
	})

	t.Run("ListNewCommits from zero OID with limit", func(t *testing.T) {
		all, err := reader.ListNewCommits(OID{}, c2OID, 10)
		if err != nil {
			t.Fatalf("ListNewCommits all: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("got %d commits, want 2", len(all))
		}
		limited, err := reader.ListNewCommits(OID{}, c2OID, 1)
		if err != nil {
			t.Fatalf("ListNewCommits limited: %v", err)
		}
		if len(limited) != 1 || limited[0].OID != c2OID {
			t.Fatalf("unexpected limited result: %+v", limited)
		}
	})

	t.Run("ListNewCommits branch deletion", func(t *testing.T) {
		commits, err := reader.ListNewCommits(c2OID, OID{}, 10)
		if err != nil {
			t.Fatalf("ListNewCommits delete: %v", err)
		}
		if len(commits) != 0 {
			t.Fatalf("got %d commits, want 0", len(commits))
		}
	})

	t.Run("DiffTree add modify delete nested", func(t *testing.T) {
		changes, err := reader.DiffTree(tree1OID, tree2OID, 100)
		if err != nil {
			t.Fatalf("DiffTree: %v", err)
		}
		if len(changes) != 3 {
			t.Fatalf("got %d changes, want 3: %+v", len(changes), changes)
		}
		byPath := make(map[string]TreeChange, len(changes))
		for _, ch := range changes {
			byPath[ch.Path] = ch
		}

		chA := byPath["a.txt"]
		if chA.Kind != ChangeModify || chA.OldOID != oidA1 || chA.NewOID != oidA2 || chA.OldSize != int64(len(blobA1)) || chA.NewSize != int64(len(blobA2)) {
			t.Fatalf("unexpected change for a.txt: %+v", chA)
		}
		chB := byPath["dir/b.txt"]
		if chB.Kind != ChangeAdd || chB.NewOID != oidB || chB.NewSize != int64(len(blobB)) {
			t.Fatalf("unexpected change for dir/b.txt: %+v", chB)
		}
		chC := byPath["c.txt"]
		if chC.Kind != ChangeDelete || chC.OldOID != oidC || chC.OldSize != int64(len(blobC)) {
			t.Fatalf("unexpected change for c.txt: %+v", chC)
		}
	})

	t.Run("DiffTree initial commit and limit", func(t *testing.T) {
		changes, err := reader.DiffTree(OID{}, tree2OID, 1)
		if err != nil {
			t.Fatalf("DiffTree initial: %v", err)
		}
		if len(changes) != 1 {
			t.Fatalf("got %d changes, want 1", len(changes))
		}
	})

	t.Run("ReadBlobChunk bounds and type check", func(t *testing.T) {
		chunk, err := reader.ReadBlobChunk(oidA2, 6, 5)
		if err != nil {
			t.Fatalf("ReadBlobChunk middle: %v", err)
		}
		if string(chunk) != "world" {
			t.Fatalf("got %q, want %q", string(chunk), "world")
		}

		tail, err := reader.ReadBlobChunk(oidA2, uint64(len(blobA2)-5), 100)
		if err != nil {
			t.Fatalf("ReadBlobChunk tail: %v", err)
		}
		if string(tail) != "bytes" {
			t.Fatalf("got %q, want %q", string(tail), "bytes")
		}

		eof, err := reader.ReadBlobChunk(oidA2, uint64(len(blobA2)), 10)
		if err != nil {
			t.Fatalf("ReadBlobChunk eof: %v", err)
		}
		if len(eof) != 0 {
			t.Fatalf("got %d bytes at EOF, want 0", len(eof))
		}

		if _, err := reader.ReadBlobChunk(c2OID, 0, 10); !errors.Is(err, ErrInvalidObjectType) {
			t.Fatalf("ReadBlobChunk on commit: got %v, want ErrInvalidObjectType", err)
		}
	})
}

func TestPackReaderDiffTreeMaxDepthExceeded(t *testing.T) {
	objs := make(map[OID]*Object)
	leafData := []byte("deep-leaf")
	leafOID := ComputeOID(ObjBlob, leafData)
	objs[leafOID] = &Object{OID: leafOID, Type: ObjBlob, Data: leafData}

	leafTreeData := buildTreePayload([]TreeEntry{
		{Mode: 0o100644, Name: "leaf.txt", OID: leafOID},
	})
	currOID := ComputeOID(ObjTree, leafTreeData)
	objs[currOID] = &Object{OID: currOID, Type: ObjTree, Data: leafTreeData}

	var shallowOID OID
	for i := 1; i <= MaxTreeDepth+2; i++ {
		data := buildTreePayload([]TreeEntry{
			{Mode: treeDirMode, Name: fmt.Sprintf("d%d", i), OID: currOID},
		})
		oid := ComputeOID(ObjTree, data)
		objs[oid] = &Object{OID: oid, Type: ObjTree, Data: data}
		currOID = oid
		if i == 10 {
			shallowOID = oid
		}
	}

	reader := NewPackReader(&ParsedPack{Objects: objs}, nil)

	t.Run("within max depth succeeds", func(t *testing.T) {
		changes, err := reader.DiffTree(OID{}, shallowOID, 10)
		if err != nil {
			t.Fatalf("DiffTree shallow: %v", err)
		}
		if len(changes) != 1 {
			t.Fatalf("got %d changes, want 1", len(changes))
		}
	})

	t.Run("exceeding max depth returns ErrMaxTreeDepthExceeded", func(t *testing.T) {
		_, err := reader.DiffTree(OID{}, currOID, 100)
		if !errors.Is(err, ErrMaxTreeDepthExceeded) {
			t.Fatalf("DiffTree deep: got %v, want ErrMaxTreeDepthExceeded", err)
		}
	})
}

func TestPackReaderDiffTreeSharedSubtreeDAGBlowup(t *testing.T) {
	buildBinaryDAG := func(leafData []byte) (*PackReader, OID) {
		objs := make(map[OID]*Object)
		var baseTreeData []byte
		if leafData != nil {
			leafOID := ComputeOID(ObjBlob, leafData)
			objs[leafOID] = &Object{OID: leafOID, Type: ObjBlob, Data: leafData}
			baseTreeData = buildTreePayload([]TreeEntry{
				{Mode: 0o100644, Name: "leaf.txt", OID: leafOID},
			})
		}
		currOID := ComputeOID(ObjTree, baseTreeData)
		objs[currOID] = &Object{OID: currOID, Type: ObjTree, Data: baseTreeData}

		for range 32 {
			data := buildTreePayload([]TreeEntry{
				{Mode: treeDirMode, Name: "a", OID: currOID},
				{Mode: treeDirMode, Name: "b", OID: currOID},
			})
			oid := ComputeOID(ObjTree, data)
			objs[oid] = &Object{OID: oid, Type: ObjTree, Data: data}
			currOID = oid
		}
		return NewPackReader(&ParsedPack{Objects: objs}, nil), currOID
	}

	t.Run("depth-32 binary DAG with leaf terminates fast", func(t *testing.T) {
		reader, rootOID := buildBinaryDAG([]byte("payload"))
		start := time.Now()
		changes, err := reader.DiffTree(OID{}, rootOID, MaxDiffTreeLimit)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("DiffTree DAG with leaf: %v", err)
		}
		if elapsed > time.Second {
			t.Fatalf("DiffTree DAG took too long: %v", elapsed)
		}
		if len(changes) == 0 {
			t.Fatal("expected at least one change from non-empty DAG")
		}
	})

	t.Run("depth-32 binary DAG with empty leaves terminates fast", func(t *testing.T) {
		reader, rootOID := buildBinaryDAG(nil)
		start := time.Now()
		changes, err := reader.DiffTree(OID{}, rootOID, MaxDiffTreeLimit)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("DiffTree empty DAG: %v", err)
		}
		if elapsed > time.Second {
			t.Fatalf("DiffTree empty DAG took too long: %v", elapsed)
		}
		if len(changes) != 0 {
			t.Fatalf("got %d changes, want 0", len(changes))
		}
	})
}

func TestPackReaderDiffTreeLimitClamping(t *testing.T) {
	const totalFiles = MaxDiffTreeLimit + 25
	objs := make(map[OID]*Object, totalFiles+1)
	blobData := []byte("x")
	blobOID := ComputeOID(ObjBlob, blobData)
	objs[blobOID] = &Object{OID: blobOID, Type: ObjBlob, Data: blobData}

	entries := make([]TreeEntry, totalFiles)
	for i := range totalFiles {
		entries[i] = TreeEntry{
			Mode: 0o100644,
			Name: fmt.Sprintf("f%05d.txt", i),
			OID:  blobOID,
		}
	}
	treeData := buildTreePayload(entries)
	treeOID := ComputeOID(ObjTree, treeData)
	objs[treeOID] = &Object{OID: treeOID, Type: ObjTree, Data: treeData}

	reader := NewPackReader(&ParsedPack{Objects: objs}, nil)

	t.Run("zero limit uses DefaultDiffTreeLimit", func(t *testing.T) {
		changes, err := reader.DiffTree(OID{}, treeOID, 0)
		if err != nil {
			t.Fatalf("DiffTree default limit: %v", err)
		}
		if len(changes) != DefaultDiffTreeLimit {
			t.Fatalf("got %d changes, want %d", len(changes), DefaultDiffTreeLimit)
		}
	})

	t.Run("excessive limit is clamped to MaxDiffTreeLimit", func(t *testing.T) {
		changes, err := reader.DiffTree(OID{}, treeOID, MaxDiffTreeLimit+5000)
		if err != nil {
			t.Fatalf("DiffTree max limit: %v", err)
		}
		if len(changes) != MaxDiffTreeLimit {
			t.Fatalf("got %d changes, want %d", len(changes), MaxDiffTreeLimit)
		}
	})
}

func TestPackReaderDiffTreeContextCancellation(t *testing.T) {
	objs := make(map[OID]*Object)
	blobData := []byte("hello")
	blobOID := ComputeOID(ObjBlob, blobData)
	objs[blobOID] = &Object{OID: blobOID, Type: ObjBlob, Data: blobData}

	subEntries := make([]TreeEntry, 20)
	for i := range 20 {
		subData := buildTreePayload([]TreeEntry{{Mode: 0o100644, Name: fmt.Sprintf("b%d.txt", i), OID: blobOID}})
		subOID := ComputeOID(ObjTree, subData)
		objs[subOID] = &Object{OID: subOID, Type: ObjTree, Data: subData}
		subEntries[i] = TreeEntry{Mode: treeDirMode, Name: fmt.Sprintf("d%d", i), OID: subOID}
	}
	rootData := buildTreePayload(subEntries)
	rootOID := ComputeOID(ObjTree, rootData)
	objs[rootOID] = &Object{OID: rootOID, Type: ObjTree, Data: rootData}

	t.Run("pre-cancelled context aborts immediately", func(t *testing.T) {
		reads := 0
		reader := NewPackReader(nil, FuncBaseResolver(func(oid OID) (ObjectType, []byte, error) {
			reads++
			obj, ok := objs[oid]
			if !ok {
				return 0, nil, ErrObjectNotFound
			}
			return obj.Type, obj.Data, nil
		}))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := reader.DiffTreeContext(ctx, OID{}, rootOID, 100)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
		if reads != 0 {
			t.Fatalf("expected 0 reads on pre-cancelled context, got %d", reads)
		}
	})

	t.Run("mid-traversal cancellation aborts promptly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reads := 0
		reader := NewPackReader(nil, FuncBaseResolver(func(oid OID) (ObjectType, []byte, error) {
			reads++
			if reads == 3 {
				cancel()
			}
			obj, ok := objs[oid]
			if !ok {
				return 0, nil, ErrObjectNotFound
			}
			return obj.Type, obj.Data, nil
		}))

		_, err := reader.DiffTreeContext(ctx, OID{}, rootOID, 100)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
		if reads > 5 {
			t.Fatalf("expected traversal to abort near read 3, got %d reads", reads)
		}
	})
}

func TestPackReaderDiffTreeVisitedNodeBudget(t *testing.T) {
	const numDirs = MaxTreeNodesVisited + 5
	objs := make(map[OID]*Object, numDirs+2)

	emptyOID := ComputeOID(ObjTree, nil)
	objs[emptyOID] = &Object{OID: emptyOID, Type: ObjTree, Data: nil}

	oldEntries := make([]TreeEntry, numDirs)
	newEntries := make([]TreeEntry, numDirs)
	for i := range numDirs {
		// Each oldSub_i has a unique entry name pointing to emptyOID, producing
		// a distinct OID per subtree while emitting zero file changes.
		subData := buildTreePayload([]TreeEntry{
			{Mode: treeDirMode, Name: fmt.Sprintf("e%d", i), OID: emptyOID},
		})
		subOID := ComputeOID(ObjTree, subData)
		objs[subOID] = &Object{OID: subOID, Type: ObjTree, Data: subData}

		name := fmt.Sprintf("d%06d", i)
		oldEntries[i] = TreeEntry{Mode: treeDirMode, Name: name, OID: subOID}
		newEntries[i] = TreeEntry{Mode: treeDirMode, Name: name, OID: emptyOID}
	}

	oldRootData := buildTreePayload(oldEntries)
	oldRootOID := ComputeOID(ObjTree, oldRootData)
	objs[oldRootOID] = &Object{OID: oldRootOID, Type: ObjTree, Data: oldRootData}

	newRootData := buildTreePayload(newEntries)
	newRootOID := ComputeOID(ObjTree, newRootData)
	objs[newRootOID] = &Object{OID: newRootOID, Type: ObjTree, Data: newRootData}

	reader := NewPackReader(&ParsedPack{Objects: objs}, nil)
	_, err := reader.DiffTree(oldRootOID, newRootOID, MaxDiffTreeLimit)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("DiffTree over visited-node budget: got %v, want ErrLimitExceeded", err)
	}
}
