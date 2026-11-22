package pack

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func buildTreePayload(entries []TreeEntry) []byte {
	var buf bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&buf, "%o %s\x00", e.Mode, e.Name)
		buf.Write(e.OID[:])
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
