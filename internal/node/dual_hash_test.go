package node

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
)

func buildSyntheticSHA256CommitPack(t *testing.T, blobContent, commitMsg string, parentOID *pack.OID) (commitOID, blobOID pack.OID, packBytes []byte) {
	t.Helper()
	blobData := []byte(blobContent)
	blobOID = pack.ComputeOIDWith(pack.SHA256, pack.ObjBlob, blobData)

	var treeBuf bytes.Buffer
	fmt.Fprintf(&treeBuf, "100644 hello.txt\x00")
	treeBuf.Write(blobOID.Bytes())
	treeData := treeBuf.Bytes()
	treeOID := pack.ComputeOIDWith(pack.SHA256, pack.ObjTree, treeData)

	var commitBuf bytes.Buffer
	fmt.Fprintf(&commitBuf, "tree %s\n", treeOID.String())
	if parentOID != nil {
		fmt.Fprintf(&commitBuf, "parent %s\n", parentOID.String())
	}
	commitBuf.WriteString("author Stravin Test <test@stravin.dev> 1700000000 +0000\n")
	commitBuf.WriteString("committer Stravin Test <test@stravin.dev> 1700000000 +0000\n\n")
	commitBuf.WriteString(commitMsg)
	commitData := commitBuf.Bytes()
	commitOID = pack.ComputeOIDWith(pack.SHA256, pack.ObjCommit, commitData)

	entries := []struct {
		typ  pack.ObjectType
		data []byte
	}{
		{pack.ObjBlob, blobData},
		{pack.ObjTree, treeData},
		{pack.ObjCommit, commitData},
	}

	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(entries)))
	for _, e := range entries {
		size := uint64(len(e.data))
		b := ((uint8(e.typ) & 0x07) << 4) | byte(size&0x0f)
		size >>= 4
		for size > 0 {
			buf.WriteByte(b | 0x80)
			b = byte(size & 0x7f)
			size >>= 7
		}
		buf.WriteByte(b)
		zw := zlib.NewWriter(&buf)
		if _, err := zw.Write(e.data); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	sum := pack.SHA256.Sum(buf.Bytes())
	buf.Write(sum.Bytes())
	return commitOID, blobOID, buf.Bytes()
}

func TestAdvertiseRefsAndParseReceiveCommandsSHA256(t *testing.T) {
	out := AdvertiseRefs(nil, "refs/heads/main", AdvertiseOptions{
		IncludeServiceHeader: true,
		ServiceName:          "git-receive-pack",
		ObjectFormat:         ddb.ObjectFormatSHA256,
	})
	s := string(out)
	if !strings.Contains(s, ddb.ZeroOIDSHA256+" capabilities^{}\x00") {
		t.Fatalf("expected 64-zero capabilities line, got %q", s)
	}
	if !strings.Contains(s, "object-format=sha256") {
		t.Fatalf("expected object-format=sha256 capability, got %q", s)
	}

	newOID := strings.Repeat("a", 64)
	var wire bytes.Buffer
	wire.Write(EncodePktLine([]byte(ddb.ZeroOIDSHA256 + " " + newOID + " refs/heads/main\x00report-status atomic\n")))
	wire.Write(FlushPkt)

	cmds, caps, err := ParseReceiveCommands(&wire)
	if err != nil {
		t.Fatalf("ParseReceiveCommands(sha256): %v", err)
	}
	if len(cmds) != 1 || cmds[0].OldOID != ddb.ZeroOIDSHA256 || cmds[0].NewOID != newOID || !caps["atomic"] {
		t.Fatalf("unexpected commands/caps: cmds=%+v caps=%+v", cmds, caps)
	}
}

func TestReceivePackAndHooksAdapterSHA256(t *testing.T) {
	env := newTestReceivePackEnv(t, "sha256-app")
	repo := env.rk
	env.ddbStore.PutMeta(ddb.RepoMeta{
		Repo:           repo,
		HomeRegion:     lease.Region("us-east-1"),
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
		ObjectFormat:   ddb.ObjectFormatSHA256,
	})

	commitOID, blobOID, packBytes := buildSyntheticSHA256CommitPack(t, "hello sha256 receive-pack\n", "initial sha256\n", nil)

	// 1. Verify DualHashObjectReader adapter works directly on SHA-256 PackReader.
	parsed, err := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{HashAlgorithm: pack.SHA256})
	if err != nil {
		t.Fatalf("pack.Parse(sha256): %v", err)
	}
	adapter, ok := NewPackObjectReaderAdapter(pack.NewPackReader(parsed, nil)).(wasm.DualHashObjectReader)
	if !ok {
		t.Fatalf("adapter does not implement wasm.DualHashObjectReader")
	}
	commits, err := adapter.ListNewCommitsByHex(ddb.ZeroOIDSHA256, commitOID.String(), 10)
	if err != nil || len(commits) != 1 || commits[0].OIDHex != commitOID.String() {
		t.Fatalf("ListNewCommitsByHex(sha256) = (%+v, %v)", commits, err)
	}
	chunk, err := adapter.ReadBlobChunkByHex(blobOID.String(), 0, 64)
	if err != nil || string(chunk) != "hello sha256 receive-pack\n" {
		t.Fatalf("ReadBlobChunkByHex(sha256) = (%q, %v)", string(chunk), err)
	}

	// 2. Push SHA-256 pack through ReceivePackHandler end-to-end over gRPC.
	var pushWire bytes.Buffer
	pushWire.Write(EncodePktLine([]byte(ddb.ZeroOIDSHA256 + " " + commitOID.String() + " refs/heads/main\x00report-status side-band-64k atomic\n")))
	pushWire.Write(FlushPkt)
	pushWire.Write(packBytes)

	outBytes, exitCode, err := sendReceivePack(t, context.Background(), env.client, repo, "", pushWire.Bytes())
	if err != nil {
		t.Fatalf("sendReceivePack(sha256): %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode = %d, want 0", exitCode)
	}
	out := string(outBytes)
	if !strings.Contains(out, "unpack ok") || !strings.Contains(out, "ok refs/heads/main") {
		t.Fatalf("expected unpack ok + ok refs/heads/main, got %q", out)
	}

	// Verify bare repo config in NVMe cache declares extensions.objectformat = sha256.
	repoPath, err := env.nvmeCache.RepoPath(repo)
	if err != nil {
		t.Fatal(err)
	}
	cfgBytes, err := os.ReadFile(filepath.Join(repoPath, "config"))
	if err != nil {
		t.Fatalf("read bare repo config: %v", err)
	}
	if !strings.Contains(string(cfgBytes), "objectformat = sha256") {
		t.Fatalf("bare repo config missing objectformat = sha256:\n%s", string(cfgBytes))
	}

	// 3. Pushing 40-hex SHA-1 OIDs to a SHA-256 repo must be rejected with InvalidArgument.
	var badWire bytes.Buffer
	badWire.Write(EncodePktLine([]byte(ddb.ZeroOID + " " + strings.Repeat("a", 40) + " refs/heads/bad\x00report-status\n")))
	badWire.Write(FlushPkt)
	_, _, err = sendReceivePack(t, context.Background(), env.client, repo, "", badWire.Bytes())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SHA-1 push to SHA-256 repo err = %v, want InvalidArgument", err)
	}
}
