package rpc_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/tenant"
)

type fakeGitDataHandler struct {
	gotRepoKey tenant.RepoKey
	blobData   []byte
}

func (h *fakeGitDataHandler) GetDiff(ctx context.Context, req *rpc.GetDiffRequest) (*rpc.GetDiffResponse, error) {
	rk, err := tenant.RequireRepoKey(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	h.gotRepoKey = rk
	return &rpc.GetDiffResponse{
		BaseOID: req.BaseOID,
		HeadOID: req.HeadOID,
		Files: []rpc.DiffFileEntry{
			{
				Path:       "README.md",
				ChangeKind: "modify",
				MimeType:   "text/markdown",
				Additions:  1,
			},
		},
	}, nil
}

func (h *fakeGitDataHandler) GetBlob(ctx context.Context, _ *rpc.GetBlobRequest, stream rpc.GitDataBlobStream) error {
	rk, err := tenant.RequireRepoKey(ctx)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	h.gotRepoKey = rk
	total := uint64(len(h.blobData))
	if len(h.blobData) > 0 {
		if err := stream.Send(&rpc.BlobChunk{Data: h.blobData, TotalBytes: total}); err != nil {
			return err
		}
	}
	return stream.Send(&rpc.BlobChunk{TotalBytes: total, EOF: true})
}

func (h *fakeGitDataHandler) ListTree(ctx context.Context, req *rpc.ListTreeRequest) (*rpc.ListTreeResponse, error) {
	rk, err := tenant.RequireRepoKey(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	h.gotRepoKey = rk
	return &rpc.ListTreeResponse{
		TreeOID: req.TreeOID,
		Entries: []rpc.TreeEntry{
			{Name: "main.go", Path: "main.go", Mode: "100644", Type: "blob", OID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 42},
		},
	}, nil
}

func startGitDataTestServer(t *testing.T, handler rpc.GitDataHandler, opts ...grpc.ServerOption) (rpc.GitDataClient, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer(opts...)
	rpc.RegisterGitDataServer(srv, handler)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return rpc.NewGRPCGitDataClient(cc), cc
}

func TestGitDataRoundTrip(t *testing.T) {
	handler := &fakeGitDataHandler{blobData: []byte("hello gitdata")}
	client, _ := startGitDataTestServer(t, handler)
	ctx := context.Background()

	diffResp, err := client.GetDiff(ctx, &rpc.GetDiffRequest{
		TenantID: "acme",
		RepoID:   "core",
		BaseOID:  "1111111111111111111111111111111111111111",
		HeadOID:  "2222222222222222222222222222222222222222",
	})
	if err != nil {
		t.Fatalf("GetDiff: %v", err)
	}
	if len(diffResp.Files) != 1 || diffResp.Files[0].Path != "README.md" {
		t.Fatalf("unexpected diff response: %+v", diffResp)
	}
	if handler.gotRepoKey.String() != "acme/core" {
		t.Fatalf("handler RepoKey = %s, want acme/core", handler.gotRepoKey)
	}

	blobStream, err := client.GetBlob(ctx, &rpc.GetBlobRequest{
		TenantID: "acme",
		RepoID:   "core",
		BlobOID:  "3333333333333333333333333333333333333333",
	})
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	var buf bytes.Buffer
	for {
		chunk, recvErr := blobStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("GetBlob Recv: %v", recvErr)
		}
		buf.Write(chunk.Data)
		if chunk.EOF {
			break
		}
	}
	if buf.String() != "hello gitdata" {
		t.Fatalf("blob = %q, want %q", buf.String(), "hello gitdata")
	}

	treeResp, err := client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID: "acme",
		RepoID:   "core",
		TreeOID:  "4444444444444444444444444444444444444444",
	})
	if err != nil {
		t.Fatalf("ListTree: %v", err)
	}
	if len(treeResp.Entries) != 1 || treeResp.Entries[0].Name != "main.go" {
		t.Fatalf("unexpected tree response: %+v", treeResp)
	}
}

func TestGitDataTenantIsolation(t *testing.T) {
	handler := &fakeGitDataHandler{}
	client, cc := startGitDataTestServer(t, handler)

	t.Run("mismatched tenant metadata rejected with PermissionDenied", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(),
			rpc.HeaderTenantID, "other-tenant",
			rpc.HeaderRepoID, "core",
		)
		_, err := client.GetDiff(ctx, &rpc.GetDiffRequest{
			TenantID: "acme",
			RepoID:   "core",
			HeadOID:  "2222222222222222222222222222222222222222",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("GetDiff code = %v (%v), want PermissionDenied", status.Code(err), err)
		}
	})

	t.Run("mismatched repo metadata rejected on GetBlob with PermissionDenied", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(),
			rpc.HeaderTenantID, "acme",
			rpc.HeaderRepoID, "other-repo",
		)
		stream, err := client.GetBlob(ctx, &rpc.GetBlobRequest{
			TenantID: "acme",
			RepoID:   "core",
			BlobOID:  "3333333333333333333333333333333333333333",
		})
		if err != nil {
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("GetBlob open code = %v, want PermissionDenied", status.Code(err))
			}
			return
		}
		_, err = stream.Recv()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("GetBlob Recv code = %v (%v), want PermissionDenied", status.Code(err), err)
		}
	})

	t.Run("missing metadata rejected with InvalidArgument", func(t *testing.T) {
		out := new(rpc.ListTreeResponse)
		err := cc.Invoke(context.Background(), rpc.GitDataListTreeMethod, &rpc.ListTreeRequest{
			TenantID: "acme",
			RepoID:   "core",
			TreeOID:  "4444444444444444444444444444444444444444",
		}, out, grpc.ForceCodec(rpc.GitDataCodec()))
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ListTree without metadata code = %v (%v), want InvalidArgument", status.Code(err), err)
		}
	})

	t.Run("invalid tenant id rejected with InvalidArgument", func(t *testing.T) {
		_, err := client.ListTree(context.Background(), &rpc.ListTreeRequest{
			TenantID: "../evil",
			RepoID:   "core",
			TreeOID:  "4444444444444444444444444444444444444444",
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ListTree invalid tenant code = %v (%v), want InvalidArgument", status.Code(err), err)
		}
	})
}

func TestUnaryAuthInterceptor_EnforcesInternalToken(t *testing.T) {
	srvOpts, err := rpc.ServerOptions(rpc.TransportConfig{
		Token:         "secret-gitdata-token",
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}
	handler := &fakeGitDataHandler{}
	client, _ := startGitDataTestServer(t, handler, srvOpts...)

	// Without token -> Unauthenticated.
	_, err = client.ListTree(context.Background(), &rpc.ListTreeRequest{
		TenantID: "acme",
		RepoID:   "core",
		TreeOID:  "4444444444444444444444444444444444444444",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("without token code = %v, want Unauthenticated", status.Code(err))
	}

	// With valid token -> OK.
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		rpc.HeaderInternalToken, "secret-gitdata-token",
		rpc.HeaderTenantID, "acme",
		rpc.HeaderRepoID, "core",
	)
	resp, err := client.ListTree(ctx, &rpc.ListTreeRequest{
		TenantID: "acme",
		RepoID:   "core",
		TreeOID:  "4444444444444444444444444444444444444444",
	})
	if err != nil {
		t.Fatalf("with valid token: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(resp.Entries))
	}
}
