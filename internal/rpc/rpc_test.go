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

func TestMetadataRoundTrip(t *testing.T) {
	meta := rpc.RequestMeta{
		TenantID:    "acme",
		RepoID:      "core",
		Operation:   rpc.OpUploadPack,
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	outCtx, err := rpc.InjectMetadata(context.Background(), meta)
	if err != nil {
		t.Fatalf("InjectMetadata: %v", err)
	}

	md, ok := metadata.FromOutgoingContext(outCtx)
	if !ok {
		t.Fatal("expected outgoing gRPC metadata")
	}
	inCtx := metadata.NewIncomingContext(context.Background(), md)

	got, err := rpc.ExtractMetadata(inCtx)
	if err != nil {
		t.Fatalf("ExtractMetadata: %v", err)
	}
	if got != meta {
		t.Fatalf("got %+v, want %+v", got, meta)
	}
}

func TestMetadataValidationRejectsInvalid(t *testing.T) {
	bad := []rpc.RequestMeta{
		{TenantID: "", RepoID: "core", Operation: rpc.OpUploadPack},
		{TenantID: "acme", RepoID: "../bad", Operation: rpc.OpUploadPack},
		{TenantID: "acme", RepoID: "core", Operation: "git-shell"},
		{TenantID: "acme", RepoID: "core", Operation: rpc.OpReceivePack, TraceParent: "bad\nheader"},
	}
	for _, meta := range bad {
		if _, err := rpc.InjectMetadata(context.Background(), meta); err == nil {
			t.Errorf("InjectMetadata(%+v) expected error, got nil", meta)
		}
	}

	if _, err := rpc.ExtractMetadata(context.Background()); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ExtractMetadata on empty ctx code = %v, want InvalidArgument", status.Code(err))
	}
}

type echoHandler struct {
	gotMeta rpc.RequestMeta
}

func (h *echoHandler) HandleGitWire(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error {
	h.gotMeta = meta
	if gotTenant, err := tenant.RequireTenant(ctx); err != nil || gotTenant != meta.TenantID {
		return status.Errorf(codes.Internal, "tenant not in handler context: %q %v", gotTenant, err)
	}

	var buf bytes.Buffer
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		buf.Write(frame.Data)
		if frame.EOF {
			break
		}
	}

	if err := stream.Send(&rpc.Frame{Data: append([]byte("ack:"), buf.Bytes()...)}); err != nil {
		return err
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
}

func TestGitWireBidirectionalStream(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	handler := &echoHandler{}
	rpc.RegisterGitWireServer(srv, handler)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.WireCodec())),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	client := rpc.NewGRPCGitWireClient(cc)
	wantMeta := rpc.RequestMeta{
		TenantID:    "acme",
		RepoID:      "core",
		Operation:   rpc.OpReceivePack,
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	stream, err := client.OpenStream(context.Background(), wantMeta)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	if err := stream.Send(&rpc.Frame{Data: []byte("001epack-part-1")}); err != nil {
		t.Fatalf("Send 1: %v", err)
	}
	if err := stream.Send(&rpc.Frame{Data: []byte("-part-2"), EOF: true}); err != nil {
		t.Fatalf("Send 2: %v", err)
	}

	var received bytes.Buffer
	var sawEOF bool
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		received.Write(frame.Data)
		if frame.EOF {
			sawEOF = true
			if frame.ExitCode != 0 {
				t.Fatalf("unexpected ExitCode %d", frame.ExitCode)
			}
			break
		}
	}

	if !sawEOF {
		t.Fatal("expected EOF frame from server")
	}
	if got, want := received.String(), "ack:001epack-part-1-part-2"; got != want {
		t.Fatalf("received = %q, want %q", got, want)
	}
	if handler.gotMeta != wantMeta {
		t.Fatalf("server meta = %+v, want %+v", handler.gotMeta, wantMeta)
	}
}
