package edge

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/rpc"
)

type proxyEchoHandler struct{}

func (proxyEchoHandler) HandleGitWire(_ context.Context, _ rpc.RequestMeta, stream rpc.GitWireStream) error {
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
	if err := stream.Send(&rpc.Frame{Data: append([]byte("ok:"), buf.Bytes()...)}); err != nil {
		return err
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
}

func TestGRPCNodeDialerWithConfig_AuthToken(t *testing.T) {
	const secret = "edge-to-node-secret"

	srvOpts, err := rpc.ServerOptions(rpc.TransportConfig{
		Token:         secret,
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer(srvOpts...)
	rpc.RegisterGitWireServer(srv, proxyEchoHandler{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	meta := rpc.RequestMeta{
		TenantID:  "acme",
		RepoID:    "core",
		Operation: rpc.OpUploadPack,
	}

	t.Run("matching token succeeds", func(t *testing.T) {
		dialer, err := NewGRPCNodeDialerWithConfig(rpc.TransportConfig{
			Token:         secret,
			AllowInsecure: true,
		})
		if err != nil {
			t.Fatalf("NewGRPCNodeDialerWithConfig: %v", err)
		}
		t.Cleanup(func() { _ = dialer.Close() })

		proxy, err := NewWireProxy(dialer)
		if err != nil {
			t.Fatalf("NewWireProxy: %v", err)
		}

		var stdout, stderr bytes.Buffer
		exitCode, err := proxy.ProxyStream(context.Background(), lis.Addr().String(), meta, strings.NewReader("hello"), &stdout, &stderr)
		if err != nil {
			t.Fatalf("ProxyStream: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("exitCode = %d, want 0", exitCode)
		}
		if got := stdout.String(); got != "ok:hello" {
			t.Fatalf("stdout = %q, want %q", got, "ok:hello")
		}
	})

	t.Run("wrong token rejected with Unauthenticated", func(t *testing.T) {
		dialer, err := NewGRPCNodeDialerWithConfig(rpc.TransportConfig{
			Token:         "bad-secret",
			AllowInsecure: true,
		})
		if err != nil {
			t.Fatalf("NewGRPCNodeDialerWithConfig: %v", err)
		}
		t.Cleanup(func() { _ = dialer.Close() })

		proxy, err := NewWireProxy(dialer)
		if err != nil {
			t.Fatalf("NewWireProxy: %v", err)
		}

		var stdout, stderr bytes.Buffer
		_, err = proxy.ProxyStream(context.Background(), lis.Addr().String(), meta, strings.NewReader("hello"), &stdout, &stderr)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("status.Code(err) = %v (err=%v), want Unauthenticated", status.Code(err), err)
		}
	})

	t.Run("empty config fails closed", func(t *testing.T) {
		if _, err := NewGRPCNodeDialerWithConfig(rpc.TransportConfig{}); err == nil {
			t.Fatal("expected error for empty TransportConfig, got nil")
		}
	})
}

type stubDialer struct {
	client rpc.GitWireClient
}

func (d stubDialer) Client(string) (rpc.GitWireClient, error) { return d.client, nil }

type stubGitWireClient struct {
	open func(ctx context.Context, meta rpc.RequestMeta) (rpc.GitWireStream, error)
}

func (c stubGitWireClient) OpenStream(ctx context.Context, meta rpc.RequestMeta) (rpc.GitWireStream, error) {
	return c.open(ctx, meta)
}

type sendEOFStream struct {
	ctx      context.Context
	sendDone chan struct{}
	recvErr  error
}

func (s *sendEOFStream) Context() context.Context { return s.ctx }

func (s *sendEOFStream) Send(*rpc.Frame) error {
	select {
	case <-s.sendDone:
	default:
		close(s.sendDone)
	}
	return io.EOF
}

func (s *sendEOFStream) Recv() (*rpc.Frame, error) {
	<-s.sendDone
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	return nil, s.recvErr
}

func TestWireProxy_SendEOFPreservesRecvError(t *testing.T) {
	wantErr := status.Error(codes.Unavailable, "upstream unavailable")
	meta := rpc.RequestMeta{
		TenantID:  "acme",
		RepoID:    "core",
		Operation: rpc.OpUploadPack,
	}

	for _, tc := range []struct {
		name  string
		stdin func() io.Reader
	}{
		{name: "with stdin", stdin: func() io.Reader { return strings.NewReader("payload") }},
		{name: "nil stdin", stdin: func() io.Reader { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := stubGitWireClient{
				open: func(ctx context.Context, _ rpc.RequestMeta) (rpc.GitWireStream, error) {
					return &sendEOFStream{
						ctx:      ctx,
						sendDone: make(chan struct{}),
						recvErr:  wantErr,
					}, nil
				},
			}
			proxy, err := NewWireProxy(stubDialer{client: client})
			if err != nil {
				t.Fatalf("NewWireProxy: %v", err)
			}

			var stdout, stderr bytes.Buffer
			_, err = proxy.ProxyStream(context.Background(), "node-1:9090", meta, tc.stdin(), &stdout, &stderr)
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "upstream unavailable") {
				t.Fatalf("ProxyStream err = %v, want Unavailable upstream unavailable", err)
			}
		})
	}
}
