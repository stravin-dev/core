package edge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/telemetry"
)

const wireChunkSize = 256 * 1024

// NodeDialer returns an rpc.GitWireClient for a target stravin-node peer address.
type NodeDialer interface {
	Client(peer string) (rpc.GitWireClient, error)
}

// GRPCNodeDialer manages pooled gRPC connections to stravin-node peers.
type GRPCNodeDialer struct {
	mu       sync.RWMutex
	conns    map[string]*grpc.ClientConn
	dialOpts []grpc.DialOption
}

// NewGRPCNodeDialer creates a GRPCNodeDialer. If no transport credentials option is provided,
// insecure credentials are used for local/internal cluster traffic.
func NewGRPCNodeDialer(opts ...grpc.DialOption) *GRPCNodeDialer {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &GRPCNodeDialer{
		conns:    make(map[string]*grpc.ClientConn),
		dialOpts: opts,
	}
}

// NewGRPCNodeDialerWithConfig creates a GRPCNodeDialer configured with internal gRPC
// authentication and TLS/mTLS options from cfg. It fails closed when cfg is incomplete.
func NewGRPCNodeDialerWithConfig(cfg rpc.TransportConfig, extraOpts ...grpc.DialOption) (*GRPCNodeDialer, error) {
	opts, err := rpc.ClientDialOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("edge: grpc client transport config: %w", err)
	}
	opts = append(opts, extraOpts...)
	return NewGRPCNodeDialer(opts...), nil
}

// Client returns a cached rpc.GitWireClient for peer, dialing lazily if needed.
func (d *GRPCNodeDialer) Client(peer string) (rpc.GitWireClient, error) {
	if peer == "" {
		return nil, ErrNoPeers
	}
	d.mu.RLock()
	cc, ok := d.conns[peer]
	d.mu.RUnlock()
	if ok {
		return rpc.NewGRPCGitWireClient(cc), nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if cc, ok = d.conns[peer]; ok {
		return rpc.NewGRPCGitWireClient(cc), nil
	}
	cc, err := grpc.NewClient(peer, d.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("edge: dial peer %q: %w", peer, err)
	}
	d.conns[peer] = cc
	return rpc.NewGRPCGitWireClient(cc), nil
}

// Close closes all pooled gRPC connections.
func (d *GRPCNodeDialer) Close() error {
	d.mu.Lock()
	conns := d.conns
	d.conns = make(map[string]*grpc.ClientConn)
	d.mu.Unlock()

	var errs []error
	for _, cc := range conns {
		if err := cc.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// WireProxy is the GoF Proxy that tunnels bidirectional Git wire streams to stravin-node
// over the frozen internal/rpc contract while propagating W3C traceparent and TenantID.
type WireProxy struct {
	dialer NodeDialer
}

// NewWireProxy constructs a WireProxy backed by dialer.
func NewWireProxy(dialer NodeDialer) (*WireProxy, error) {
	if dialer == nil {
		return nil, errors.New("edge: node dialer is required")
	}
	return &WireProxy{dialer: dialer}, nil
}

// ProxyStream opens a bidirectional GitWire stream to peer, streams stdin -> node and
// node -> stdout/stderr concurrently without unbounded buffering, and returns the remote exit code.
func (p *WireProxy) ProxyStream(
	ctx context.Context,
	peer string,
	meta rpc.RequestMeta,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
) (int32, error) {
	client, err := p.dialer.Client(peer)
	if err != nil {
		return 1, err
	}

	if meta.TraceParent == "" {
		meta.TraceParent = telemetry.TraceparentFromContext(ctx)
	}
	if err := meta.Validate(); err != nil {
		return 1, err
	}

	streamCtx, cancel := context.WithCancel(telemetry.InjectGRPC(ctx))
	defer cancel()

	stream, err := client.OpenStream(streamCtx, meta)
	if err != nil {
		return 1, fmt.Errorf("edge: open stream to %s: %w", peer, err)
	}

	sendErrCh := make(chan error, 1)
	go func() {
		err := pumpStdin(streamCtx, stdin, stream)
		sendErrCh <- err
		if err != nil {
			cancel()
		}
	}()

	var exitCode int32
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			cancel()
			if sendErr := nonBlockingErr(sendErrCh); sendErr != nil && !errors.Is(sendErr, context.Canceled) {
				return 1, sendErr
			}
			return 1, recvErr
		}
		if frame.ErrMessage != "" && stderr != nil {
			_, _ = io.WriteString(stderr, frame.ErrMessage)
		}
		if len(frame.Data) > 0 && stdout != nil {
			if _, wErr := stdout.Write(frame.Data); wErr != nil {
				cancel()
				return 1, wErr
			}
		}
		if frame.EOF {
			exitCode = frame.ExitCode
			break
		}
	}

	cancel()
	if sendErr := nonBlockingErr(sendErrCh); sendErr != nil && !errors.Is(sendErr, context.Canceled) {
		return 1, sendErr
	}
	return exitCode, nil
}

func pumpStdin(ctx context.Context, stdin io.Reader, stream rpc.GitWireStream) error {
	if stdin == nil {
		return stream.Send(&rpc.Frame{EOF: true})
	}
	buf := make([]byte, wireChunkSize)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, readErr := stdin.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if sendErr := stream.Send(&rpc.Frame{Data: chunk}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			return stream.Send(&rpc.Frame{EOF: true})
		}
		if readErr != nil {
			return readErr
		}
	}
}

func nonBlockingErr(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	default:
		return nil
	}
}
