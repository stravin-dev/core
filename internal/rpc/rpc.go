// Package rpc defines the frozen internal gRPC wire contract between
// stravin-edge and stravin-node for streaming Git wire I/O with mandatory
// tenant, repository, operation, and W3C traceparent propagation.
package rpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/tenant"
)

const (
	// ServiceName is the canonical gRPC service identifier for internal Git wire streaming.
	ServiceName = "stravin.core.v1.GitWire"
	// StreamMethodName is the full gRPC method path for bidirectional Git wire streaming.
	StreamMethodName = "/" + ServiceName + "/Stream"
	// CodecName is the registered gRPC codec name for Frame wire messages.
	CodecName = "stravin-gitwire"

	HeaderTenantID    = "x-stravin-tenant-id"
	HeaderRepoID      = "x-stravin-repo-id"
	HeaderOperation   = "x-stravin-operation"
	HeaderTraceParent = "traceparent"
)

// Operation identifies the Git wire service being streamed.
type Operation string

const (
	OpUploadPack  Operation = "git-upload-pack"
	OpReceivePack Operation = "git-receive-pack"
)

// Validate ensures op is a supported Git wire service.
func (op Operation) Validate() error {
	switch op {
	case OpUploadPack, OpReceivePack:
		return nil
	default:
		return fmt.Errorf("rpc: unsupported operation %q", op)
	}
}

// RequestMeta carries the mandatory routing, isolation, and tracing headers for a stream.
type RequestMeta struct {
	TenantID    tenant.TenantID
	RepoID      tenant.RepoID
	Operation   Operation
	TraceParent string
	GitProtocol string
}

// Validate checks that TenantID, RepoID, Operation, and optional TraceParent/GitProtocol are well-formed.
func (m RequestMeta) Validate() error {
	if err := m.TenantID.Validate(); err != nil {
		return err
	}
	if err := m.RepoID.Validate(); err != nil {
		return err
	}
	if err := m.Operation.Validate(); err != nil {
		return err
	}
	if m.TraceParent != "" && strings.ContainsAny(m.TraceParent, "\r\n\x00") {
		return errors.New("rpc: invalid traceparent header")
	}
	if m.GitProtocol != "" {
		clean, ok := SanitizeGitProtocol(m.GitProtocol)
		if !ok || clean != m.GitProtocol {
			return errors.New("rpc: invalid git-protocol header")
		}
	}
	return nil
}

// RepoKey returns the validated tenant.RepoKey for this request.
func (m RequestMeta) RepoKey() tenant.RepoKey {
	return tenant.RepoKey{TenantID: m.TenantID, RepoID: m.RepoID}
}

// InjectMetadata validates meta and attaches it to outgoing gRPC metadata on ctx.
func InjectMetadata(ctx context.Context, meta RequestMeta) (context.Context, error) {
	if err := meta.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pairs := []string{
		HeaderTenantID, string(meta.TenantID),
		HeaderRepoID, string(meta.RepoID),
		HeaderOperation, string(meta.Operation),
	}
	if meta.TraceParent != "" {
		pairs = append(pairs, HeaderTraceParent, meta.TraceParent)
	}
	if meta.GitProtocol != "" {
		pairs = append(pairs, HeaderGitProtocol, meta.GitProtocol)
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...), nil
}

// ExtractMetadata reads and validates RequestMeta from incoming gRPC metadata on ctx.
func ExtractMetadata(ctx context.Context) (RequestMeta, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return RequestMeta{}, status.Error(codes.InvalidArgument, "rpc: missing incoming metadata")
	}
	meta := RequestMeta{
		TenantID:    tenant.TenantID(firstValue(md, HeaderTenantID)),
		RepoID:      tenant.RepoID(firstValue(md, HeaderRepoID)),
		Operation:   Operation(firstValue(md, HeaderOperation)),
		TraceParent: firstValue(md, HeaderTraceParent),
		GitProtocol: firstValue(md, HeaderGitProtocol),
	}
	if err := meta.Validate(); err != nil {
		return RequestMeta{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return meta, nil
}

func firstValue(md metadata.MD, key string) string {
	vals := md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// Frame is the binary wire payload exchanged over a bidirectional GitWire stream.
type Frame struct {
	Data       []byte
	EOF        bool
	ExitCode   int32
	ErrMessage string
}

// GitWireStream is the narrow bidirectional stream interface shared by clients and handlers.
type GitWireStream interface {
	Send(*Frame) error
	Recv() (*Frame, error)
	Context() context.Context
}

// GitWireHandler handles incoming Git wire streams on stravin-node.
type GitWireHandler interface {
	HandleGitWire(ctx context.Context, meta RequestMeta, stream GitWireStream) error
}

// GitWireClient opens bidirectional Git wire streams from stravin-edge to stravin-node.
type GitWireClient interface {
	OpenStream(ctx context.Context, meta RequestMeta) (GitWireStream, error)
}

func init() {
	encoding.RegisterCodec(gitWireCodec{})
}

// WireCodec returns the registered gRPC codec for Frame payloads.
func WireCodec() encoding.Codec {
	return gitWireCodec{}
}

type gitWireCodec struct{}

func (gitWireCodec) Name() string { return CodecName }

func (gitWireCodec) Marshal(v any) ([]byte, error) {
	frame, ok := v.(*Frame)
	if !ok || frame == nil {
		return nil, fmt.Errorf("rpc: expected *Frame, got %T", v)
	}
	errBytes := []byte(frame.ErrMessage)
	buf := make([]byte, 1+4+4+len(errBytes)+4+len(frame.Data))
	if frame.EOF {
		buf[0] = 1
	}
	binary.BigEndian.PutUint32(buf[1:5], uint32(frame.ExitCode))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(errBytes)))
	offset := 9
	copy(buf[offset:offset+len(errBytes)], errBytes)
	offset += len(errBytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(frame.Data)))
	offset += 4
	copy(buf[offset:], frame.Data)
	return buf, nil
}

func (gitWireCodec) Unmarshal(data []byte, v any) error {
	frame, ok := v.(*Frame)
	if !ok || frame == nil {
		return fmt.Errorf("rpc: expected *Frame, got %T", v)
	}
	const minHeader = 1 + 4 + 4 + 4
	if len(data) < minHeader {
		return errors.New("rpc: truncated frame header")
	}
	frame.EOF = data[0]&1 == 1
	frame.ExitCode = int32(binary.BigEndian.Uint32(data[1:5]))
	errLen := int(binary.BigEndian.Uint32(data[5:9]))
	offset := 9
	if offset+errLen+4 > len(data) {
		return errors.New("rpc: truncated frame error message")
	}
	frame.ErrMessage = string(data[offset : offset+errLen])
	offset += errLen
	dataLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if offset+dataLen != len(data) {
		return errors.New("rpc: truncated frame payload")
	}
	if dataLen > 0 {
		frame.Data = append([]byte(nil), data[offset:offset+dataLen]...)
	} else {
		frame.Data = nil
	}
	return nil
}

type grpcStreamAdapter struct {
	stream grpc.ServerStream
}

func (a grpcStreamAdapter) Send(f *Frame) error      { return a.stream.SendMsg(f) }
func (a grpcStreamAdapter) Recv() (*Frame, error)    { f := new(Frame); return f, a.stream.RecvMsg(f) }
func (a grpcStreamAdapter) Context() context.Context { return a.stream.Context() }

type grpcClientStreamAdapter struct {
	stream grpc.ClientStream
}

func (a grpcClientStreamAdapter) Send(f *Frame) error { return a.stream.SendMsg(f) }
func (a grpcClientStreamAdapter) Recv() (*Frame, error) {
	f := new(Frame)
	return f, a.stream.RecvMsg(f)
}
func (a grpcClientStreamAdapter) Context() context.Context { return a.stream.Context() }

type grpcGitWireClient struct {
	cc grpc.ClientConnInterface
}

// NewGRPCGitWireClient returns a GitWireClient backed by a gRPC ClientConnInterface.
func NewGRPCGitWireClient(cc grpc.ClientConnInterface) GitWireClient {
	return &grpcGitWireClient{cc: cc}
}

func (c *grpcGitWireClient) OpenStream(ctx context.Context, meta RequestMeta) (GitWireStream, error) {
	outCtx, err := InjectMetadata(ctx, meta)
	if err != nil {
		return nil, err
	}
	desc := &grpc.StreamDesc{
		StreamName:    "Stream",
		ServerStreams: true,
		ClientStreams: true,
	}
	cs, err := c.cc.NewStream(outCtx, desc, StreamMethodName, grpc.ForceCodec(WireCodec()))
	if err != nil {
		return nil, err
	}
	return grpcClientStreamAdapter{stream: cs}, nil
}

// RegisterGitWireServer registers handler on s under the frozen ServiceName contract.
func RegisterGitWireServer(s grpc.ServiceRegistrar, h GitWireHandler) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*GitWireHandler)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Stream",
				ServerStreams: true,
				ClientStreams: true,
				Handler: func(srv any, stream grpc.ServerStream) error {
					meta, err := ExtractMetadata(stream.Context())
					if err != nil {
						return err
					}
					ctx, err := tenant.WithRepoKey(stream.Context(), meta.RepoKey())
					if err != nil {
						return status.Error(codes.InvalidArgument, err.Error())
					}
					return srv.(GitWireHandler).HandleGitWire(ctx, meta, grpcStreamAdapter{stream: stream})
				},
			},
		},
	}, h)
}
