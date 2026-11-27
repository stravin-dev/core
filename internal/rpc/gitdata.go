package rpc

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/tenant"
)

const (
	// GitDataServiceName is the canonical gRPC service identifier for read-only Git data queries.
	GitDataServiceName = "stravin.core.v1.GitData"
	// GitDataGetDiffMethod is the full gRPC method path for GetDiff.
	GitDataGetDiffMethod = "/" + GitDataServiceName + "/GetDiff"
	// GitDataGetBlobMethod is the full gRPC method path for GetBlob.
	GitDataGetBlobMethod = "/" + GitDataServiceName + "/GetBlob"
	// GitDataListTreeMethod is the full gRPC method path for ListTree.
	GitDataListTreeMethod = "/" + GitDataServiceName + "/ListTree"
	// GitDataCodecName is the registered gRPC codec name for GitData JSON messages.
	GitDataCodecName = "stravin-gitdata"

	// HeaderActorID carries the optional end-user/agent actor ID for audit trails.
	HeaderActorID = "x-stravin-actor-id"
)

func init() {
	encoding.RegisterCodec(gitDataCodec{})
}

// GitDataCodec returns the registered gRPC codec for GitData RPC payloads.
func GitDataCodec() encoding.Codec {
	return gitDataCodec{}
}

type gitDataCodec struct{}

func (gitDataCodec) Name() string { return GitDataCodecName }

func (gitDataCodec) Marshal(v any) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("rpc: cannot marshal nil GitData payload")
	}
	return json.Marshal(v)
}

func (gitDataCodec) Unmarshal(data []byte, v any) error {
	if v == nil {
		return fmt.Errorf("rpc: cannot unmarshal into nil GitData payload")
	}
	return json.Unmarshal(data, v)
}

// GetDiffRequest requests a diff between base_oid (or head_oid's parent) and head_oid.
type GetDiffRequest struct {
	TenantID      string `json:"tenant_id"`
	RepoID        string `json:"repo_id"`
	BaseOID       string `json:"base_oid,omitempty"`
	HeadOID       string `json:"head_oid"`
	ThreeDot      bool   `json:"three_dot,omitempty"`
	ContextLines  uint32 `json:"context_lines,omitempty"`
	MaxFiles      uint32 `json:"max_files,omitempty"`
	MaxPatchBytes uint64 `json:"max_patch_bytes,omitempty"`
	PageToken     string `json:"page_token,omitempty"`
}

// GetDiffResponse returns structured diff file entries between base_oid and head_oid.
type GetDiffResponse struct {
	BaseOID       string          `json:"base_oid"`
	HeadOID       string          `json:"head_oid"`
	MergeBaseOID  string          `json:"merge_base_oid,omitempty"`
	Files         []DiffFileEntry `json:"files"`
	NextPageToken string          `json:"next_page_token,omitempty"`
}

// DiffFileEntry describes a single changed file in a diff.
type DiffFileEntry struct {
	Path           string     `json:"path"`
	OldPath        string     `json:"old_path,omitempty"`
	ChangeKind     string     `json:"change_kind"`
	MimeType       string     `json:"mime_type"`
	IsBinary       bool       `json:"is_binary"`
	OldBlobOID     string     `json:"old_blob_oid,omitempty"`
	NewBlobOID     string     `json:"new_blob_oid,omitempty"`
	OldSizeBytes   uint64     `json:"old_size_bytes"`
	NewSizeBytes   uint64     `json:"new_size_bytes"`
	Additions      uint32     `json:"additions"`
	Deletions      uint32     `json:"deletions"`
	Hunks          []DiffHunk `json:"hunks"`
	UnifiedPatch   []byte     `json:"unified_patch,omitempty"`
	PatchTruncated bool       `json:"patch_truncated,omitempty"`
}

// DiffHunk represents a unified diff hunk within a text file.
type DiffHunk struct {
	OldStart uint32     `json:"old_start"`
	OldLines uint32     `json:"old_lines"`
	NewStart uint32     `json:"new_start"`
	NewLines uint32     `json:"new_lines"`
	Header   string     `json:"header"`
	Lines    []DiffLine `json:"lines"`
}

// DiffLine represents a single line inside a DiffHunk.
type DiffLine struct {
	Kind    string `json:"kind"` // "context" | "add" | "delete"
	Content string `json:"content"`
	OldLine uint32 `json:"old_line,omitempty"`
	NewLine uint32 `json:"new_line,omitempty"`
}

// GetBlobRequest requests streaming raw blob bytes by OID.
type GetBlobRequest struct {
	TenantID string `json:"tenant_id"`
	RepoID   string `json:"repo_id"`
	BlobOID  string `json:"blob_oid"`
	MaxBytes uint64 `json:"max_bytes,omitempty"`
}

// BlobChunk carries a bounded chunk of raw blob bytes on a GetBlob server stream.
type BlobChunk struct {
	Data       []byte `json:"data,omitempty"`
	TotalBytes uint64 `json:"total_bytes,omitempty"`
	EOF        bool   `json:"eof,omitempty"`
}

// ListTreeRequest requests directory entries for a commit or tree OID and optional path.
type ListTreeRequest struct {
	TenantID  string `json:"tenant_id"`
	RepoID    string `json:"repo_id"`
	CommitOID string `json:"commit_oid,omitempty"`
	TreeOID   string `json:"tree_oid,omitempty"`
	Path      string `json:"path,omitempty"`
	Recursive bool   `json:"recursive,omitempty"`
}

// ListTreeResponse returns the resolved tree OID and its entries.
type ListTreeResponse struct {
	TreeOID string      `json:"tree_oid"`
	Entries []TreeEntry `json:"entries"`
}

// TreeEntry describes a single Git tree entry.
type TreeEntry struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Mode      string `json:"mode"`
	Type      string `json:"type"` // "blob" | "tree" | "commit"
	OID       string `json:"oid"`
	SizeBytes uint64 `json:"size_bytes"`
}

// GitDataBlobStream is the server-side stream for GetBlob responses.
type GitDataBlobStream interface {
	Send(*BlobChunk) error
	Context() context.Context
}

// GitDataBlobClientStream is the client-side stream for GetBlob responses.
type GitDataBlobClientStream interface {
	Recv() (*BlobChunk, error)
	Context() context.Context
}

// GitDataHandler handles incoming read-only GitData RPCs on stravin-node.
type GitDataHandler interface {
	GetDiff(ctx context.Context, req *GetDiffRequest) (*GetDiffResponse, error)
	GetBlob(ctx context.Context, req *GetBlobRequest, stream GitDataBlobStream) error
	ListTree(ctx context.Context, req *ListTreeRequest) (*ListTreeResponse, error)
}

// GitDataClient invokes read-only GitData RPCs on stravin-node.
type GitDataClient interface {
	GetDiff(ctx context.Context, req *GetDiffRequest, opts ...grpc.CallOption) (*GetDiffResponse, error)
	GetBlob(ctx context.Context, req *GetBlobRequest, opts ...grpc.CallOption) (GitDataBlobClientStream, error)
	ListTree(ctx context.Context, req *ListTreeRequest, opts ...grpc.CallOption) (*ListTreeResponse, error)
}

// InjectGitDataMetadata validates tenantID and repoID and attaches routing headers
// to outgoing gRPC metadata on ctx if not already present.
func InjectGitDataMetadata(ctx context.Context, tenantID, repoID string) (context.Context, error) {
	rk, err := tenant.NewRepoKey(tenantID, repoID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return metadata.AppendToOutgoingContext(ctx,
		HeaderTenantID, string(rk.TenantID),
		HeaderRepoID, string(rk.RepoID),
	), nil
}

// AuthorizeGitDataContext enforces strict tenant_id and repo_id isolation at the
// gRPC boundary by validating the request identity, verifying mandatory incoming
// metadata headers in constant time, and attaching the validated RepoKey to ctx.
func AuthorizeGitDataContext(ctx context.Context, reqTenantID, reqRepoID string) (context.Context, tenant.RepoKey, error) {
	rk, err := tenant.NewRepoKey(reqTenantID, reqRepoID)
	if err != nil {
		return nil, tenant.RepoKey{}, status.Error(codes.InvalidArgument, err.Error())
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, tenant.RepoKey{}, status.Error(codes.InvalidArgument, "rpc: missing incoming tenant/repo metadata")
	}
	mdTenant := firstValue(md, HeaderTenantID)
	mdRepo := firstValue(md, HeaderRepoID)
	if mdTenant == "" || mdRepo == "" {
		return nil, tenant.RepoKey{}, status.Error(codes.InvalidArgument, "rpc: missing x-stravin-tenant-id or x-stravin-repo-id metadata")
	}
	if subtle.ConstantTimeCompare([]byte(mdTenant), []byte(rk.TenantID)) != 1 {
		return nil, tenant.RepoKey{}, status.Error(codes.PermissionDenied, "rpc: tenant_id metadata does not match request")
	}
	if subtle.ConstantTimeCompare([]byte(mdRepo), []byte(rk.RepoID)) != 1 {
		return nil, tenant.RepoKey{}, status.Error(codes.PermissionDenied, "rpc: repo_id metadata does not match request")
	}
	authedCtx, err := tenant.WithRepoKey(ctx, rk)
	if err != nil {
		return nil, tenant.RepoKey{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return authedCtx, rk, nil
}

type gitDataBlobServerStreamAdapter struct {
	ctx    context.Context
	stream grpc.ServerStream
}

func (a gitDataBlobServerStreamAdapter) Send(c *BlobChunk) error  { return a.stream.SendMsg(c) }
func (a gitDataBlobServerStreamAdapter) Context() context.Context { return a.ctx }

type gitDataBlobClientStreamAdapter struct {
	stream grpc.ClientStream
}

func (a gitDataBlobClientStreamAdapter) Recv() (*BlobChunk, error) {
	c := new(BlobChunk)
	return c, a.stream.RecvMsg(c)
}

func (a gitDataBlobClientStreamAdapter) Context() context.Context { return a.stream.Context() }

// RegisterGitDataServer registers handler on s under GitDataServiceName.
func RegisterGitDataServer(s grpc.ServiceRegistrar, h GitDataHandler) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: GitDataServiceName,
		HandlerType: (*GitDataHandler)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "GetDiff",
				Handler:    gitDataGetDiffHandler,
			},
			{
				MethodName: "ListTree",
				Handler:    gitDataListTreeHandler,
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "GetBlob",
				ServerStreams: true,
				ClientStreams: false,
				Handler:       gitDataGetBlobHandler,
			},
		},
	}, h)
}

func gitDataGetDiffHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(GetDiffRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	invoke := func(callCtx context.Context, req any) (any, error) {
		r := req.(*GetDiffRequest)
		authedCtx, _, err := AuthorizeGitDataContext(callCtx, r.TenantID, r.RepoID)
		if err != nil {
			return nil, err
		}
		return srv.(GitDataHandler).GetDiff(authedCtx, r)
	}
	if interceptor == nil {
		return invoke(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GitDataGetDiffMethod,
	}
	return interceptor(ctx, in, info, invoke)
}

func gitDataListTreeHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ListTreeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	invoke := func(callCtx context.Context, req any) (any, error) {
		r := req.(*ListTreeRequest)
		authedCtx, _, err := AuthorizeGitDataContext(callCtx, r.TenantID, r.RepoID)
		if err != nil {
			return nil, err
		}
		return srv.(GitDataHandler).ListTree(authedCtx, r)
	}
	if interceptor == nil {
		return invoke(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GitDataListTreeMethod,
	}
	return interceptor(ctx, in, info, invoke)
}

func gitDataGetBlobHandler(srv any, stream grpc.ServerStream) error {
	in := new(GetBlobRequest)
	if err := stream.RecvMsg(in); err != nil {
		return err
	}
	authedCtx, _, err := AuthorizeGitDataContext(stream.Context(), in.TenantID, in.RepoID)
	if err != nil {
		return err
	}
	return srv.(GitDataHandler).GetBlob(authedCtx, in, gitDataBlobServerStreamAdapter{
		ctx:    authedCtx,
		stream: stream,
	})
}

type grpcGitDataClient struct {
	cc grpc.ClientConnInterface
}

// NewGRPCGitDataClient returns a GitDataClient backed by a gRPC ClientConnInterface.
func NewGRPCGitDataClient(cc grpc.ClientConnInterface) GitDataClient {
	return &grpcGitDataClient{cc: cc}
}

func (c *grpcGitDataClient) GetDiff(ctx context.Context, req *GetDiffRequest, opts ...grpc.CallOption) (*GetDiffResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "rpc: GetDiff request is nil")
	}
	outCtx, err := ensureGitDataOutgoingMetadata(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}
	out := new(GetDiffResponse)
	callOpts := append([]grpc.CallOption{grpc.ForceCodec(GitDataCodec())}, opts...)
	if err := c.cc.Invoke(outCtx, GitDataGetDiffMethod, req, out, callOpts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *grpcGitDataClient) GetBlob(ctx context.Context, req *GetBlobRequest, opts ...grpc.CallOption) (GitDataBlobClientStream, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "rpc: GetBlob request is nil")
	}
	outCtx, err := ensureGitDataOutgoingMetadata(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}
	desc := &grpc.StreamDesc{
		StreamName:    "GetBlob",
		ServerStreams: true,
		ClientStreams: false,
	}
	callOpts := append([]grpc.CallOption{grpc.ForceCodec(GitDataCodec())}, opts...)
	cs, err := c.cc.NewStream(outCtx, desc, GitDataGetBlobMethod, callOpts...)
	if err != nil {
		return nil, err
	}
	if err := cs.SendMsg(req); err != nil {
		return nil, err
	}
	if err := cs.CloseSend(); err != nil {
		return nil, err
	}
	return gitDataBlobClientStreamAdapter{stream: cs}, nil
}

func (c *grpcGitDataClient) ListTree(ctx context.Context, req *ListTreeRequest, opts ...grpc.CallOption) (*ListTreeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "rpc: ListTree request is nil")
	}
	outCtx, err := ensureGitDataOutgoingMetadata(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}
	out := new(ListTreeResponse)
	callOpts := append([]grpc.CallOption{grpc.ForceCodec(GitDataCodec())}, opts...)
	if err := c.cc.Invoke(outCtx, GitDataListTreeMethod, req, out, callOpts...); err != nil {
		return nil, err
	}
	return out, nil
}

func ensureGitDataOutgoingMetadata(ctx context.Context, tenantID, repoID string) (context.Context, error) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if len(md.Get(HeaderTenantID)) > 0 || len(md.Get(HeaderRepoID)) > 0 {
			return ctx, nil
		}
	}
	return InjectGitDataMetadata(ctx, tenantID, repoID)
}
