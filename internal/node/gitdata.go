package node

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	defaultDiffContextLines  = 3
	maxDiffContextLines      = 100
	defaultDiffMaxFiles      = 500
	maxDiffMaxFiles          = 2000
	defaultMaxPatchBytes     = 1 << 20 // 1 MiB
	blobSniffBytes           = 8192
	blobStreamChunkBytes     = 64 * 1024
	defaultListRefsLimit     = 100
	maxListRefsLimit         = 500
	defaultCommitLogLimit    = 20
	maxCommitLogLimit        = 100
	maxCommitCountCap        = 100000
	maxCommitObjectBytes     = 1 << 20 // 1 MiB
	maxTreePathEntries       = 200
	treePathScanCommitLimit  = 200
	gitDataSubprocessTimeout = 10 * time.Second
	commitLogFormat          = "%H%x00%T%x00%P%x00%an%x00%ae%x00%aI%x00%cn%x00%ce%x00%cI%x00%s%x00%b"
	commitSummaryFormat      = "%H%x00%an%x00%aI%x00%s"
	treePathNameOnlyFormat   = "%x1e%H%x00%an%x00%aI%x00%s%x00"
)

// GitDataServerConfig configures the read-only GitData gRPC service on stravin-node.
type GitDataServerConfig struct {
	Materializer *Materializer
	GitDataExec  gitexec.GitDataExecutor
	Telemetry    *telemetry.Provider
	Metrics      *Metrics
	Logger       *slog.Logger
	Region       string
	BlobChunkSz  int
}

// GitDataServer implements rpc.GitDataHandler backed by the warm NVMe Materializer
// and sandboxed read-only git subprocesses.
type GitDataServer struct {
	materializer *Materializer
	gitExec      gitexec.GitDataExecutor
	tracer       trace.Tracer
	metrics      *Metrics
	logger       *slog.Logger
	blobChunkSz  int
}

var _ rpc.GitDataHandler = (*GitDataServer)(nil)

// NewGitDataServer constructs a read-only GitData gRPC handler.
func NewGitDataServer(cfg GitDataServerConfig) (*GitDataServer, error) {
	if cfg.Materializer == nil {
		return nil, errors.New("node: materializer is required for GitDataServer")
	}
	if cfg.GitDataExec == nil {
		return nil, errors.New("node: git data executor is required for GitDataServer")
	}
	tp := cfg.Telemetry
	if tp == nil {
		tp = telemetry.NewNoopProvider()
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(tp, cfg.Region)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	chunkSz := cfg.BlobChunkSz
	if chunkSz <= 0 {
		chunkSz = blobStreamChunkBytes
	}
	return &GitDataServer{
		materializer: cfg.Materializer,
		gitExec:      cfg.GitDataExec,
		tracer:       tp.Tracer("stravin-node"),
		metrics:      metrics,
		logger:       logger,
		blobChunkSz:  chunkSz,
	}, nil
}

// GetDiff computes the diff between req.BaseOID (or head's first parent) and req.HeadOID.
func (s *GitDataServer) GetDiff(ctx context.Context, req *rpc.GetDiffRequest) (*rpc.GetDiffResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "gitdata: request is nil")
	}
	repoKey, err := resolveAuthorizedRepoKey(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}

	ctx = telemetry.ExtractGRPC(ctx)
	ctx, span := s.tracer.Start(ctx, "node.gitdata.get_diff", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repoKey.TenantID)).
			RepoID(string(repoKey.RepoID)).
			GitOperation("gitdata_diff").
			Build()...,
	))
	defer span.End()

	resp, err := s.handleGetDiff(ctx, repoKey, req)
	s.recordMetricsAndSpan(ctx, span, "gitdata_diff", start, err)
	return resp, err
}

func (s *GitDataServer) handleGetDiff(ctx context.Context, repoKey tenant.RepoKey, req *rpc.GetDiffRequest) (*rpc.GetDiffResponse, error) {
	if !gitexec.IsValidHexOID(req.HeadOID) {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: invalid head_oid %q", req.HeadOID)
	}
	if req.BaseOID != "" && !gitexec.IsValidHexOID(req.BaseOID) {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: invalid base_oid %q", req.BaseOID)
	}

	offset, err := parsePageToken(req.PageToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: %v", err)
	}

	contextLines := req.ContextLines
	if contextLines == 0 {
		contextLines = defaultDiffContextLines
	}
	if contextLines > maxDiffContextLines {
		contextLines = maxDiffContextLines
	}
	maxFiles := req.MaxFiles
	if maxFiles == 0 {
		maxFiles = defaultDiffMaxFiles
	}
	if maxFiles > maxDiffMaxFiles {
		maxFiles = maxDiffMaxFiles
	}
	maxPatchBytes := req.MaxPatchBytes
	if maxPatchBytes == 0 {
		maxPatchBytes = defaultMaxPatchBytes
	}

	syncRes, err := s.materializer.SyncAndPin(ctx, repoKey)
	if err != nil {
		return nil, mapSyncError(repoKey, err)
	}
	defer syncRes.Handle.Release()
	repoDir := syncRes.Handle.Path()

	if _, err := s.gitExec.ExecRevParse(ctx, gitexec.RevParseRequest{
		Repo: repoKey, RepoDir: repoDir, Rev: req.HeadOID + "^{tree}",
	}); err != nil {
		return nil, status.Errorf(codes.NotFound, "gitdata: head_oid %s not found", req.HeadOID)
	}

	effectiveBase, mergeBaseOID, err := s.resolveDiffBase(ctx, repoKey, repoDir, string(syncRes.Meta.EffectiveObjectFormat()), req)
	if err != nil {
		return nil, err
	}

	var rawBuf bytes.Buffer
	if err := s.gitExec.ExecDiffTreeRaw(ctx, gitexec.DiffTreeRawRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		BaseOID: effectiveBase,
		HeadOID: req.HeadOID,
		Stdout:  &rawBuf,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: diff-tree raw: %v", err)
	}

	rawEntries, err := parseRawDiffTree(rawBuf.Bytes())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	if offset > len(rawEntries) {
		offset = len(rawEntries)
	}
	end := offset + int(maxFiles)
	nextToken := ""
	if end < len(rawEntries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(rawEntries)
	}
	pageEntries := rawEntries[offset:end]

	blobOIDs := make([]string, 0, len(pageEntries)*2)
	for _, e := range pageEntries {
		if e.OldBlobOID != "" {
			blobOIDs = append(blobOIDs, e.OldBlobOID)
		}
		if e.NewBlobOID != "" {
			blobOIDs = append(blobOIDs, e.NewBlobOID)
		}
	}
	blobMeta, err := s.gitExec.ExecCatFileBatchCheck(ctx, gitexec.CatFileBatchCheckRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		OIDs:    blobOIDs,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: cat-file batch-check: %v", err)
	}

	files := make([]rpc.DiffFileEntry, 0, len(pageEntries))
	for _, raw := range pageEntries {
		entry, err := s.buildDiffFileEntry(ctx, repoKey, repoDir, effectiveBase, req.HeadOID, contextLines, maxPatchBytes, raw, blobMeta)
		if err != nil {
			return nil, err
		}
		files = append(files, entry)
	}

	return &rpc.GetDiffResponse{
		BaseOID:       effectiveBase,
		HeadOID:       req.HeadOID,
		MergeBaseOID:  mergeBaseOID,
		Files:         files,
		NextPageToken: nextToken,
	}, nil
}

func (s *GitDataServer) resolveDiffBase(
	ctx context.Context,
	repoKey tenant.RepoKey,
	repoDir string,
	objFormat string,
	req *rpc.GetDiffRequest,
) (effectiveBase, mergeBaseOID string, err error) {
	if req.BaseOID == "" {
		parentOID, revErr := s.gitExec.ExecRevParse(ctx, gitexec.RevParseRequest{
			Repo: repoKey, RepoDir: repoDir, Rev: req.HeadOID + "^1",
		})
		if revErr != nil {
			if objFormat == "sha256" {
				return emptyTreeSHA256, "", nil
			}
			return emptyTreeSHA1, "", nil
		}
		return parentOID, "", nil
	}

	if _, revErr := s.gitExec.ExecRevParse(ctx, gitexec.RevParseRequest{
		Repo: repoKey, RepoDir: repoDir, Rev: req.BaseOID + "^{tree}",
	}); revErr != nil {
		return "", "", status.Errorf(codes.NotFound, "gitdata: base_oid %s not found", req.BaseOID)
	}

	if req.ThreeDot {
		mb, mbErr := s.gitExec.ExecMergeBase(ctx, gitexec.MergeBaseRequest{
			Repo: repoKey, RepoDir: repoDir, BaseOID: req.BaseOID, HeadOID: req.HeadOID,
		})
		if mbErr != nil {
			return "", "", status.Errorf(codes.FailedPrecondition, "gitdata: no merge base between %s and %s", req.BaseOID, req.HeadOID)
		}
		return mb, mb, nil
	}
	return req.BaseOID, "", nil
}

func (s *GitDataServer) buildDiffFileEntry(
	ctx context.Context,
	repoKey tenant.RepoKey,
	repoDir string,
	baseOID string,
	headOID string,
	contextLines uint32,
	maxPatchBytes uint64,
	raw rawDiffEntry,
	blobMeta map[string]gitexec.ObjectMeta,
) (rpc.DiffFileEntry, error) {
	entry := rpc.DiffFileEntry{
		Path:         raw.Path,
		OldPath:      raw.OldPath,
		ChangeKind:   raw.ChangeKind,
		OldBlobOID:   raw.OldBlobOID,
		NewBlobOID:   raw.NewBlobOID,
		OldSizeBytes: blobMeta[raw.OldBlobOID].Size,
		NewSizeBytes: blobMeta[raw.NewBlobOID].Size,
		Hunks:        []rpc.DiffHunk{},
	}

	targetOID := raw.NewBlobOID
	if targetOID == "" {
		targetOID = raw.OldBlobOID
	}

	var sample []byte
	if targetOID != "" && blobMeta[targetOID].Exists && blobMeta[targetOID].Type == "blob" && blobMeta[targetOID].Size > 0 {
		var err error
		sample, err = s.gitExec.ExecCatFileBlobHead(ctx, gitexec.CatFileBlobHeadRequest{
			Repo:     repoKey,
			RepoDir:  repoDir,
			BlobOID:  targetOID,
			MaxBytes: blobSniffBytes,
		})
		if err != nil {
			return rpc.DiffFileEntry{}, status.Errorf(codes.Internal, "gitdata: sniff blob %s: %v", targetOID, err)
		}
	}

	mimeType, isBinary := detectContentMetadata(raw.Path, sample)
	if !isBinary && raw.OldBlobOID != "" && raw.OldBlobOID != targetOID && blobMeta[raw.OldBlobOID].Exists && blobMeta[raw.OldBlobOID].Type == "blob" && blobMeta[raw.OldBlobOID].Size > 0 {
		oldSample, err := s.gitExec.ExecCatFileBlobHead(ctx, gitexec.CatFileBlobHeadRequest{
			Repo:     repoKey,
			RepoDir:  repoDir,
			BlobOID:  raw.OldBlobOID,
			MaxBytes: blobSniffBytes,
		})
		if err != nil {
			return rpc.DiffFileEntry{}, status.Errorf(codes.Internal, "gitdata: sniff old blob %s: %v", raw.OldBlobOID, err)
		}
		if _, oldBin := detectContentMetadata(raw.Path, oldSample); oldBin {
			isBinary = true
		}
	}

	entry.MimeType = mimeType
	entry.IsBinary = isBinary
	if isBinary {
		return entry, nil
	}

	paths := []string{raw.Path}
	if raw.OldPath != "" && raw.OldPath != raw.Path {
		paths = []string{raw.OldPath, raw.Path}
	}
	var patchBuf bytes.Buffer
	if err := s.gitExec.ExecDiffTreePatch(ctx, gitexec.DiffTreePatchRequest{
		Repo:         repoKey,
		RepoDir:      repoDir,
		BaseOID:      baseOID,
		HeadOID:      headOID,
		ContextLines: contextLines,
		Paths:        paths,
		Stdout:       &patchBuf,
	}); err != nil {
		return rpc.DiffFileEntry{}, status.Errorf(codes.Internal, "gitdata: diff-tree patch %s: %v", raw.Path, err)
	}

	patchBytes := patchBuf.Bytes()
	hunks, adds, dels := parseUnifiedPatch(patchBytes)
	entry.Hunks = hunks
	entry.Additions = adds
	entry.Deletions = dels
	if uint64(len(patchBytes)) > maxPatchBytes {
		entry.UnifiedPatch = append([]byte(nil), patchBytes[:maxPatchBytes]...)
		entry.PatchTruncated = true
	} else if len(patchBytes) > 0 {
		entry.UnifiedPatch = patchBytes
	}
	return entry, nil
}

// GetBlob streams raw blob bytes in bounded chunks without buffering the entire object in memory.
func (s *GitDataServer) GetBlob(ctx context.Context, req *rpc.GetBlobRequest, stream rpc.GitDataBlobStream) error {
	start := time.Now()
	if req == nil {
		return status.Error(codes.InvalidArgument, "gitdata: request is nil")
	}
	repoKey, err := resolveAuthorizedRepoKey(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return err
	}

	ctx = telemetry.ExtractGRPC(ctx)
	ctx, span := s.tracer.Start(ctx, "node.gitdata.get_blob", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repoKey.TenantID)).
			RepoID(string(repoKey.RepoID)).
			GitOperation("gitdata_blob").
			Build()...,
	))
	defer span.End()

	err = s.handleGetBlob(ctx, repoKey, req, stream)
	s.recordMetricsAndSpan(ctx, span, "gitdata_blob", start, err)
	return err
}

func (s *GitDataServer) handleGetBlob(ctx context.Context, repoKey tenant.RepoKey, req *rpc.GetBlobRequest, stream rpc.GitDataBlobStream) error {
	if !gitexec.IsValidHexOID(req.BlobOID) {
		return status.Errorf(codes.InvalidArgument, "gitdata: invalid blob_oid %q", req.BlobOID)
	}

	syncRes, err := s.materializer.SyncAndPin(ctx, repoKey)
	if err != nil {
		return mapSyncError(repoKey, err)
	}
	defer syncRes.Handle.Release()
	repoDir := syncRes.Handle.Path()

	metas, err := s.gitExec.ExecCatFileBatchCheck(ctx, gitexec.CatFileBatchCheckRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		OIDs:    []string{req.BlobOID},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "gitdata: cat-file batch-check: %v", err)
	}
	meta := metas[req.BlobOID]
	if !meta.Exists {
		return status.Errorf(codes.NotFound, "gitdata: blob %s not found", req.BlobOID)
	}
	if meta.Type != "blob" {
		return status.Errorf(codes.FailedPrecondition, "gitdata: object %s is %s, not blob", req.BlobOID, meta.Type)
	}
	if req.MaxBytes > 0 && meta.Size > req.MaxBytes {
		return status.Errorf(codes.ResourceExhausted, "gitdata: blob %s size %d exceeds max_bytes %d", req.BlobOID, meta.Size, req.MaxBytes)
	}

	writer := &blobChunkStreamWriter{
		stream:     stream,
		totalBytes: meta.Size,
		chunkSize:  s.blobChunkSz,
	}
	if err := s.gitExec.ExecCatFileBlob(ctx, gitexec.CatFileBlobRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		BlobOID: req.BlobOID,
		Stdout:  writer,
	}); err != nil {
		if writer.sendErr != nil {
			return writer.sendErr
		}
		return status.Errorf(codes.Internal, "gitdata: cat-file blob %s: %v", req.BlobOID, err)
	}

	return stream.Send(&rpc.BlobChunk{
		TotalBytes: meta.Size,
		EOF:        true,
	})
}

// ListTree returns tree entries for a given commit_oid or tree_oid and optional path.
func (s *GitDataServer) ListTree(ctx context.Context, req *rpc.ListTreeRequest) (*rpc.ListTreeResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "gitdata: request is nil")
	}
	repoKey, err := resolveAuthorizedRepoKey(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}

	ctx = telemetry.ExtractGRPC(ctx)
	ctx, span := s.tracer.Start(ctx, "node.gitdata.list_tree", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repoKey.TenantID)).
			RepoID(string(repoKey.RepoID)).
			GitOperation("gitdata_tree").
			Build()...,
	))
	defer span.End()

	resp, err := s.handleListTree(ctx, repoKey, req)
	s.recordMetricsAndSpan(ctx, span, "gitdata_tree", start, err)
	return resp, err
}

func (s *GitDataServer) handleListTree(ctx context.Context, repoKey tenant.RepoKey, req *rpc.ListTreeRequest) (*rpc.ListTreeResponse, error) {
	if req.CommitOID == "" && req.TreeOID == "" {
		return nil, status.Error(codes.InvalidArgument, "gitdata: commit_oid or tree_oid is required")
	}
	if req.CommitOID != "" && req.TreeOID != "" {
		return nil, status.Error(codes.InvalidArgument, "gitdata: specify only one of commit_oid or tree_oid")
	}
	baseOID := req.CommitOID
	if baseOID == "" {
		baseOID = req.TreeOID
	}
	if !gitexec.IsValidHexOID(baseOID) {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: invalid oid %q", baseOID)
	}

	cleanPath, err := sanitizeTreePath(req.Path)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: %v", err)
	}

	syncRes, err := s.materializer.SyncAndPin(ctx, repoKey)
	if err != nil {
		return nil, mapSyncError(repoKey, err)
	}
	defer syncRes.Handle.Release()
	repoDir := syncRes.Handle.Path()

	rev := baseOID + "^{tree}"
	if cleanPath != "" {
		rev = baseOID + ":" + cleanPath
	}
	resolvedOID, err := s.gitExec.ExecRevParse(ctx, gitexec.RevParseRequest{
		Repo: repoKey, RepoDir: repoDir, Rev: rev,
	})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "gitdata: tree %s not found", rev)
	}

	metas, err := s.gitExec.ExecCatFileBatchCheck(ctx, gitexec.CatFileBatchCheckRequest{
		Repo: repoKey, RepoDir: repoDir, OIDs: []string{resolvedOID},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: cat-file batch-check: %v", err)
	}
	meta := metas[resolvedOID]
	if !meta.Exists {
		return nil, status.Errorf(codes.NotFound, "gitdata: tree %s not found", resolvedOID)
	}
	if meta.Type != "tree" {
		return nil, status.Errorf(codes.FailedPrecondition, "gitdata: path %q resolves to %s, not tree", cleanPath, meta.Type)
	}

	var lsBuf bytes.Buffer
	if err := s.gitExec.ExecLsTree(ctx, gitexec.LsTreeRequest{
		Repo:      repoKey,
		RepoDir:   repoDir,
		TreeOID:   resolvedOID,
		Recursive: req.Recursive,
		Stdout:    &lsBuf,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: ls-tree %s: %v", resolvedOID, err)
	}

	entries, err := parseLsTreeOutput(lsBuf.Bytes(), cleanPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	return &rpc.ListTreeResponse{
		TreeOID: resolvedOID,
		Entries: entries,
	}, nil
}

// ListRefs returns repository branches, tags, default branch HEAD resolution, and counts.
func (s *GitDataServer) ListRefs(ctx context.Context, req *rpc.ListRefsRequest) (*rpc.ListRefsResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "gitdata: request is nil")
	}
	repoKey, err := resolveAuthorizedRepoKey(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}

	ctx = telemetry.ExtractGRPC(ctx)
	ctx, span := s.tracer.Start(ctx, "node.gitdata.list_refs", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repoKey.TenantID)).
			RepoID(string(repoKey.RepoID)).
			GitOperation("gitdata_refs").
			Build()...,
	))
	defer span.End()

	resp, err := s.handleListRefs(ctx, repoKey, req)
	s.recordMetricsAndSpan(ctx, span, "gitdata_refs", start, err)
	return resp, err
}

func (s *GitDataServer) handleListRefs(ctx context.Context, repoKey tenant.RepoKey, req *rpc.ListRefsRequest) (*rpc.ListRefsResponse, error) {
	if err := validateRefPrefix(req.Prefix); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: %v", err)
	}
	offset, err := parsePageToken(req.Cursor)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: invalid cursor")
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultListRefsLimit
	}
	if limit > maxListRefsLimit {
		limit = maxListRefsLimit
	}

	meta, refs, err := s.materializer.LoadAuthoritativeRefs(ctx, repoKey)
	if err != nil {
		return nil, mapSyncError(repoKey, err)
	}

	defaultRef := normalizeDefaultBranch(meta.DefaultBranch)
	sorted := make([]ddb.RefState, len(refs))
	copy(sorted, refs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].RefName < sorted[j].RefName
	})

	var (
		branchCount uint32
		tagCount    uint32
		headOID     string
		matched     = make([]rpc.RefEntry, 0, len(sorted))
	)
	for _, r := range sorted {
		kind, shortName := classifyRef(r.RefName)
		switch kind {
		case "branch":
			branchCount++
		case "tag":
			tagCount++
		}
		isDefault := defaultRef != "" && r.RefName == defaultRef
		if isDefault {
			headOID = r.OID
		}
		if req.Prefix != "" && !strings.HasPrefix(r.RefName, req.Prefix) {
			continue
		}
		matched = append(matched, rpc.RefEntry{
			Name:      r.RefName,
			ShortName: shortName,
			TargetOID: r.OID,
			Kind:      kind,
			IsDefault: isDefault,
		})
	}

	if offset > len(matched) {
		offset = len(matched)
	}
	end := offset + int(limit)
	nextCursor := ""
	if end < len(matched) {
		nextCursor = strconv.Itoa(end)
	} else {
		end = len(matched)
	}

	page := make([]rpc.RefEntry, end-offset)
	copy(page, matched[offset:end])

	return &rpc.ListRefsResponse{
		DefaultBranch: defaultRef,
		HeadOID:       headOID,
		Refs:          page,
		BranchCount:   branchCount,
		TagCount:      tagCount,
		NextCursor:    nextCursor,
	}, nil
}

// GetCommitLog returns commit metadata, raw cryptographic signature payloads,
// bounded total commit count, and optional per-path last-commit summaries.
func (s *GitDataServer) GetCommitLog(ctx context.Context, req *rpc.GetCommitLogRequest) (*rpc.GetCommitLogResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "gitdata: request is nil")
	}
	repoKey, err := resolveAuthorizedRepoKey(ctx, req.TenantID, req.RepoID)
	if err != nil {
		return nil, err
	}

	ctx = telemetry.ExtractGRPC(ctx)
	ctx, span := s.tracer.Start(ctx, "node.gitdata.get_commit_log", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repoKey.TenantID)).
			RepoID(string(repoKey.RepoID)).
			GitOperation("gitdata_commits").
			Build()...,
	))
	defer span.End()

	resp, err := s.handleGetCommitLog(ctx, repoKey, req)
	s.recordMetricsAndSpan(ctx, span, "gitdata_commits", start, err)
	return resp, err
}

func (s *GitDataServer) handleGetCommitLog(ctx context.Context, repoKey tenant.RepoKey, req *rpc.GetCommitLogRequest) (*rpc.GetCommitLogResponse, error) {
	ref := req.Ref
	if ref == "" {
		ref = "HEAD"
	}
	if !isSafeRefOrOID(ref) {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: invalid ref %q", req.Ref)
	}

	cleanPath, err := sanitizeTreePath(req.Path)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "gitdata: %v", err)
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultCommitLogLimit
	}
	if limit > maxCommitLogLimit {
		limit = maxCommitLogLimit
	}

	syncRes, err := s.materializer.SyncAndPin(ctx, repoKey)
	if err != nil {
		return nil, mapSyncError(repoKey, err)
	}
	defer syncRes.Handle.Release()
	repoDir := syncRes.Handle.Path()

	// Empty repository with no commits yet returns an empty log gracefully.
	if len(syncRes.Refs) == 0 && isDefaultOrHeadRef(ref, syncRes.Meta.DefaultBranch) {
		return &rpc.GetCommitLogResponse{
			TotalCommits: 0,
			Commits:      []rpc.CommitEntry{},
		}, nil
	}

	commitOID, err := s.gitExec.ExecRevParse(ctx, gitexec.RevParseRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		Rev:     ref + "^{commit}",
		Timeout: gitDataSubprocessTimeout,
	})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "gitdata: ref %q not found", ref)
	}

	var logBuf bytes.Buffer
	if err := s.gitExec.ExecLog(ctx, gitexec.LogRequest{
		Repo:      repoKey,
		RepoDir:   repoDir,
		CommitOID: commitOID,
		Path:      cleanPath,
		MaxCount:  limit,
		Format:    commitLogFormat,
		Stdout:    &logBuf,
		Timeout:   gitDataSubprocessTimeout,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: log %s: %v", commitOID, err)
	}

	commits, err := parseCommitLogOutput(logBuf.Bytes())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	if len(commits) > 0 {
		oids := make([]string, len(commits))
		for i, c := range commits {
			oids[i] = c.OID
		}
		rawObjects, err := s.gitExec.ExecCatFileBatch(ctx, gitexec.CatFileBatchRequest{
			Repo:           repoKey,
			RepoDir:        repoDir,
			OIDs:           oids,
			MaxObjectBytes: maxCommitObjectBytes,
			Timeout:        gitDataSubprocessTimeout,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "gitdata: cat-file batch commits: %v", err)
		}
		for i := range commits {
			if raw, ok := rawObjects[commits[i].OID]; ok {
				commits[i].RawSignature, commits[i].SignedPayload = extractCommitSignature(raw)
			}
		}
	}

	totalCommits, err := s.gitExec.ExecRevListCount(ctx, gitexec.RevListCountRequest{
		Repo:      repoKey,
		RepoDir:   repoDir,
		CommitOID: commitOID,
		Path:      cleanPath,
		MaxCount:  maxCommitCountCap,
		Timeout:   gitDataSubprocessTimeout,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: rev-list count %s: %v", commitOID, err)
	}

	var treePathCommits map[string]rpc.CommitSummary
	if req.IncludePathLastCommits {
		treePathCommits, err = s.resolveTreePathCommits(ctx, repoKey, repoDir, commitOID, cleanPath)
		if err != nil {
			return nil, err
		}
	}

	return &rpc.GetCommitLogResponse{
		TotalCommits:    totalCommits,
		Commits:         commits,
		TreePathCommits: treePathCommits,
	}, nil
}

func (s *GitDataServer) resolveTreePathCommits(
	ctx context.Context,
	repoKey tenant.RepoKey,
	repoDir string,
	commitOID string,
	cleanPath string,
) (map[string]rpc.CommitSummary, error) {
	treeRev := commitOID + "^{tree}"
	if cleanPath != "" {
		treeRev = commitOID + ":" + cleanPath
	}
	treeOID, err := s.gitExec.ExecRevParse(ctx, gitexec.RevParseRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		Rev:     treeRev,
		Timeout: gitDataSubprocessTimeout,
	})
	if err != nil {
		return nil, nil
	}

	metas, err := s.gitExec.ExecCatFileBatchCheck(ctx, gitexec.CatFileBatchCheckRequest{
		Repo:    repoKey,
		RepoDir: repoDir,
		OIDs:    []string{treeOID},
		Timeout: gitDataSubprocessTimeout,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: cat-file batch-check tree: %v", err)
	}
	if !metas[treeOID].Exists || metas[treeOID].Type != "tree" {
		return nil, nil
	}

	var lsBuf bytes.Buffer
	if err := s.gitExec.ExecLsTree(ctx, gitexec.LsTreeRequest{
		Repo:      repoKey,
		RepoDir:   repoDir,
		TreeOID:   treeOID,
		Recursive: false,
		Stdout:    &lsBuf,
		Timeout:   gitDataSubprocessTimeout,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: ls-tree %s: %v", treeOID, err)
	}

	entries, err := parseLsTreeOutput(lsBuf.Bytes(), cleanPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if len(entries) == 0 {
		return map[string]rpc.CommitSummary{}, nil
	}
	if len(entries) > maxTreePathEntries {
		entries = entries[:maxTreePathEntries]
	}

	wanted := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		wanted[e.Name] = struct{}{}
	}

	var scanBuf bytes.Buffer
	if err := s.gitExec.ExecLog(ctx, gitexec.LogRequest{
		Repo:      repoKey,
		RepoDir:   repoDir,
		CommitOID: commitOID,
		Path:      cleanPath,
		MaxCount:  treePathScanCommitLimit,
		Format:    treePathNameOnlyFormat,
		NameOnly:  true,
		Stdout:    &scanBuf,
		Timeout:   gitDataSubprocessTimeout,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "gitdata: log path scan %s: %v", commitOID, err)
	}

	result := parseNameOnlyLogOutput(scanBuf.Bytes(), cleanPath, wanted)
	if len(result) == len(wanted) {
		return result, nil
	}

	// Fallback for entries untouched in the recent commit scan window.
	for _, e := range entries {
		if _, ok := result[e.Name]; ok {
			continue
		}
		var oneBuf bytes.Buffer
		if err := s.gitExec.ExecLog(ctx, gitexec.LogRequest{
			Repo:      repoKey,
			RepoDir:   repoDir,
			CommitOID: commitOID,
			Path:      e.Path,
			MaxCount:  1,
			Format:    commitSummaryFormat,
			Stdout:    &oneBuf,
			Timeout:   gitDataSubprocessTimeout,
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "gitdata: log last commit %s: %v", e.Path, err)
		}
		if summary, ok := parseCommitSummaryOutput(oneBuf.Bytes()); ok {
			result[e.Name] = summary
		}
	}
	return result, nil
}

func isDefaultOrHeadRef(ref, defaultBranch string) bool {
	if ref == "HEAD" {
		return true
	}
	normDefault := normalizeDefaultBranch(defaultBranch)
	if normDefault == "" {
		return false
	}
	return ref == normDefault || ref == strings.TrimPrefix(normDefault, "refs/heads/")
}

func (s *GitDataServer) recordMetricsAndSpan(ctx context.Context, span trace.Span, op string, start time.Time, err error) {
	statusLabel := "ok"
	if err != nil {
		statusLabel = "error"
		recordSpanError(span, err)
	}
	s.metrics.RecordRequest(ctx, op, statusLabel, time.Since(start))
}

func resolveAuthorizedRepoKey(ctx context.Context, reqTenantID, reqRepoID string) (tenant.RepoKey, error) {
	rk, err := tenant.NewRepoKey(reqTenantID, reqRepoID)
	if err != nil {
		return tenant.RepoKey{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if ctxKey, ok := tenant.RepoKeyFromContext(ctx); ok {
		if ctxKey != rk {
			return tenant.RepoKey{}, status.Error(codes.PermissionDenied, "gitdata: request tenant/repo does not match authorized context")
		}
	}
	return rk, nil
}

func mapSyncError(repoKey tenant.RepoKey, err error) error {
	if errors.Is(err, ddb.ErrNotFound) {
		return status.Errorf(codes.NotFound, "repository %s not found", repoKey)
	}
	return status.Errorf(codes.Internal, "sync repository %s: %v", repoKey, err)
}

func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(token)
	if err != nil || n < 0 {
		return 0, errors.New("invalid page_token")
	}
	return n, nil
}

type blobChunkStreamWriter struct {
	stream     rpc.GitDataBlobStream
	totalBytes uint64
	chunkSize  int
	sendErr    error
}

func (w *blobChunkStreamWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > w.chunkSize {
			n = w.chunkSize
		}
		chunk := make([]byte, n)
		copy(chunk, p[:n])
		if err := w.stream.Send(&rpc.BlobChunk{
			Data:       chunk,
			TotalBytes: w.totalBytes,
		}); err != nil {
			w.sendErr = err
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}
