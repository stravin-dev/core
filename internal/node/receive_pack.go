package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

// PackPutter is the narrow S3 write seam for durability-before-visibility pack uploads.
// *s3.Store satisfies this interface directly.
type PackPutter interface {
	PutPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string, packData, idxData []byte) error
}

// PushCommitter is the narrow DynamoDB write seam for atomic Ref-CAS push commits.
// *ddb.Repository satisfies this interface directly.
type PushCommitter interface {
	CommitPush(ctx context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error)
}

// HookEvaluator is the narrow Tier 1 synchronous Wasm hook evaluation seam.
// *wasm.Engine satisfies this interface directly.
type HookEvaluator interface {
	EvaluatePush(ctx context.Context, pushCtx wasm.PushContext, reader wasm.ObjectReader) (wasm.PipelineResult, error)
}

// ReceivePackConfig configures the concrete stravin-node git-receive-pack WriteHandler.
type ReceivePackConfig struct {
	Materializer   *Materializer
	Cache          *cache.Cache
	Packs          PackPutter
	Committer      PushCommitter
	LeaseRefresher *LeaseRefresher
	HookEngine     HookEvaluator
	ThinCompleter  pack.ThinPackCompleter
	PackLimits     pack.Limits
	Region         lease.Region
	Telemetry      *telemetry.Provider
	Metrics        *Metrics
	Logger         *slog.Logger
}

// ReceivePackHandler implements WriteHandler for stravin-node's git-receive-pack write path.
type ReceivePackHandler struct {
	materializer   *Materializer
	cache          *cache.Cache
	packs          PackPutter
	committer      PushCommitter
	leaseRefresher *LeaseRefresher
	hookEngine     HookEvaluator
	thinCompleter  pack.ThinPackCompleter
	packLimits     pack.Limits
	region         lease.Region
	tracer         trace.Tracer
	metrics        *Metrics
	logger         *slog.Logger
}

var _ WriteHandler = (*ReceivePackHandler)(nil)

// NewReceivePackHandler constructs a validated ReceivePackHandler.
func NewReceivePackHandler(cfg ReceivePackConfig) (*ReceivePackHandler, error) {
	if cfg.Materializer == nil {
		return nil, errors.New("node: materializer is required")
	}
	if cfg.Cache == nil {
		return nil, errors.New("node: cache is required")
	}
	if cfg.Packs == nil {
		return nil, errors.New("node: pack putter is required")
	}
	if cfg.Committer == nil {
		return nil, errors.New("node: push committer is required")
	}
	if cfg.LeaseRefresher == nil {
		return nil, errors.New("node: lease refresher is required")
	}
	limits := cfg.PackLimits
	if limits == (pack.Limits{}) {
		limits = pack.DefaultLimits()
	}
	completer := cfg.ThinCompleter
	if completer == nil {
		completer = pack.NewPureGoThinCompleter(limits)
	}
	tp := cfg.Telemetry
	if tp == nil {
		tp = telemetry.NewNoopProvider()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &ReceivePackHandler{
		materializer:   cfg.Materializer,
		cache:          cfg.Cache,
		packs:          cfg.Packs,
		committer:      cfg.Committer,
		leaseRefresher: cfg.LeaseRefresher,
		hookEngine:     cfg.HookEngine,
		thinCompleter:  completer,
		packLimits:     limits,
		region:         cfg.Region,
		tracer:         tp.Tracer("stravin-node"),
		metrics:        cfg.Metrics,
		logger:         logger,
	}, nil
}

// HandleReceivePack executes the git-receive-pack write path over the GitWire stream.
func (h *ReceivePackHandler) HandleReceivePack(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error {
	repo := meta.RepoKey()

	if boolHeader(ctx, HeaderAdvertiseRefs) {
		return h.serveAdvertiseRefs(ctx, repo, stream)
	}

	statelessRPC := boolHeader(ctx, HeaderStatelessRPC)
	if !statelessRPC {
		if err := h.sendAdvertiseRefs(ctx, repo, stream); err != nil {
			return err
		}
	}

	br := bufio.NewReader(&streamFrameReader{stream: stream})
	cmds, caps, err := ParseReceiveCommands(br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
		}
		if errors.Is(err, ErrTooManyReceiveCommands) {
			useSideBand := caps["side-band-64k"]
			reportStatus := caps["report-status"] || useSideBand
			reason := fmt.Sprintf("push exceeds max atomic ref limit (%d)", ddb.MaxAtomicPushRefs)
			return h.sendAllRejected(stream, reportStatus, useSideBand, "unpack ok", cmds, reason, []string{"remote: " + reason + "\n"})
		}
		return status.Errorf(codes.InvalidArgument, "node: parse receive-pack commands: %v", err)
	}
	if len(cmds) == 0 {
		return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
	}

	useSideBand := caps["side-band-64k"]
	reportStatus := caps["report-status"] || useSideBand

	// 1. Fail-closed regional write-lease check before accepting writes.
	if err := h.leaseRefresher.CheckWriteAllowed(); err != nil {
		return status.Errorf(codes.FailedPrecondition, "node: regional write lease unavailable: %v", err)
	}
	currentLease, err := h.leaseRefresher.Current()
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "node: regional write lease unavailable: %v", err)
	}

	// 2. Load authoritative repo metadata and current refs from DynamoDB.
	repoMeta, currentRefs, err := h.materializer.LoadAuthoritativeRefs(ctx, repo)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return status.Errorf(codes.NotFound, "repository %s not found", repo)
		}
		return status.Errorf(codes.Internal, "node: load refs %s: %v", repo, err)
	}

	// 3. Enforce repository write state, home region, and failover_epoch fencing.
	if repoMeta.WriteState != lease.ModeActive {
		return status.Errorf(codes.FailedPrecondition, "node: repository %s write state is %s", repo, repoMeta.WriteState)
	}
	if h.region != "" && repoMeta.HomeRegion != h.region {
		return status.Errorf(codes.FailedPrecondition, "node: region %s is not home region %s for %s", h.region, repoMeta.HomeRegion, repo)
	}
	if err := h.leaseRefresher.CheckEpochFence(repoMeta.FailoverEpoch); err != nil {
		return status.Errorf(codes.FailedPrecondition, "node: failover_epoch fenced: %v", err)
	}

	// 4. Reject pushes exceeding DynamoDB's atomic transaction ref limit (>45 refs).
	if len(cmds) > ddb.MaxAtomicPushRefs {
		reason := fmt.Sprintf("push exceeds max atomic ref limit (%d)", ddb.MaxAtomicPushRefs)
		return h.sendAllRejected(stream, reportStatus, useSideBand, "unpack ok", cmds, reason, []string{"remote: " + reason + "\n"})
	}

	// 5. Optimistic stale-old_oid fast reject before pack indexing, S3 upload, or Wasm hooks.
	if stale := findStaleRefUpdates(cmds, currentRefs); len(stale) > 0 {
		statuses := make([]refStatus, 0, len(cmds))
		for _, c := range cmds {
			if stale[c.RefName] {
				statuses = append(statuses, refStatus{RefName: c.RefName, Err: "stale old_oid"})
			} else {
				statuses = append(statuses, refStatus{RefName: c.RefName, Err: "atomic push failed"})
			}
		}
		return h.sendStatusAndFinish(stream, reportStatus, useSideBand, "unpack ok", statuses, nil)
	}

	// 6. Ingest packfile, run thin-pack completion, and build .idx v2 when objects are pushed.
	needsPack := pushNeedsPack(cmds)
	var (
		completedPack []byte
		idxBytes      []byte
		packSHA256    string
		packReader    *pack.PackReader
		syncHandle    *cache.RepoHandle
	)
	if needsPack {
		ingested, ingestErr := h.ingestAndIndexPack(ctx, repo, repoMeta, cmds, br)
		if ingestErr != nil {
			errMsg := fmt.Sprintf("unpack %v", ingestErr)
			return h.sendAllRejected(stream, reportStatus, useSideBand, errMsg, cmds, "unpack failed", nil)
		}
		completedPack = ingested.packBytes
		idxBytes = ingested.idxBytes
		packSHA256 = ingested.packSHA256
		packReader = ingested.reader
		syncHandle = ingested.handle
		defer syncHandle.Release()
	}

	// 7. Overlap S3 PutPack (Goroutine A) and Tier 1 Wasm Hooks (Goroutine B).
	coord := &pushOverlapCoordinator{
		packs:      h.packs,
		hookEngine: h.hookEngine,
		tracer:     h.tracer,
	}
	overlapRes := coord.Execute(ctx, pushOverlapRequest{
		Repo:          repo,
		Meta:          repoMeta,
		Updates:       cmds,
		NeedsPack:     needsPack,
		PackSHA256:    packSHA256,
		CompletedPack: completedPack,
		IdxBytes:      idxBytes,
		PackReader:    packReader,
	})

	stderrLines := formatHookWarnings(overlapRes.HookResult.Warnings)
	if overlapRes.HookErr != nil {
		msg := fmt.Sprintf("hook error: %v", overlapRes.HookErr)
		stderrLines = append(stderrLines, "remote: "+msg+"\n")
		return h.sendAllRejected(stream, reportStatus, useSideBand, "unpack ok", cmds, msg, stderrLines)
	}
	if !overlapRes.HookResult.Allowed {
		rejectMsg := strings.TrimSpace(overlapRes.HookResult.Message)
		if rejectMsg == "" {
			rejectMsg = fmt.Sprintf("rejected by hook %s", overlapRes.HookResult.RejectedBy)
		}
		stderrLines = append(stderrLines, "remote: "+rejectMsg+"\n")
		statuses := buildHookRejectStatuses(cmds, overlapRes.HookResult, rejectMsg)
		return h.sendStatusAndFinish(stream, reportStatus, useSideBand, "unpack ok", statuses, stderrLines)
	}
	if overlapRes.S3Err != nil {
		return status.Errorf(codes.Internal, "node: s3 put pack %s: %v", repo, overlapRes.S3Err)
	}

	// 8. Atomic DynamoDB TransactWriteItems (Ref-CAS + Ref History w/ traceparent + Manifest).
	originRegion := h.region
	if originRegion == "" {
		originRegion = repoMeta.HomeRegion
	}
	var packUpload *ddb.PackUpload
	if needsPack {
		packUpload = &ddb.PackUpload{
			PackID:       packSHA256,
			SizeBytes:    int64(len(completedPack)),
			OriginRegion: originRegion,
		}
	}

	ddbCtx, ddbSpan := h.tracer.Start(ctx, "ddb.transact_write", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repo.TenantID)).
			RepoID(string(repo.RepoID)).
			HomeRegion(string(repoMeta.HomeRegion)).
			FailoverEpoch(int64(currentLease.Epoch)).
			Build()...,
	))
	if packSHA256 != "" {
		ddbSpan.SetAttributes(telemetry.AttrPackID.String(packSHA256))
	}
	traceParent := telemetry.TraceparentFromContext(ddbCtx)
	if traceParent == "" {
		traceParent = meta.TraceParent
	}

	_, commitErr := h.committer.CommitPush(ddbCtx, ddb.CommitPushInput{
		Repo:           repo,
		RefShards:      repoMeta.RefShards,
		ManifestShards: repoMeta.ManifestShards,
		Epoch:          currentLease.Epoch,
		OriginRegion:   originRegion,
		TraceParent:    traceParent,
		Updates:        cmds,
		Pack:           packUpload,
	})
	if commitErr != nil {
		ddbSpan.RecordError(commitErr)
		ddbSpan.SetStatus(otelcodes.Error, commitErr.Error())
		ddbSpan.End()

		if errors.Is(commitErr, ddb.ErrRefCASConflict) {
			return h.sendCASConflict(stream, reportStatus, useSideBand, cmds, commitErr, stderrLines)
		}
		if errors.Is(commitErr, ddb.ErrTooManyRefs) {
			return h.sendAllRejected(stream, reportStatus, useSideBand, "unpack ok", cmds, "too many refs", stderrLines)
		}
		return status.Errorf(codes.Internal, "node: commit push %s: %v", repo, commitErr)
	}
	ddbSpan.End()

	// 9. Install committed pack into local warm NVMe cache.
	if needsPack {
		if err := h.cache.InstallPack(repo, packSHA256, completedPack, idxBytes); err != nil {
			h.logger.WarnContext(ctx, "node: warm cache install after commit failed",
				"tenant_id", string(repo.TenantID),
				"repo_id", string(repo.RepoID),
				"pack_id", packSHA256,
				"error", err,
			)
		} else if h.metrics != nil {
			h.metrics.RecordCacheBytes(ctx, h.cache.Stats().TotalBytes)
		}
	}

	// 10. ACK client with report-status ok <ref>.
	statuses := make([]refStatus, 0, len(cmds))
	for _, c := range cmds {
		statuses = append(statuses, refStatus{RefName: c.RefName})
	}
	return h.sendStatusAndFinish(stream, reportStatus, useSideBand, "unpack ok", statuses, stderrLines)
}

func (h *ReceivePackHandler) serveAdvertiseRefs(ctx context.Context, repo tenant.RepoKey, stream rpc.GitWireStream) error {
	if err := h.sendAdvertiseRefs(ctx, repo, stream); err != nil {
		return err
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
}

func (h *ReceivePackHandler) sendAdvertiseRefs(ctx context.Context, repo tenant.RepoKey, stream rpc.GitWireStream) error {
	repoMeta, refs, err := h.materializer.LoadAuthoritativeRefs(ctx, repo)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return status.Errorf(codes.NotFound, "repository %s not found", repo)
		}
		return status.Errorf(codes.Internal, "node: load refs %s: %v", repo, err)
	}
	payload := AdvertiseRefs(refs, repoMeta.DefaultBranch, AdvertiseOptions{
		IncludeServiceHeader: boolHeader(ctx, HeaderSmartHTTPServiceHeader),
		ServiceName:          string(rpc.OpReceivePack),
	})
	if len(payload) > 0 {
		return stream.Send(&rpc.Frame{Data: payload})
	}
	return nil
}

type ingestedPack struct {
	packBytes  []byte
	idxBytes   []byte
	packSHA256 string
	reader     *pack.PackReader
	handle     *cache.RepoHandle
}

func (h *ReceivePackHandler) ingestAndIndexPack(
	ctx context.Context,
	repo tenant.RepoKey,
	repoMeta ddb.RepoMeta,
	cmds []ddb.RefUpdate,
	r io.Reader,
) (ingestedPack, error) {
	packCtx, span := h.tracer.Start(ctx, "node.pack.index_validate", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repo.TenantID)).
			RepoID(string(repo.RepoID)).
			HomeRegion(string(repoMeta.HomeRegion)).
			FailoverEpoch(int64(repoMeta.FailoverEpoch)).
			Build()...,
	))
	defer span.End()

	maxBytes := h.packLimits.MaxPackSize
	if maxBytes <= 0 {
		maxBytes = pack.DefaultLimits().MaxPackSize
	}
	rawPack, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, fmt.Errorf("read pack stream: %w", err)
	}
	if int64(len(rawPack)) > maxBytes {
		err = fmt.Errorf("pack exceeds max size %d: %w", maxBytes, pack.ErrLimitExceeded)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, err
	}
	if len(rawPack) == 0 {
		err = errors.New("missing packfile data")
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, err
	}

	syncRes, err := h.materializer.SyncAndPin(packCtx, repo)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, fmt.Errorf("sync base packs: %w", err)
	}
	baseResolver := syncRes.Handle.BaseResolver()

	completed, err := h.thinCompleter.Complete(packCtx, rawPack, baseResolver)
	if err != nil {
		syncRes.Handle.Release()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, fmt.Errorf("thin-pack completion: %w", err)
	}

	parsed, err := pack.Parse(bytes.NewReader(completed), pack.ParseOptions{
		Limits: h.packLimits,
	})
	if err != nil {
		syncRes.Handle.Release()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, fmt.Errorf("parse completed pack: %w", err)
	}

	// Verify every pushed non-delete target OID is reachable in the new pack or existing repo packs.
	for _, c := range cmds {
		if c.IsDelete() {
			continue
		}
		oid, err := pack.ParseOID(c.NewOID)
		if err != nil {
			syncRes.Handle.Release()
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
			return ingestedPack{}, fmt.Errorf("invalid target oid for %s: %w", c.RefName, err)
		}
		if _, inNew := parsed.Objects[oid]; !inNew {
			if _, _, baseErr := baseResolver.ResolveBase(oid); baseErr != nil {
				syncRes.Handle.Release()
				err = fmt.Errorf("missing target object %s for %s", c.NewOID, c.RefName)
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, err.Error())
				return ingestedPack{}, err
			}
		}
	}

	idxBytes, err := pack.BuildIdx(parsed)
	if err != nil {
		syncRes.Handle.Release()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return ingestedPack{}, fmt.Errorf("build idx v2: %w", err)
	}

	sum := sha256.Sum256(completed)
	packSHA256 := hex.EncodeToString(sum[:])
	span.SetAttributes(telemetry.AttrPackID.String(packSHA256))

	return ingestedPack{
		packBytes:  completed,
		idxBytes:   idxBytes,
		packSHA256: packSHA256,
		reader:     pack.NewPackReader(parsed, baseResolver),
		handle:     syncRes.Handle,
	}, nil
}

type pushOverlapRequest struct {
	Repo          tenant.RepoKey
	Meta          ddb.RepoMeta
	Updates       []ddb.RefUpdate
	NeedsPack     bool
	PackSHA256    string
	CompletedPack []byte
	IdxBytes      []byte
	PackReader    *pack.PackReader
}

type pushOverlapResult struct {
	S3Err      error
	HookResult wasm.PipelineResult
	HookErr    error
}

// pushOverlapCoordinator implements the GoF Coordinator pattern for structured
// concurrent execution of S3 PutPack and Tier 1 synchronous Wasm hook evaluation.
type pushOverlapCoordinator struct {
	packs      PackPutter
	hookEngine HookEvaluator
	tracer     trace.Tracer
}

func (c *pushOverlapCoordinator) Execute(ctx context.Context, req pushOverlapRequest) pushOverlapResult {
	overlapCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := pushOverlapResult{
		HookResult: wasm.PipelineResult{Allowed: true},
	}

	var wg sync.WaitGroup

	// Goroutine A: Durability-before-visibility S3 PutObject (.pack + .idx).
	if req.NeedsPack {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s3Ctx, s3Span := c.tracer.Start(overlapCtx, "s3.put_pack", trace.WithAttributes(
				telemetry.AttrTenantID.String(string(req.Repo.TenantID)),
				telemetry.AttrRepoID.String(string(req.Repo.RepoID)),
				telemetry.AttrPackID.String(req.PackSHA256),
			))
			defer s3Span.End()

			err := c.packs.PutPack(s3Ctx, req.Repo, req.PackSHA256, req.CompletedPack, req.IdxBytes)
			if err != nil {
				s3Span.RecordError(err)
				s3Span.SetStatus(otelcodes.Error, err.Error())
				res.S3Err = err
				cancel()
			}
		}()
	}

	// Goroutine B: Tier 1 synchronous wazero Wasm hook evaluation.
	if c.hookEngine != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hookCtx, hookSpan := c.tracer.Start(overlapCtx, "hook.wasm.exec", trace.WithAttributes(
				telemetry.NewSpanAttrs().
					TenantID(string(req.Repo.TenantID)).
					RepoID(string(req.Repo.RepoID)).
					HomeRegion(string(req.Meta.HomeRegion)).
					FailoverEpoch(int64(req.Meta.FailoverEpoch)).
					Build()...,
			))
			defer hookSpan.End()

			pushCtx := buildWasmPushContext(req.Repo, req.Updates)
			var reader wasm.ObjectReader
			if req.PackReader != nil {
				reader = NewPackObjectReaderAdapter(req.PackReader)
			}

			pipeRes, err := c.hookEngine.EvaluatePush(hookCtx, pushCtx, reader)
			res.HookResult = pipeRes
			res.HookErr = err
			if err != nil {
				hookSpan.RecordError(err)
				hookSpan.SetStatus(otelcodes.Error, err.Error())
				cancel()
				return
			}
			if !pipeRes.Allowed {
				hookSpan.SetAttributes(telemetry.AttrHookID.String(pipeRes.RejectedBy))
				cancel()
			}
		}()
	}

	wg.Wait()
	return res
}

func (h *ReceivePackHandler) sendAllRejected(
	stream rpc.GitWireStream,
	reportStatus, useSideBand bool,
	unpackStatus string,
	cmds []ddb.RefUpdate,
	reason string,
	stderrLines []string,
) error {
	statuses := make([]refStatus, 0, len(cmds))
	for _, c := range cmds {
		statuses = append(statuses, refStatus{RefName: c.RefName, Err: reason})
	}
	return h.sendStatusAndFinish(stream, reportStatus, useSideBand, unpackStatus, statuses, stderrLines)
}

func (h *ReceivePackHandler) sendCASConflict(
	stream rpc.GitWireStream,
	reportStatus, useSideBand bool,
	cmds []ddb.RefUpdate,
	commitErr error,
	stderrLines []string,
) error {
	conflicted := make(map[string]bool)
	var refErr *ddb.RefConflictError
	if errors.As(commitErr, &refErr) {
		for _, name := range refErr.ConflictingRefs {
			conflicted[name] = true
		}
	}
	statuses := make([]refStatus, 0, len(cmds))
	for _, c := range cmds {
		if len(conflicted) == 0 || conflicted[c.RefName] {
			statuses = append(statuses, refStatus{RefName: c.RefName, Err: "ref CAS conflict"})
		} else {
			statuses = append(statuses, refStatus{RefName: c.RefName, Err: "atomic push failed"})
		}
	}
	return h.sendStatusAndFinish(stream, reportStatus, useSideBand, "unpack ok", statuses, stderrLines)
}

func (h *ReceivePackHandler) sendStatusAndFinish(
	stream rpc.GitWireStream,
	reportStatus, useSideBand bool,
	unpackStatus string,
	statuses []refStatus,
	stderrLines []string,
) error {
	if reportStatus {
		payload := EncodeReportStatus(unpackStatus, statuses, useSideBand, stderrLines)
		w := &streamChunkWriter{stream: stream}
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
}

func findStaleRefUpdates(cmds []ddb.RefUpdate, currentRefs []ddb.RefState) map[string]bool {
	current := make(map[string]string, len(currentRefs))
	for _, r := range currentRefs {
		current[r.RefName] = r.OID
	}

	stale := make(map[string]bool)
	for _, c := range cmds {
		curOID, exists := current[c.RefName]
		if c.IsCreate() {
			if exists {
				stale[c.RefName] = true
			}
			continue
		}
		if !exists || curOID != c.OldOID {
			stale[c.RefName] = true
		}
	}
	return stale
}

func pushNeedsPack(cmds []ddb.RefUpdate) bool {
	for _, c := range cmds {
		if !c.IsDelete() {
			return true
		}
	}
	return false
}

func buildWasmPushContext(repo tenant.RepoKey, cmds []ddb.RefUpdate) wasm.PushContext {
	updates := make([]wasm.RefUpdate, 0, len(cmds))
	for _, c := range cmds {
		oldOID, _ := pack.ParseOID(c.OldOID)
		newOID, _ := pack.ParseOID(c.NewOID)
		updates = append(updates, wasm.RefUpdate{
			Ref:    c.RefName,
			OldOID: [20]byte(oldOID),
			NewOID: [20]byte(newOID),
		})
	}
	return wasm.PushContext{
		TenantID: string(repo.TenantID),
		RepoID:   string(repo.RepoID),
		Updates:  updates,
	}
}

func buildHookRejectStatuses(cmds []ddb.RefUpdate, res wasm.PipelineResult, fallbackMsg string) []refStatus {
	perRef := make(map[string]wasm.RefVerdict, len(res.RefVerdicts))
	for _, rv := range res.RefVerdicts {
		perRef[rv.Ref] = rv
	}
	statuses := make([]refStatus, 0, len(cmds))
	for _, c := range cmds {
		if rv, ok := perRef[c.RefName]; ok && rv.Decision == wasm.VerdictReject {
			msg := strings.TrimSpace(rv.Message)
			if msg == "" {
				msg = fallbackMsg
			}
			statuses = append(statuses, refStatus{RefName: c.RefName, Err: msg})
			continue
		}
		statuses = append(statuses, refStatus{RefName: c.RefName, Err: fallbackMsg})
	}
	return statuses
}

func formatHookWarnings(warnings []string) []string {
	if len(warnings) == 0 {
		return nil
	}
	lines := make([]string, 0, len(warnings))
	for _, w := range warnings {
		w = strings.TrimSpace(w)
		if w != "" {
			lines = append(lines, "remote: warning: "+w+"\n")
		}
	}
	return lines
}

// streamFrameReader adapts rpc.GitWireStream.Recv into an io.Reader.
type streamFrameReader struct {
	stream rpc.GitWireStream
	buf    []byte
	eof    bool
}

func (r *streamFrameReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.eof {
			return 0, io.EOF
		}
		frame, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.eof = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		if len(frame.Data) > 0 {
			r.buf = frame.Data
		}
		if frame.EOF {
			r.eof = true
			if len(r.buf) == 0 {
				return 0, io.EOF
			}
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
