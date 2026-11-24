package ctrl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultLeaseTTL is the default regional write-lease TTL (5s per ha-consistency.md).
	DefaultLeaseTTL = 5 * time.Second
	// DefaultHeartbeatInterval is the default lease renewal cadence (2s).
	DefaultHeartbeatInterval = 2 * time.Second
)

var (
	// ErrInvalidConfig is returned when controller configuration fails validation.
	ErrInvalidConfig = errors.New("ctrl: invalid configuration")
	// ErrInvalidTransition is returned when a requested write_state transition is disallowed.
	ErrInvalidTransition = errors.New("ctrl: invalid write_state transition")
	// ErrNonMonotonicEpoch is returned when an epoch update is not strictly greater than the current epoch.
	ErrNonMonotonicEpoch = errors.New("ctrl: failover_epoch must advance monotonically")
)

// RegionalLeaseStore is the narrow DynamoDB seam for managing regional write leases.
// *ddb.Repository satisfies this interface directly.
type RegionalLeaseStore interface {
	GetRegionalWriteLease(ctx context.Context, region lease.Region) (ddb.RegionalWriteLease, error)
	AcquireRegionalWriteLease(ctx context.Context, region lease.Region, holderID string, epoch lease.Epoch, mode lease.Mode, ttl time.Duration) (ddb.RegionalWriteLease, error)
	PutRegionalWriteLease(ctx context.Context, l ddb.RegionalWriteLease) error
}

// RepoMetaStore is the narrow DynamoDB seam for reading and updating repo META records.
// *ddb.Repository satisfies this interface directly.
type RepoMetaStore interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error)
	UpdateMeta(ctx context.Context, meta ddb.RepoMeta, expectedEpoch lease.Epoch) error
}

// LeaseState implements the GoF State pattern for the regional write-lease lifecycle.
type LeaseState interface {
	Mode() lease.Mode
	AllowsWrites() bool
	CanTransitionTo(target lease.Mode) bool
}

type activeState struct{}

func (activeState) Mode() lease.Mode   { return lease.ModeActive }
func (activeState) AllowsWrites() bool { return true }
func (activeState) CanTransitionTo(target lease.Mode) bool {
	return target == lease.ModeActive || target == lease.ModeDraining || target == lease.ModeReadOnly
}

type drainingState struct{}

func (drainingState) Mode() lease.Mode   { return lease.ModeDraining }
func (drainingState) AllowsWrites() bool { return false }
func (drainingState) CanTransitionTo(target lease.Mode) bool {
	return target == lease.ModeDraining || target == lease.ModeReadOnly || target == lease.ModeActive
}

type readOnlyState struct{}

func (readOnlyState) Mode() lease.Mode   { return lease.ModeReadOnly }
func (readOnlyState) AllowsWrites() bool { return false }
func (readOnlyState) CanTransitionTo(target lease.Mode) bool {
	return target == lease.ModeReadOnly || target == lease.ModeActive || target == lease.ModeDraining
}

func stateForMode(mode lease.Mode) (LeaseState, error) {
	switch mode {
	case lease.ModeActive:
		return activeState{}, nil
	case lease.ModeDraining:
		return drainingState{}, nil
	case lease.ModeReadOnly:
		return readOnlyState{}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported mode %q", ErrInvalidTransition, mode)
	}
}

// LeaseManagerConfig configures the single-region write-lease heartbeat and fencing manager.
type LeaseManagerConfig struct {
	Leases            RegionalLeaseStore
	Meta              RepoMetaStore
	Region            lease.Region
	HolderID          string
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	FenceWait         time.Duration
	InitialMode       lease.Mode
	Clock             lease.Clock
	ULIDGen           func(time.Time) (string, error)
	PromotionGuard    PromotionGuard
	PromotionAudit    PromotionAuditStore
	Metrics           *Metrics
	Logger            *slog.Logger
}

// FenceRequest describes a regional lease fencing and/or epoch advancement operation.
type FenceRequest struct {
	TargetMode    lease.Mode      `json:"mode,omitempty"`
	AdvanceEpoch  bool            `json:"advance_epoch"`
	MinEpoch      lease.Epoch     `json:"min_epoch,omitempty"`
	FenceWait     time.Duration   `json:"-"`
	Repo          *tenant.RepoKey `json:"-"`
	HomeRegion    lease.Region    `json:"home_region,omitempty"`
	OperatorID    string          `json:"operator_id,omitempty"`
	ApprovalToken string          `json:"-"`
	Reason        string          `json:"reason,omitempty"`
}

// FenceResult reports the outcome of a fencing or epoch advancement transition.
type FenceResult struct {
	Region            lease.Region       `json:"region"`
	PreviousEpoch     lease.Epoch        `json:"previous_epoch"`
	CurrentEpoch      lease.Epoch        `json:"current_epoch"`
	PreviousMode      lease.Mode         `json:"previous_mode"`
	CurrentMode       lease.Mode         `json:"current_mode"`
	HolderID          string             `json:"holder_id"`
	ExpiresAt         time.Time          `json:"expires_at"`
	FenceWaitApplied  time.Duration      `json:"fence_wait_applied_ns"`
	RepoMeta          *ddb.RepoMeta      `json:"repo_meta,omitempty"`
	PromotionDecision *PromotionDecision `json:"promotion_decision,omitempty"`
}

// LeaseStatus is a point-in-time snapshot of the regional write lease.
type LeaseStatus struct {
	Region       lease.Region `json:"region"`
	Epoch        lease.Epoch  `json:"epoch"`
	Mode         lease.Mode   `json:"mode"`
	HolderID     string       `json:"holder_id"`
	ExpiresAt    time.Time    `json:"expires_at"`
	Valid        bool         `json:"valid"`
	AllowsWrites bool         `json:"allows_writes"`
	Initialized  bool         `json:"initialized"`
}

// LeaseManager continuously renews the regional write lease in DynamoDB and
// coordinates monotonic failover_epoch fencing transitions.
type LeaseManager struct {
	leases            RegionalLeaseStore
	meta              RepoMetaStore
	region            lease.Region
	holderID          string
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	fenceWait         time.Duration
	clock             lease.Clock
	ulidGen           func(time.Time) (string, error)
	promotionGuard    PromotionGuard
	promotionAudit    PromotionAuditStore
	metrics           *Metrics
	logger            *slog.Logger

	mu          sync.RWMutex
	initialized bool
	state       LeaseState
	current     ddb.RegionalWriteLease
}

// NewLeaseManager constructs a validated LeaseManager.
func NewLeaseManager(cfg LeaseManagerConfig) (*LeaseManager, error) {
	if cfg.Leases == nil {
		return nil, fmt.Errorf("%w: leases store is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(string(cfg.Region)) == "" {
		return nil, lease.ErrEmptyRegion
	}
	if strings.TrimSpace(cfg.HolderID) == "" {
		return nil, fmt.Errorf("%w: holder_id is required", ErrInvalidConfig)
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	heartbeat := cfg.HeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = DefaultHeartbeatInterval
	}
	if heartbeat >= leaseTTL {
		return nil, fmt.Errorf("%w: heartbeat_interval (%s) must be less than lease_ttl (%s)", ErrInvalidConfig, heartbeat, leaseTTL)
	}
	fenceWait := cfg.FenceWait
	if fenceWait < 0 {
		fenceWait = 0
	}
	initialMode := cfg.InitialMode
	if initialMode == "" {
		initialMode = lease.ModeActive
	}
	st, err := stateForMode(initialMode)
	if err != nil {
		return nil, err
	}
	clk := cfg.Clock
	if clk == nil {
		clk = lease.RealClock{}
	}
	ulidGen := cfg.ULIDGen
	if ulidGen == nil {
		ulidGen = ddb.NewULID
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &LeaseManager{
		leases:            cfg.Leases,
		meta:              cfg.Meta,
		region:            cfg.Region,
		holderID:          cfg.HolderID,
		leaseTTL:          leaseTTL,
		heartbeatInterval: heartbeat,
		fenceWait:         fenceWait,
		clock:             clk,
		ulidGen:           ulidGen,
		promotionGuard:    cfg.PromotionGuard,
		promotionAudit:    cfg.PromotionAudit,
		metrics:           cfg.Metrics,
		logger:            logger,
		state:             st,
		current: ddb.RegionalWriteLease{
			Region:   cfg.Region,
			Epoch:    1,
			Mode:     initialMode,
			HolderID: cfg.HolderID,
		},
	}, nil
}

// Region returns the managed AWS region.
func (m *LeaseManager) Region() lease.Region {
	return m.region
}

// State returns the current GoF LeaseState.
func (m *LeaseManager) State() LeaseState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// Status returns a thread-safe snapshot of the regional write lease.
func (m *LeaseManager) Status() LeaseStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := m.clock.Now().UTC()
	valid := m.initialized && m.current.Epoch > 0 && now.Before(m.current.ExpiresAt)
	allowsWrites := valid && m.state.AllowsWrites()
	return LeaseStatus{
		Region:       m.region,
		Epoch:        m.current.Epoch,
		Mode:         m.state.Mode(),
		HolderID:     m.current.HolderID,
		ExpiresAt:    m.current.ExpiresAt,
		Valid:        valid,
		AllowsWrites: allowsWrites,
		Initialized:  m.initialized,
	}
}

// Run starts the heartbeat renewal loop until ctx is cancelled.
func (m *LeaseManager) Run(ctx context.Context) error {
	if _, err := m.RenewOnce(ctx); err != nil && ctx.Err() == nil {
		m.logger.Warn("ctrl: initial regional write lease renewal failed",
			"region", string(m.region),
			"error", err,
		)
	}

	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := m.RenewOnce(ctx); err != nil && ctx.Err() == nil {
				m.logger.Warn("ctrl: regional write lease heartbeat failed",
					"region", string(m.region),
					"error", err,
				)
			}
		}
	}
}

// RenewOnce acquires or renews the regional write lease in DynamoDB while
// preserving monotonic failover_epoch ordering.
func (m *LeaseManager) RenewOnce(ctx context.Context) (ddb.RegionalWriteLease, error) {
	m.mu.Lock()
	initialized := m.initialized
	epoch := m.current.Epoch
	mode := m.state.Mode()
	m.mu.Unlock()

	if !initialized {
		existing, err := m.leases.GetRegionalWriteLease(ctx, m.region)
		if err == nil {
			if existing.Epoch > epoch {
				epoch = existing.Epoch
			}
			if existing.Mode != "" {
				if _, stErr := stateForMode(existing.Mode); stErr == nil {
					mode = existing.Mode
				}
			}
		} else if !errors.Is(err, lease.ErrLeaseNotFound) {
			m.metrics.recordLeaseRenewal(ctx, m.region, "error")
			return ddb.RegionalWriteLease{}, fmt.Errorf("ctrl: read regional lease: %w", err)
		}
	}

	if epoch == 0 {
		epoch = 1
	}

	acquired, err := m.leases.AcquireRegionalWriteLease(ctx, m.region, m.holderID, epoch, mode, m.leaseTTL)
	if err != nil {
		m.metrics.recordLeaseRenewal(ctx, m.region, "error")
		return ddb.RegionalWriteLease{}, err
	}

	st, err := stateForMode(acquired.Mode)
	if err != nil {
		m.metrics.recordLeaseRenewal(ctx, m.region, "error")
		return ddb.RegionalWriteLease{}, err
	}

	m.mu.Lock()
	if acquired.Epoch >= m.current.Epoch {
		m.current = acquired
		m.state = st
		m.initialized = true
	}
	result := m.current
	m.mu.Unlock()

	m.metrics.recordLeaseRenewal(ctx, m.region, "ok")
	return result, nil
}

// FenceRegion advances the regional failover_epoch monotonically and/or switches
// the regional write_state with optional lease-fence wait semantics. If req.Repo
// is set, the repository's META item is also updated to the new epoch and mode.
func (m *LeaseManager) FenceRegion(ctx context.Context, req FenceRequest) (FenceResult, error) {
	m.mu.Lock()
	prevEpoch := m.current.Epoch
	prevState := m.state
	initialized := m.initialized
	m.mu.Unlock()

	if !initialized {
		existing, err := m.leases.GetRegionalWriteLease(ctx, m.region)
		if err == nil {
			if existing.Epoch > prevEpoch {
				prevEpoch = existing.Epoch
			}
			if st, stErr := stateForMode(existing.Mode); stErr == nil {
				prevState = st
			}
		} else if !errors.Is(err, lease.ErrLeaseNotFound) {
			return FenceResult{}, fmt.Errorf("ctrl: read regional lease before fence: %w", err)
		}
	}

	targetMode := req.TargetMode
	if targetMode == "" {
		targetMode = prevState.Mode()
	}
	nextState, err := stateForMode(targetMode)
	if err != nil {
		return FenceResult{}, err
	}
	if !prevState.CanTransitionTo(targetMode) {
		return FenceResult{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, prevState.Mode(), targetMode)
	}

	newEpoch := prevEpoch
	if req.AdvanceEpoch || req.MinEpoch > 0 {
		newEpoch = prevEpoch + 1
		if req.MinEpoch > 0 {
			if req.MinEpoch <= prevEpoch {
				return FenceResult{}, fmt.Errorf("%w: requested=%d current=%d", ErrNonMonotonicEpoch, req.MinEpoch, prevEpoch)
			}
			if req.MinEpoch > newEpoch {
				newEpoch = req.MinEpoch
			}
		}
	}
	if newEpoch == 0 {
		newEpoch = 1
	}

	spanAttrs := telemetry.NewSpanAttrs().
		HomeRegion(string(m.region)).
		FailoverEpoch(epochToInt64(newEpoch))
	if req.Repo != nil {
		spanAttrs.TenantID(string(req.Repo.TenantID)).RepoID(string(req.Repo.RepoID))
	}
	attrs := append(spanAttrs.Build(),
		attribute.String("previous_mode", string(prevState.Mode())),
		attribute.String("target_mode", string(targetMode)),
	)

	ctx, span := m.metrics.Tracer().Start(ctx, "ctrl.failover.transition", trace.WithAttributes(attrs...))
	defer span.End()

	var promotionDecision *PromotionDecision
	isPromotion := newEpoch > prevEpoch || (targetMode == lease.ModeActive && prevState.Mode() != lease.ModeActive)
	if m.promotionGuard != nil && isPromotion {
		dec, err := m.evaluatePromotionGuard(ctx, req, prevEpoch, newEpoch, targetMode)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			m.metrics.recordEpochTransition(ctx, m.region, targetMode, "denied")
			return FenceResult{}, err
		}
		promotionDecision = &dec
	}

	waitDur := req.FenceWait
	if waitDur <= 0 {
		waitDur = m.fenceWait
	}
	var appliedWait time.Duration
	if waitDur > 0 && (newEpoch > prevEpoch || targetMode != prevState.Mode()) {
		if err := waitWithContext(ctx, waitDur); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			m.metrics.recordEpochTransition(ctx, m.region, targetMode, "cancelled")
			return FenceResult{}, err
		}
		appliedWait = waitDur
	}

	acquired, err := m.leases.AcquireRegionalWriteLease(ctx, m.region, m.holderID, newEpoch, targetMode, m.leaseTTL)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		m.metrics.recordEpochTransition(ctx, m.region, targetMode, "error")
		return FenceResult{}, err
	}

	m.mu.Lock()
	m.current = acquired
	m.state = nextState
	m.initialized = true
	m.mu.Unlock()

	res := FenceResult{
		Region:            m.region,
		PreviousEpoch:     prevEpoch,
		CurrentEpoch:      acquired.Epoch,
		PreviousMode:      prevState.Mode(),
		CurrentMode:       acquired.Mode,
		HolderID:          acquired.HolderID,
		ExpiresAt:         acquired.ExpiresAt,
		FenceWaitApplied:  appliedWait,
		PromotionDecision: promotionDecision,
	}

	if req.Repo != nil {
		meta, err := m.UpdateRepoEpoch(ctx, *req.Repo, newEpoch, targetMode)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			m.metrics.recordEpochTransition(ctx, m.region, targetMode, "error")
			return FenceResult{}, err
		}
		res.RepoMeta = &meta
	}

	m.metrics.recordEpochTransition(ctx, m.region, targetMode, "ok")
	return res, nil
}

// UpdateRepoEpoch monotonically advances failover_epoch and updates write_state
// on a repository's DynamoDB META item using conditional epoch CAS.
// If newEpoch is 0, the repository's current failover_epoch is incremented by 1.
func (m *LeaseManager) UpdateRepoEpoch(ctx context.Context, repo tenant.RepoKey, newEpoch lease.Epoch, writeState lease.Mode) (ddb.RepoMeta, error) {
	if err := repo.Validate(); err != nil {
		return ddb.RepoMeta{}, err
	}
	if m.meta == nil {
		return ddb.RepoMeta{}, fmt.Errorf("%w: repo meta store is not configured", ErrInvalidConfig)
	}

	current, err := m.meta.GetMeta(ctx, repo)
	if err != nil {
		return ddb.RepoMeta{}, err
	}

	targetEpoch := newEpoch
	if targetEpoch == 0 {
		targetEpoch = current.FailoverEpoch + 1
	}
	if targetEpoch <= current.FailoverEpoch {
		return ddb.RepoMeta{}, fmt.Errorf("%w: target=%d current=%d", ErrNonMonotonicEpoch, targetEpoch, current.FailoverEpoch)
	}

	targetMode := writeState
	if targetMode == "" {
		targetMode = current.WriteState
	}
	if _, err := stateForMode(targetMode); err != nil {
		return ddb.RepoMeta{}, err
	}

	ctx, span := m.metrics.Tracer().Start(ctx, "ctrl.failover.transition", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repo.TenantID)).
			RepoID(string(repo.RepoID)).
			HomeRegion(string(current.HomeRegion)).
			FailoverEpoch(epochToInt64(targetEpoch)).
			Build()...,
	))
	defer span.End()

	updated := current
	updated.FailoverEpoch = targetEpoch
	updated.WriteState = targetMode

	if err := m.meta.UpdateMeta(ctx, updated, current.FailoverEpoch); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		m.metrics.recordEpochTransition(ctx, current.HomeRegion, targetMode, "error")
		return ddb.RepoMeta{}, err
	}

	m.metrics.recordEpochTransition(ctx, current.HomeRegion, targetMode, "ok")
	return updated, nil
}

func (m *LeaseManager) evaluatePromotionGuard(ctx context.Context, req FenceRequest, prevEpoch, targetEpoch lease.Epoch, targetMode lease.Mode) (PromotionDecision, error) {
	homeRegion := req.HomeRegion
	if homeRegion == "" && req.Repo != nil && m.meta != nil {
		if meta, err := m.meta.GetMeta(ctx, *req.Repo); err == nil && meta.HomeRegion != "" {
			homeRegion = meta.HomeRegion
		}
	}
	if homeRegion == "" {
		homeRegion = m.region
	}

	promReq := PromotionRequest{
		Region:        m.region,
		HomeRegion:    homeRegion,
		CurrentEpoch:  prevEpoch,
		TargetEpoch:   targetEpoch,
		TargetMode:    targetMode,
		Repo:          req.Repo,
		OperatorID:    req.OperatorID,
		ApprovalToken: req.ApprovalToken,
		Reason:        req.Reason,
	}

	dec, guardErr := m.promotionGuard.AuthorizePromotion(ctx, promReq)
	allowed := guardErr == nil && dec.Allowed
	outcome := "denied"
	if allowed {
		outcome = "allowed"
	}
	m.metrics.recordPromotionGuard(ctx, m.region, outcome)

	if m.promotionAudit != nil {
		now := m.clock.Now().UTC()
		auditID, idErr := m.ulidGen(now)
		if idErr != nil {
			auditID = fmt.Sprintf("prom-%d", now.UnixNano())
		}
		record := PromotionAuditRecord{
			AuditID:      auditID,
			Region:       m.region,
			HomeRegion:   homeRegion,
			Repo:         req.Repo,
			CurrentEpoch: prevEpoch,
			TargetEpoch:  targetEpoch,
			TargetMode:   targetMode,
			Decision:     dec,
			Reason:       req.Reason,
			EvaluatedAt:  now,
		}
		if auditErr := m.promotionAudit.RecordPromotionAudit(ctx, record); auditErr != nil {
			m.logger.Warn("ctrl: failed to persist promotion guard audit record",
				"region", string(m.region),
				"outcome", outcome,
				"error", auditErr,
			)
			if allowed {
				return dec, fmt.Errorf("ctrl: persist promotion audit: %w", auditErr)
			}
		}
	}

	if !allowed {
		if guardErr == nil {
			guardErr = fmt.Errorf("%w: %s", ErrPromotionDenied, dec.Reason)
		}
		return dec, guardErr
	}
	return dec, nil
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
