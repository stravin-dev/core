package ctrl

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultWitnessTimeout is the per-request HTTP timeout when querying witness endpoints.
	DefaultWitnessTimeout = 3 * time.Second
	maxWitnessBodyBytes   = 64 << 10 // 64 KiB
)

var (
	// ErrPromotionDenied is returned when a failover promotion fails guard authorization.
	ErrPromotionDenied = errors.New("ctrl: failover promotion denied")
	// ErrPrimaryHealthy is returned when one or more witnesses report the home_region primary is still healthy.
	ErrPrimaryHealthy = errors.New("ctrl: primary home_region reported healthy by witness")
)

// CompositePolicy determines how CompositePromotionGuard combines child guards.
type CompositePolicy string

const (
	// CompositePolicyRequireAll requires every configured guard to authorize the promotion (AND).
	CompositePolicyRequireAll CompositePolicy = "require_all"
	// CompositePolicyWitnessOrOperator allows promotion if any configured guard authorizes it (OR).
	CompositePolicyWitnessOrOperator CompositePolicy = "witness_or_operator"
)

// PromotionRequest carries context for authorizing a failover epoch advance or region promotion.
type PromotionRequest struct {
	Region        lease.Region    `json:"region"`
	HomeRegion    lease.Region    `json:"home_region,omitempty"`
	CurrentEpoch  lease.Epoch     `json:"current_epoch"`
	TargetEpoch   lease.Epoch     `json:"target_epoch"`
	TargetMode    lease.Mode      `json:"target_mode,omitempty"`
	Repo          *tenant.RepoKey `json:"repo,omitempty"`
	OperatorID    string          `json:"operator_id,omitempty"`
	ApprovalToken string          `json:"-"` // never serialize secrets into audit logs or JSON
	Reason        string          `json:"reason,omitempty"`
}

// WitnessVote captures a single witness endpoint's observation of home_region health/fencing.
type WitnessVote struct {
	Endpoint      string `json:"endpoint"`
	PrimaryFenced bool   `json:"primary_fenced"`
	PrimaryState  string `json:"primary_state,omitempty"`
	Error         string `json:"error,omitempty"`
}

// PromotionDecision captures the auditable outcome of a PromotionGuard evaluation.
type PromotionDecision struct {
	Allowed       bool          `json:"allowed"`
	GuardType     string        `json:"guard_type"`
	Reason        string        `json:"reason"`
	OperatorID    string        `json:"operator_id,omitempty"`
	WitnessVotes  []WitnessVote `json:"witness_votes,omitempty"`
	QuorumNeeded  int           `json:"quorum_needed,omitempty"`
	QuorumReached int           `json:"quorum_reached,omitempty"`
}

// PromotionGuard implements the GoF Strategy pattern for authorizing failover promotions.
type PromotionGuard interface {
	AuthorizePromotion(ctx context.Context, req PromotionRequest) (PromotionDecision, error)
}

// OperatorPromotionGuard requires an explicit operator identity and constant-time
// matching approval token before failover promotion proceeds. It fails closed when
// no expected approval token is configured.
type OperatorPromotionGuard struct {
	approvalToken string
}

var _ PromotionGuard = (*OperatorPromotionGuard)(nil)

// NewOperatorPromotionGuard constructs an OperatorPromotionGuard.
func NewOperatorPromotionGuard(approvalToken string) *OperatorPromotionGuard {
	return &OperatorPromotionGuard{
		approvalToken: strings.TrimSpace(approvalToken),
	}
}

// AuthorizePromotion verifies operator identity and constant-time approval token match.
func (g *OperatorPromotionGuard) AuthorizePromotion(_ context.Context, req PromotionRequest) (PromotionDecision, error) {
	opID := strings.TrimSpace(req.OperatorID)
	dec := PromotionDecision{
		GuardType:  "operator",
		OperatorID: opID,
	}
	if g.approvalToken == "" {
		dec.Reason = "operator promotion approval token is not configured"
		return dec, fmt.Errorf("%w: %s", ErrPromotionDenied, dec.Reason)
	}
	if opID == "" {
		dec.Reason = "operator_id is required for promotion approval"
		return dec, fmt.Errorf("%w: %s", ErrPromotionDenied, dec.Reason)
	}
	provided := strings.TrimSpace(req.ApprovalToken)
	if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(g.approvalToken)) != 1 {
		dec.Reason = "invalid or missing operator approval token"
		return dec, fmt.Errorf("%w: %s", ErrPromotionDenied, dec.Reason)
	}
	dec.Allowed = true
	dec.Reason = "operator approval verified"
	return dec, nil
}

// WitnessQuorumConfig configures WitnessQuorumGuard.
type WitnessQuorumConfig struct {
	Endpoints  []string
	Quorum     int
	Timeout    time.Duration
	HTTPClient *http.Client
}

// WitnessQuorumGuard queries independent witness endpoints to confirm that
// home_region is fenced or unreachable before allowing failover promotion.
type WitnessQuorumGuard struct {
	endpoints  []string
	quorum     int
	timeout    time.Duration
	httpClient *http.Client
}

var _ PromotionGuard = (*WitnessQuorumGuard)(nil)

// NewWitnessQuorumGuard constructs a validated WitnessQuorumGuard.
func NewWitnessQuorumGuard(cfg WitnessQuorumConfig) (*WitnessQuorumGuard, error) {
	endpoints := make([]string, 0, len(cfg.Endpoints))
	for _, raw := range cfg.Endpoints {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		parsed, err := url.Parse(trimmed)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return nil, fmt.Errorf("%w: invalid witness endpoint %q", ErrInvalidConfig, trimmed)
		}
		endpoints = append(endpoints, trimmed)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: at least one witness endpoint is required", ErrInvalidConfig)
	}

	quorum := cfg.Quorum
	if quorum <= 0 {
		quorum = (len(endpoints) / 2) + 1
	}
	if quorum > len(endpoints) {
		return nil, fmt.Errorf("%w: witness quorum %d exceeds endpoint count %d", ErrInvalidConfig, quorum, len(endpoints))
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultWitnessTimeout
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy:                 nil,
				DialContext:           (&net.Dialer{Timeout: timeout}).DialContext,
				TLSHandshakeTimeout:   timeout,
				ResponseHeaderTimeout: timeout,
			},
		}
	}

	return &WitnessQuorumGuard{
		endpoints:  endpoints,
		quorum:     quorum,
		timeout:    timeout,
		httpClient: client,
	}, nil
}

type witnessResponsePayload struct {
	PrimaryFenced  bool   `json:"primary_fenced"`
	PrimaryHealthy *bool  `json:"primary_healthy,omitempty"`
	State          string `json:"state,omitempty"`
}

// AuthorizePromotion queries all configured witness endpoints concurrently and
// fails closed unless at least quorum endpoints confirm home_region is fenced.
func (g *WitnessQuorumGuard) AuthorizePromotion(ctx context.Context, req PromotionRequest) (PromotionDecision, error) {
	homeRegion := req.HomeRegion
	if homeRegion == "" {
		homeRegion = req.Region
	}

	votes := make([]WitnessVote, len(g.endpoints))
	var wg sync.WaitGroup
	for i, ep := range g.endpoints {
		wg.Add(1)
		go func(idx int, endpoint string) {
			defer wg.Done()
			votes[idx] = g.queryWitness(ctx, endpoint, homeRegion, req.Region, req.CurrentEpoch)
		}(i, ep)
	}
	wg.Wait()

	fencedVotes := 0
	healthyVotes := 0
	for _, v := range votes {
		if v.PrimaryFenced {
			fencedVotes++
		}
		if v.PrimaryState == "healthy" {
			healthyVotes++
		}
	}

	dec := PromotionDecision{
		GuardType:     "witness_quorum",
		WitnessVotes:  votes,
		QuorumNeeded:  g.quorum,
		QuorumReached: fencedVotes,
	}

	if fencedVotes >= g.quorum {
		dec.Allowed = true
		dec.Reason = fmt.Sprintf("witness quorum reached: %d/%d confirm primary fenced", fencedVotes, g.quorum)
		return dec, nil
	}

	if healthyVotes > 0 {
		dec.Reason = fmt.Sprintf("primary reported healthy by %d witness(es); fenced=%d/%d required", healthyVotes, fencedVotes, g.quorum)
		return dec, fmt.Errorf("%w: %s", ErrPrimaryHealthy, dec.Reason)
	}

	dec.Reason = fmt.Sprintf("witness quorum not reached: %d/%d confirm primary fenced", fencedVotes, g.quorum)
	return dec, fmt.Errorf("%w: %s", ErrPromotionDenied, dec.Reason)
}

func (g *WitnessQuorumGuard) queryWitness(ctx context.Context, endpoint string, homeRegion, targetRegion lease.Region, currentEpoch lease.Epoch) WitnessVote {
	vote := WitnessVote{
		Endpoint:     endpoint,
		PrimaryState: "unreachable",
	}

	reqCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	u, err := url.Parse(endpoint)
	if err != nil {
		vote.PrimaryState = "error"
		vote.Error = err.Error()
		return vote
	}
	q := u.Query()
	q.Set("region", string(homeRegion))
	q.Set("target_region", string(targetRegion))
	q.Set("epoch", strconv.FormatUint(uint64(currentEpoch), 10))
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		vote.PrimaryState = "error"
		vote.Error = err.Error()
		return vote
	}
	httpReq.Header.Set("Accept", "application/json")

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		vote.Error = err.Error()
		return vote
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		vote.PrimaryState = "error"
		vote.Error = fmt.Sprintf("unexpected status %d", resp.StatusCode)
		return vote
	}

	var payload witnessResponsePayload
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxWitnessBodyBytes)).Decode(&payload); err != nil {
		vote.PrimaryState = "error"
		vote.Error = fmt.Sprintf("invalid json: %v", err)
		return vote
	}

	reportedHealthy := (payload.PrimaryHealthy != nil && *payload.PrimaryHealthy) || strings.EqualFold(strings.TrimSpace(payload.State), "healthy")
	if reportedHealthy {
		vote.PrimaryFenced = false
		vote.PrimaryState = "healthy"
		return vote
	}

	if payload.PrimaryFenced {
		vote.PrimaryFenced = true
		vote.PrimaryState = "fenced"
		return vote
	}

	state := strings.TrimSpace(payload.State)
	if state == "" {
		state = "unknown"
	}
	vote.PrimaryState = state
	return vote
}

// CompositePromotionGuard implements the GoF Composite pattern to combine
// multiple PromotionGuard strategies under an AND (require_all) or OR
// (witness_or_operator) policy.
type CompositePromotionGuard struct {
	policy CompositePolicy
	guards []PromotionGuard
}

var _ PromotionGuard = (*CompositePromotionGuard)(nil)

// NewCompositePromotionGuard constructs a validated CompositePromotionGuard.
func NewCompositePromotionGuard(policy CompositePolicy, guards ...PromotionGuard) (*CompositePromotionGuard, error) {
	if policy == "" {
		policy = CompositePolicyRequireAll
	}
	if policy != CompositePolicyRequireAll && policy != CompositePolicyWitnessOrOperator {
		return nil, fmt.Errorf("%w: unsupported promotion guard policy %q", ErrInvalidConfig, policy)
	}
	filtered := make([]PromotionGuard, 0, len(guards))
	for _, g := range guards {
		if g != nil {
			filtered = append(filtered, g)
		}
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("%w: composite promotion guard requires at least one guard", ErrInvalidConfig)
	}
	return &CompositePromotionGuard{
		policy: policy,
		guards: filtered,
	}, nil
}

// AuthorizePromotion evaluates child guards according to the configured policy.
func (g *CompositePromotionGuard) AuthorizePromotion(ctx context.Context, req PromotionRequest) (PromotionDecision, error) {
	combined := PromotionDecision{
		GuardType: fmt.Sprintf("composite:%s", g.policy),
	}

	var (
		reasons        []string
		firstErr       error
		primaryHealthy error
		anyAllowed     bool
		allAllowed     = true
		allowedReasons []string
		deniedReasons  []string
	)

	for _, child := range g.guards {
		dec, err := child.AuthorizePromotion(ctx, req)
		if dec.OperatorID != "" {
			combined.OperatorID = dec.OperatorID
		}
		if len(dec.WitnessVotes) > 0 {
			combined.WitnessVotes = append(combined.WitnessVotes, dec.WitnessVotes...)
		}
		if dec.QuorumNeeded > 0 {
			combined.QuorumNeeded = dec.QuorumNeeded
			combined.QuorumReached = dec.QuorumReached
		}

		if err == nil && dec.Allowed {
			anyAllowed = true
			if dec.Reason != "" {
				allowedReasons = append(allowedReasons, dec.Reason)
			}
		} else {
			allAllowed = false
			if dec.Reason != "" {
				deniedReasons = append(deniedReasons, dec.Reason)
			}
			if errors.Is(err, ErrPrimaryHealthy) && primaryHealthy == nil {
				primaryHealthy = err
			}
			if firstErr == nil {
				if err != nil {
					firstErr = err
				} else {
					firstErr = ErrPromotionDenied
				}
			}
		}
	}

	switch g.policy {
	case CompositePolicyRequireAll:
		if allAllowed {
			combined.Allowed = true
			combined.Reason = strings.Join(allowedReasons, "; ")
			return combined, nil
		}
		reasons = deniedReasons
		combined.Reason = strings.Join(reasons, "; ")
		if primaryHealthy != nil {
			return combined, primaryHealthy
		}
		return combined, firstErr

	case CompositePolicyWitnessOrOperator:
		if anyAllowed {
			combined.Allowed = true
			combined.Reason = strings.Join(allowedReasons, "; ")
			return combined, nil
		}
		combined.Reason = strings.Join(deniedReasons, "; ")
		if primaryHealthy != nil {
			return combined, primaryHealthy
		}
		return combined, firstErr

	default:
		combined.Reason = fmt.Sprintf("unsupported policy %q", g.policy)
		return combined, fmt.Errorf("%w: %s", ErrPromotionDenied, combined.Reason)
	}
}
