package ctrl

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

const (
	defaultServiceName = "stravin-ctrl"
	maxAdminBodyBytes  = 1 << 20 // 1 MiB
)

// HandlerConfig wires the stravin-ctrl HTTP admin API.
type HandlerConfig struct {
	ServiceName string
	Leases      *LeaseManager
	Scrubber    *Scrubber
	AdminToken  string
}

// Handler serves stravin-ctrl's localhost/internal HTTP control and admin endpoints:
//   - GET  /healthz
//   - GET  /v1/lease
//   - GET  /v1/lease/status
//   - POST /v1/lease/fence
//   - POST /v1/epoch/advance
//   - POST /v1/scrub
type Handler struct {
	serviceName string
	leases      *LeaseManager
	scrubber    *Scrubber
	adminToken  string
	mux         *http.ServeMux
}

// NewHandler constructs the stravin-ctrl HTTP admin Handler.
func NewHandler(cfg HandlerConfig) (*Handler, error) {
	if cfg.Leases == nil {
		return nil, fmt.Errorf("%w: lease manager is required", ErrInvalidConfig)
	}
	if cfg.Scrubber == nil {
		return nil, fmt.Errorf("%w: scrubber is required", ErrInvalidConfig)
	}
	svc := strings.TrimSpace(cfg.ServiceName)
	if svc == "" {
		svc = defaultServiceName
	}

	h := &Handler{
		serviceName: svc,
		leases:      cfg.Leases,
		scrubber:    cfg.Scrubber,
		adminToken:  strings.TrimSpace(cfg.AdminToken),
		mux:         http.NewServeMux(),
	}
	h.mux.HandleFunc("/healthz", h.handleHealthz)
	h.mux.HandleFunc("/v1/lease", h.requireAdminAuth(h.handleLeaseStatus))
	h.mux.HandleFunc("/v1/lease/status", h.requireAdminAuth(h.handleLeaseStatus))
	h.mux.HandleFunc("/v1/lease/fence", h.requireAdminAuth(h.handleLeaseFence))
	h.mux.HandleFunc("/v1/epoch/advance", h.requireAdminAuth(h.handleEpochAdvance))
	h.mux.HandleFunc("/v1/scrub", h.requireAdminAuth(h.handleScrub))
	return h, nil
}

// ServeHTTP dispatches incoming HTTP admin requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) requireAdminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.isAuthorized(r.Header.Get("Authorization")) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (h *Handler) isAuthorized(authHeader string) bool {
	if h.adminToken == "" {
		return false
	}
	scheme, token, ok := strings.Cut(authHeader, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(h.adminToken)) == 1
}

func (h *Handler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": h.serviceName,
	})
}

func (h *Handler) handleLeaseStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, h.leases.Status())
}

type fenceHTTPRequest struct {
	Mode         lease.Mode  `json:"mode,omitempty"`
	AdvanceEpoch *bool       `json:"advance_epoch,omitempty"`
	MinEpoch     lease.Epoch `json:"min_epoch,omitempty"`
	FenceWaitMs  int64       `json:"fence_wait_ms,omitempty"`
	TenantID     string      `json:"tenant_id,omitempty"`
	RepoID       string      `json:"repo_id,omitempty"`
}

func (h *Handler) handleLeaseFence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var payload fenceHTTPRequest
	if err := decodeOptionalJSON(w, r, &payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	advance := true
	if payload.AdvanceEpoch != nil {
		advance = *payload.AdvanceEpoch
	}

	req := FenceRequest{
		TargetMode:   payload.Mode,
		AdvanceEpoch: advance,
		MinEpoch:     payload.MinEpoch,
	}
	if payload.FenceWaitMs > 0 {
		req.FenceWait = time.Duration(payload.FenceWaitMs) * time.Millisecond
	}
	if payload.TenantID != "" || payload.RepoID != "" {
		rk, err := tenant.NewRepoKey(payload.TenantID, payload.RepoID)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.Repo = &rk
	}

	res, err := h.leases.FenceRegion(r.Context(), req)
	if err != nil {
		writeJSONError(w, mapControlError(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type epochAdvanceHTTPRequest struct {
	TenantID    string      `json:"tenant_id,omitempty"`
	RepoID      string      `json:"repo_id,omitempty"`
	Epoch       lease.Epoch `json:"epoch,omitempty"`
	Mode        lease.Mode  `json:"mode,omitempty"`
	FenceWaitMs int64       `json:"fence_wait_ms,omitempty"`
}

func (h *Handler) handleEpochAdvance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var payload epochAdvanceHTTPRequest
	if err := decodeOptionalJSON(w, r, &payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	req := FenceRequest{
		TargetMode:   payload.Mode,
		AdvanceEpoch: true,
		MinEpoch:     payload.Epoch,
	}
	if payload.FenceWaitMs > 0 {
		req.FenceWait = time.Duration(payload.FenceWaitMs) * time.Millisecond
	}
	if payload.TenantID != "" || payload.RepoID != "" {
		rk, err := tenant.NewRepoKey(payload.TenantID, payload.RepoID)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.Repo = &rk
	}

	res, err := h.leases.FenceRegion(r.Context(), req)
	if err != nil {
		writeJSONError(w, mapControlError(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type scrubHTTPRequest struct {
	TenantID        string `json:"tenant_id"`
	RepoID          string `json:"repo_id"`
	LookbackSeconds int64  `json:"lookback_seconds,omitempty"`
	Concurrency     int    `json:"concurrency,omitempty"`
	DryRun          bool   `json:"dry_run,omitempty"`
}

func (h *Handler) handleScrub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var payload scrubHTTPRequest
	if err := decodeJSON(w, r, &payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	rk, err := tenant.NewRepoKey(payload.TenantID, payload.RepoID)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	req := ScrubRequest{
		Repo:        rk,
		Concurrency: payload.Concurrency,
		DryRun:      payload.DryRun,
	}
	if payload.LookbackSeconds > 0 {
		req.LookbackWindow = time.Duration(payload.LookbackSeconds) * time.Second
	}

	report, err := h.scrubber.ScrubRepo(r.Context(), req)
	if err != nil {
		writeJSONError(w, mapControlError(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid json payload: %w", err)
	}
	return nil
}

func decodeOptionalJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	return decodeJSON(w, r, dst)
}

func mapControlError(err error) int {
	switch {
	case errors.Is(err, ddb.ErrNotFound), errors.Is(err, lease.ErrLeaseNotFound):
		return http.StatusNotFound
	case errors.Is(err, ddb.ErrEpochConflict), errors.Is(err, ddb.ErrLeaseHeld), errors.Is(err, ddb.ErrRefConflict), errors.Is(err, ErrNonMonotonicEpoch):
		return http.StatusConflict
	case errors.Is(err, ErrInvalidTransition), errors.Is(err, ddb.ErrInvalidInput), errors.Is(err, tenant.ErrInvalidTenantID), errors.Is(err, tenant.ErrInvalidRepoID):
		return http.StatusBadRequest
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
