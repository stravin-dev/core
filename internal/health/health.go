// Package health provides a thread-safe Kubernetes-style health probe registry
// supporting /livez, /readyz, /startupz, and backward-compatible /healthz
// endpoints with optional ?verbose=1 per-check diagnostics.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// CheckFunc evaluates a single probe check. Returning a non-nil error marks the
// check as failed.
type CheckFunc func(ctx context.Context) error

type namedCheck struct {
	name string
	fn   CheckFunc
}

type checkResult struct {
	name string
	err  error
}

// Option configures a Registry.
type Option func(*Registry)

// WithMetadata attaches a static key/value pair to non-verbose JSON probe
// responses (for example, "region").
func WithMetadata(key, value string) Option {
	return func(r *Registry) {
		k := strings.TrimSpace(key)
		if k == "" || k == "status" || k == "service" {
			return
		}
		r.metadata[k] = value
	}
}

// Registry tracks daemon startup, readiness, and draining lifecycle state
// along with pluggable liveness, readiness, and startup checks.
type Registry struct {
	mu              sync.RWMutex
	serviceName     string
	metadata        map[string]string
	started         bool
	ready           bool
	draining        bool
	livenessChecks  []namedCheck
	readinessChecks []namedCheck
	startupChecks   []namedCheck
}

// New constructs a Registry for serviceName. The registry starts alive but not
// started and not ready until MarkStarted / MarkReady are called.
func New(serviceName string, opts ...Option) *Registry {
	r := &Registry{
		serviceName: strings.TrimSpace(serviceName),
		metadata:    make(map[string]string),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

// MarkStarted marks initialization complete so /startupz can pass.
func (r *Registry) MarkStarted() {
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
}

// MarkReady marks the daemon started and ready to accept traffic.
func (r *Registry) MarkReady() {
	r.mu.Lock()
	r.started = true
	r.ready = true
	r.draining = false
	r.mu.Unlock()
}

// MarkNotReady marks the daemon temporarily not ready without entering drain.
func (r *Registry) MarkNotReady() {
	r.mu.Lock()
	r.ready = false
	r.mu.Unlock()
}

// SetDraining marks the daemon as shutting down; /readyz and /healthz
// immediately transition to 503 Service Unavailable.
func (r *Registry) SetDraining() {
	r.mu.Lock()
	r.draining = true
	r.ready = false
	r.mu.Unlock()
}

// Started reports whether MarkStarted or MarkReady has been called.
func (r *Registry) Started() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.started
}

// Ready reports whether the daemon is started, ready, and not draining.
func (r *Registry) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.started && r.ready && !r.draining
}

// Draining reports whether SetDraining has been called.
func (r *Registry) Draining() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.draining
}

// AddLivenessCheck registers a named liveness check evaluated on /livez and /healthz.
func (r *Registry) AddLivenessCheck(name string, fn CheckFunc) {
	if strings.TrimSpace(name) == "" || fn == nil {
		return
	}
	r.mu.Lock()
	r.livenessChecks = append(r.livenessChecks, namedCheck{name: name, fn: fn})
	r.mu.Unlock()
}

// AddReadinessCheck registers a named readiness check evaluated on /readyz and /healthz.
func (r *Registry) AddReadinessCheck(name string, fn CheckFunc) {
	if strings.TrimSpace(name) == "" || fn == nil {
		return
	}
	r.mu.Lock()
	r.readinessChecks = append(r.readinessChecks, namedCheck{name: name, fn: fn})
	r.mu.Unlock()
}

// AddStartupCheck registers a named startup check evaluated on /startupz.
func (r *Registry) AddStartupCheck(name string, fn CheckFunc) {
	if strings.TrimSpace(name) == "" || fn == nil {
		return
	}
	r.mu.Lock()
	r.startupChecks = append(r.startupChecks, namedCheck{name: name, fn: fn})
	r.mu.Unlock()
}

// Register mounts /livez, /readyz, /startupz, and /healthz on mux.
func (r *Registry) Register(mux *http.ServeMux) {
	mux.HandleFunc("/livez", r.LivezHandler())
	mux.HandleFunc("/readyz", r.ReadyzHandler())
	mux.HandleFunc("/startupz", r.StartupzHandler())
	mux.HandleFunc("/healthz", r.HealthzHandler())
}

// ServeProbe handles /livez, /readyz, /startupz, and /healthz directly when
// invoked from a custom http.Handler. It returns true if the request path
// matched a probe endpoint.
func (r *Registry) ServeProbe(w http.ResponseWriter, req *http.Request) bool {
	switch req.URL.Path {
	case "/livez":
		r.serveProbe(w, req, "livez", r.evalLivez)
		return true
	case "/readyz":
		r.serveProbe(w, req, "readyz", r.evalReadyz)
		return true
	case "/startupz":
		r.serveProbe(w, req, "startupz", r.evalStartupz)
		return true
	case "/healthz":
		r.serveProbe(w, req, "healthz", r.evalHealthz)
		return true
	default:
		return false
	}
}

// LivezHandler returns the /livez HTTP handler.
func (r *Registry) LivezHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.serveProbe(w, req, "livez", r.evalLivez)
	}
}

// ReadyzHandler returns the /readyz HTTP handler.
func (r *Registry) ReadyzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.serveProbe(w, req, "readyz", r.evalReadyz)
	}
}

// StartupzHandler returns the /startupz HTTP handler.
func (r *Registry) StartupzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.serveProbe(w, req, "startupz", r.evalStartupz)
	}
}

// HealthzHandler returns the backward-compatible /healthz HTTP handler.
func (r *Registry) HealthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.serveProbe(w, req, "healthz", r.evalHealthz)
	}
}

func (r *Registry) serveProbe(w http.ResponseWriter, req *http.Request, probeName string, eval func(context.Context) []checkResult) {
	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		if req.Method != http.MethodHead {
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		}
		return
	}

	results := eval(req.Context())
	ok := true
	for _, res := range results {
		if res.err != nil {
			ok = false
			break
		}
	}

	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}

	if isVerbose(req) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		if req.Method == http.MethodHead {
			return
		}
		var b strings.Builder
		for _, res := range results {
			if res.err == nil {
				fmt.Fprintf(&b, "[+]%s ok\n", res.name)
			} else {
				fmt.Fprintf(&b, "[-]%s failed: %v\n", res.name, res.err)
			}
		}
		if ok {
			fmt.Fprintf(&b, "%s check passed\n", probeName)
		} else {
			fmt.Fprintf(&b, "%s check failed\n", probeName)
		}
		_, _ = w.Write([]byte(b.String()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if req.Method == http.MethodHead {
		return
	}

	payload := make(map[string]string, 2+len(r.metadata))
	if ok {
		payload["status"] = "ok"
	} else {
		payload["status"] = "unavailable"
	}
	if r.serviceName != "" {
		payload["service"] = r.serviceName
	}
	for k, v := range r.metadata {
		payload[k] = v
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func (r *Registry) evalLivez(ctx context.Context) []checkResult {
	r.mu.RLock()
	checks := append([]namedCheck(nil), r.livenessChecks...)
	r.mu.RUnlock()

	results := make([]checkResult, 0, 1+len(checks))
	results = append(results, checkResult{name: "ping"})
	for _, c := range checks {
		results = append(results, checkResult{name: c.name, err: c.fn(ctx)})
	}
	return results
}

func (r *Registry) evalStartupz(ctx context.Context) []checkResult {
	r.mu.RLock()
	started := r.started
	checks := append([]namedCheck(nil), r.startupChecks...)
	r.mu.RUnlock()

	results := make([]checkResult, 0, 1+len(checks))
	if started {
		results = append(results, checkResult{name: "started"})
	} else {
		results = append(results, checkResult{name: "started", err: errors.New("not started")})
	}
	for _, c := range checks {
		results = append(results, checkResult{name: c.name, err: c.fn(ctx)})
	}
	return results
}

func (r *Registry) evalReadyz(ctx context.Context) []checkResult {
	r.mu.RLock()
	started := r.started
	draining := r.draining
	ready := r.ready
	checks := append([]namedCheck(nil), r.readinessChecks...)
	r.mu.RUnlock()

	results := make([]checkResult, 0, 3+len(checks))
	if started {
		results = append(results, checkResult{name: "started"})
	} else {
		results = append(results, checkResult{name: "started", err: errors.New("not started")})
	}
	if !draining {
		results = append(results, checkResult{name: "draining"})
	} else {
		results = append(results, checkResult{name: "draining", err: errors.New("shutting down")})
	}
	if ready {
		results = append(results, checkResult{name: "ready"})
	} else {
		results = append(results, checkResult{name: "ready", err: errors.New("not ready")})
	}
	for _, c := range checks {
		results = append(results, checkResult{name: c.name, err: c.fn(ctx)})
	}
	return results
}

func (r *Registry) evalHealthz(ctx context.Context) []checkResult {
	live := r.evalLivez(ctx)
	ready := r.evalReadyz(ctx)
	results := make([]checkResult, 0, len(live)+len(ready))
	results = append(results, live...)
	results = append(results, ready...)
	return results
}

func isVerbose(req *http.Request) bool {
	q := req.URL.Query()
	vals, ok := q["verbose"]
	if !ok {
		return false
	}
	if len(vals) == 0 {
		return true
	}
	v := strings.ToLower(strings.TrimSpace(vals[0]))
	return v != "0" && v != "false"
}
