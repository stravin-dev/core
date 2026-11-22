package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"stravin.dev/core/internal/health"
)

func TestRegistry_LifecycleTransitions(t *testing.T) {
	reg := health.New("stravin-test")
	mux := http.NewServeMux()
	reg.Register(mux)

	assertStatus := func(path string, want int) {
		t.Helper()
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Fatalf("GET %s = %d, want %d (body=%q)", path, rec.Code, want, rec.Body.String())
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("GET %s Cache-Control = %q, want %q", path, cc, "no-store")
		}
	}

	// Initial state: alive, not started, not ready.
	assertStatus("/livez", http.StatusOK)
	assertStatus("/startupz", http.StatusServiceUnavailable)
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/healthz", http.StatusServiceUnavailable)

	// Started, not yet ready.
	reg.MarkStarted()
	assertStatus("/livez", http.StatusOK)
	assertStatus("/startupz", http.StatusOK)
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/healthz", http.StatusServiceUnavailable)

	// Ready.
	reg.MarkReady()
	assertStatus("/livez", http.StatusOK)
	assertStatus("/startupz", http.StatusOK)
	assertStatus("/readyz", http.StatusOK)
	assertStatus("/healthz", http.StatusOK)

	// Temporarily not ready.
	reg.MarkNotReady()
	assertStatus("/livez", http.StatusOK)
	assertStatus("/startupz", http.StatusOK)
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/healthz", http.StatusServiceUnavailable)

	// Ready again, then draining.
	reg.MarkReady()
	assertStatus("/readyz", http.StatusOK)

	reg.SetDraining()
	assertStatus("/livez", http.StatusOK)
	assertStatus("/startupz", http.StatusOK)
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/healthz", http.StatusServiceUnavailable)
}

func TestRegistry_VerboseOutputAndCustomChecks(t *testing.T) {
	reg := health.New("stravin-node", health.WithMetadata("region", "us-east-1"))
	reg.MarkStarted()
	reg.MarkReady()

	var failLive, failReady, failStartup bool
	reg.AddLivenessCheck("goroutines", func(context.Context) error {
		if failLive {
			return errors.New("deadlock detected")
		}
		return nil
	})
	reg.AddReadinessCheck("ddb", func(context.Context) error {
		if failReady {
			return errors.New("ddb unreachable")
		}
		return nil
	})
	reg.AddStartupCheck("cache-warm", func(context.Context) error {
		if failStartup {
			return errors.New("warming cache")
		}
		return nil
	})

	mux := http.NewServeMux()
	reg.Register(mux)

	// Non-verbose JSON includes service and metadata.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if payload["status"] != "ok" || payload["service"] != "stravin-node" || payload["region"] != "us-east-1" {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	// Verbose passing (?verbose=1 and ?verbose).
	for _, query := range []string{"?verbose=1", "?verbose"} {
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz"+query, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /readyz%s = %d, want 200", query, rec.Code)
		}
		body := rec.Body.String()
		for _, want := range []string{"[+]started ok", "[+]draining ok", "[+]ready ok", "[+]ddb ok", "readyz check passed"} {
			if !strings.Contains(body, want) {
				t.Fatalf("verbose readyz body missing %q:\n%s", want, body)
			}
		}
	}

	// Failing readiness check.
	failReady = true
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz?verbose=1", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz with failing check = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "[-]ddb failed: ddb unreachable") {
		t.Fatalf("unexpected failing readyz verbose output:\n%s", rec.Body.String())
	}
	failReady = false

	// Failing liveness check fails /livez and /healthz.
	failLive = true
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/livez?verbose=1", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /livez with failing check = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "[-]goroutines failed: deadlock detected") {
		t.Fatalf("unexpected failing livez verbose output:\n%s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz with failing liveness = %d, want 503", rec.Code)
	}
	failLive = false

	// Failing startup check fails /startupz.
	failStartup = true
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/startupz?verbose=1", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /startupz with failing check = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "[-]cache-warm failed: warming cache") {
		t.Fatalf("unexpected failing startupz verbose output:\n%s", rec.Body.String())
	}
	failStartup = false

	// Draining verbose output.
	reg.SetDraining()
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz?verbose=1", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz while draining = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "[-]draining failed: shutting down") {
		t.Fatalf("unexpected draining verbose output:\n%s", rec.Body.String())
	}
}

func TestRegistry_MethodsAndServeProbe(t *testing.T) {
	reg := health.New("stravin-edge")
	reg.MarkReady()

	// HEAD returns 200 with empty body.
	rec := httptest.NewRecorder()
	if !reg.ServeProbe(rec, httptest.NewRequest(http.MethodHead, "/readyz", nil)) {
		t.Fatal("ServeProbe returned false for /readyz")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD /readyz = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD /readyz body len = %d, want 0", rec.Body.Len())
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("HEAD /readyz Cache-Control = %q, want no-store", cc)
	}

	// POST returns 405 Method Not Allowed.
	rec = httptest.NewRecorder()
	if !reg.ServeProbe(rec, httptest.NewRequest(http.MethodPost, "/livez", nil)) {
		t.Fatal("ServeProbe returned false for /livez")
	}
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /livez = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Fatalf("POST /livez Allow = %q, want %q", allow, "GET, HEAD")
	}

	// Non-probe path returns false.
	rec = httptest.NewRecorder()
	if reg.ServeProbe(rec, httptest.NewRequest(http.MethodGet, "/acme/repo.git/info/refs", nil)) {
		t.Fatal("ServeProbe returned true for non-probe path")
	}
}

func TestRegistry_ConcurrentAccessRaceFree(t *testing.T) {
	reg := health.New("stravin-pack")
	mux := http.NewServeMux()
	reg.Register(mux)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				switch (id + j) % 4 {
				case 0:
					reg.MarkStarted()
				case 1:
					reg.MarkReady()
				case 2:
					reg.MarkNotReady()
				case 3:
					reg.SetDraining()
				}
				for _, path := range []string{"/livez", "/readyz", "/startupz", "/healthz?verbose=1"} {
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
				}
			}
		}(i)
	}
	wg.Wait()
}
