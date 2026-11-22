package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/chaos"
)

func newCLIToxiproxyServer(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	proxies := map[string]chaos.Proxy{
		"edge_http": {Name: "edge_http", Listen: "0.0.0.0:18080", Upstream: "stravin-edge:8080", Enabled: true},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/proxies" {
			_ = json.NewEncoder(w).Encode(proxies)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/toxics") {
			var tx chaos.Toxic
			_ = json.NewDecoder(r.Body).Decode(&tx)
			p := proxies["edge_http"]
			p.Toxics = append(p.Toxics, tx)
			proxies["edge_http"] = p
			_ = json.NewEncoder(w).Encode(tx)
			return
		}
		if r.Method == http.MethodDelete {
			p := proxies["edge_http"]
			p.Toxics = nil
			proxies["edge_http"] = p
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRun_CompletesOnDurationAndClearsToxics(t *testing.T) {
	srv := newCLIToxiproxyServer(t)

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--toxiproxy-url", srv.URL,
		"--seed", "123",
		"--interval", "10ms",
		"--fault-duration", "10ms",
		"--duration", "50ms",
		"--enable-restarts=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run failed: %v (stderr=%s)", err, stderr.String())
	}

	client := chaos.NewToxiproxyClient(srv.URL, srv.Client())
	proxies, err := client.ListProxies(context.Background())
	if err != nil {
		t.Fatalf("ListProxies failed: %v", err)
	}
	for _, p := range proxies {
		if len(p.Toxics) != 0 {
			t.Fatalf("expected 0 toxics after CLI exit, got %d", len(p.Toxics))
		}
	}
}

func TestRun_RejectsFlociInRestartTargets(t *testing.T) {
	srv := newCLIToxiproxyServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var stdout, stderr bytes.Buffer
	err := run(ctx, []string{
		"--toxiproxy-url", srv.URL,
		"--enable-restarts=true",
		"--restart-targets", "stravin-node,floci",
		"--duration", "20ms",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when --restart-targets contains floci, got nil")
	}
}
