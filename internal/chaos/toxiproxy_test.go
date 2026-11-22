package chaos_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"stravin.dev/core/internal/chaos"
)

type fakeToxiproxyServer struct {
	mu      sync.Mutex
	proxies map[string]chaos.Proxy
}

func newFakeToxiproxyServer(t *testing.T, initial []chaos.Proxy) (*fakeToxiproxyServer, *httptest.Server) {
	t.Helper()
	f := &fakeToxiproxyServer{
		proxies: make(map[string]chaos.Proxy, len(initial)),
	}
	for _, p := range initial {
		f.proxies[p.Name] = p
	}
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeToxiproxyServer) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet && r.URL.Path == "/proxies" {
		_ = json.NewEncoder(w).Encode(f.proxies)
		return
	}

	if !strings.HasPrefix(r.URL.Path, "/proxies/") {
		http.NotFound(w, r)
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/proxies/")
	parts := strings.Split(rest, "/")
	if len(parts) < 2 || parts[1] != "toxics" {
		http.NotFound(w, r)
		return
	}

	proxyName := parts[0]
	proxy, ok := f.proxies[proxyName]
	if !ok {
		http.Error(w, `{"error":"proxy not found"}`, http.StatusNotFound)
		return
	}

	switch {
	case r.Method == http.MethodPost && len(parts) == 2:
		var toxic chaos.Toxic
		if err := json.NewDecoder(r.Body).Decode(&toxic); err != nil {
			http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
			return
		}
		if toxic.Name == "" {
			toxic.Name = toxic.Type + "_" + toxic.Stream
		}
		proxy.Toxics = append(proxy.Toxics, toxic)
		f.proxies[proxyName] = proxy
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(toxic)
		return

	case r.Method == http.MethodDelete && len(parts) == 3:
		toxicName := parts[2]
		filtered := make([]chaos.Toxic, 0, len(proxy.Toxics))
		found := false
		for _, tx := range proxy.Toxics {
			if tx.Name == toxicName {
				found = true
				continue
			}
			filtered = append(filtered, tx)
		}
		if !found {
			http.Error(w, `{"error":"toxic not found"}`, http.StatusNotFound)
			return
		}
		proxy.Toxics = filtered
		f.proxies[proxyName] = proxy
		w.WriteHeader(http.StatusNoContent)
		return
	}

	http.NotFound(w, r)
}

func TestToxiproxyClient_ListAddRemoveReset(t *testing.T) {
	_, srv := newFakeToxiproxyServer(t, []chaos.Proxy{
		{Name: "node_to_floci", Listen: "0.0.0.0:14566", Upstream: "floci:4566", Enabled: true},
		{Name: "edge_http", Listen: "0.0.0.0:18080", Upstream: "stravin-edge:8080", Enabled: true},
	})

	client := chaos.NewToxiproxyClient(srv.URL, srv.Client())
	ctx := context.Background()

	proxies, err := client.ListProxies(ctx)
	if err != nil {
		t.Fatalf("ListProxies failed: %v", err)
	}
	if len(proxies) != 2 {
		t.Fatalf("expected 2 proxies, got %d", len(proxies))
	}
	// Sorted alphabetically for determinism.
	if proxies[0].Name != "edge_http" || proxies[1].Name != "node_to_floci" {
		t.Fatalf("expected sorted proxy names, got %q, %q", proxies[0].Name, proxies[1].Name)
	}

	added, err := client.AddToxic(ctx, "edge_http", chaos.Toxic{
		Name:       "chaos_latency_1",
		Type:       "latency",
		Stream:     "downstream",
		Toxicity:   1.0,
		Attributes: map[string]any{"latency": 150, "jitter": 25},
	})
	if err != nil {
		t.Fatalf("AddToxic failed: %v", err)
	}
	if added.Name != "chaos_latency_1" {
		t.Fatalf("expected toxic name chaos_latency_1, got %q", added.Name)
	}

	if _, err := client.AddToxic(ctx, "node_to_floci", chaos.Toxic{
		Name:       "chaos_bw_2",
		Type:       "bandwidth",
		Stream:     "downstream",
		Toxicity:   1.0,
		Attributes: map[string]any{"rate": 128},
	}); err != nil {
		t.Fatalf("second AddToxic failed: %v", err)
	}

	if err := client.RemoveToxic(ctx, "edge_http", "chaos_latency_1"); err != nil {
		t.Fatalf("RemoveToxic failed: %v", err)
	}

	if err := client.ResetAll(ctx); err != nil {
		t.Fatalf("ResetAll failed: %v", err)
	}

	proxies, err = client.ListProxies(ctx)
	if err != nil {
		t.Fatalf("ListProxies after ResetAll failed: %v", err)
	}
	for _, p := range proxies {
		if len(p.Toxics) != 0 {
			t.Fatalf("expected 0 toxics on %s after ResetAll, got %d", p.Name, len(p.Toxics))
		}
	}
}
