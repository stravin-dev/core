package daemon_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"stravin.dev/core/internal/daemon"
	"stravin.dev/core/internal/health"
)

func TestDaemonHealthzAndGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyAddr := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- daemon.Run(ctx, daemon.Config{
			ServiceName: "stravin-edge",
			ListenAddr:  "127.0.0.1:0",
			OnReady: func(addr string) {
				readyAddr <- addr
			},
		})
	}()

	var addr string
	select {
	case addr = <-readyAddr:
	case err := <-errCh:
		t.Fatalf("daemon exited before ready: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for daemon ready")
	}

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var payload struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode /healthz: %v", err)
	}
	if payload.Status != "ok" || payload.Service != "stravin-edge" {
		t.Fatalf("payload = %+v, want {ok stravin-edge}", payload)
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("daemon.Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for graceful shutdown")
	}
}

func TestDaemonProbesLifecycleAndDraining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	probes := health.New("stravin-pack")
	if probes.Ready() || probes.Started() {
		t.Fatal("probes should start not-started and not-ready")
	}

	readyAddr := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- daemon.Run(ctx, daemon.Config{
			ServiceName: "stravin-pack",
			ListenAddr:  "127.0.0.1:0",
			DrainDelay:  200 * time.Millisecond,
			Probes:      probes,
			OnReady: func(addr string) {
				readyAddr <- addr
			},
		})
	}()

	var addr string
	select {
	case addr = <-readyAddr:
	case err := <-errCh:
		t.Fatalf("daemon exited before ready: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for daemon ready")
	}

	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/livez", "/readyz", "/startupz", "/healthz"} {
		resp, err := client.Get("http://" + addr + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, resp.StatusCode)
		}
	}

	// Cancel context -> immediately enters draining and returns 503 on /readyz while still alive on /livez.
	cancel()

	var sawDraining bool
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/readyz?verbose=1")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(body), "[-]draining failed: shutting down") {
			sawDraining = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawDraining {
		t.Fatal("expected /readyz to transition to 503 draining on shutdown")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("daemon.Run returned error on shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for graceful shutdown")
	}
}
