package daemon_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"stravin.dev/core/internal/daemon"
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
