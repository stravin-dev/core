package edge

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func reserveTestAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestServer_HealthAddrServesHealthzAndNotMetrics(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	hostKey, err := LoadOrGenerateHostKey("")
	if err != nil {
		t.Fatalf("LoadOrGenerateHostKey: %v", err)
	}

	healthAddr := reserveTestAddr(t)
	srv, err := NewServer(ServerConfig{
		SSHAddr:       "127.0.0.1:0",
		HTTPAddr:      "127.0.0.1:0",
		HealthAddr:    healthAddr,
		DrainDelay:    200 * time.Millisecond,
		HostKey:       hostKey,
		Authenticator: env.handler.auth,
		Limiter:       env.handler.limiter,
		Resolver:      env.handler.resolver,
		Router:        env.handler.router,
		Proxy:         env.handler.proxy,
		Telemetry:     env.tel,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- srv.Run(ctx)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	var healthResp *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		healthResp, err = client.Get("http://" + healthAddr + "/healthz")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer func() { _ = healthResp.Body.Close() }()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", healthResp.StatusCode)
	}

	for _, path := range []string{"/livez", "/readyz", "/startupz", "/readyz?verbose=1"} {
		resp, reqErr := client.Get("http://" + healthAddr + path)
		if reqErr != nil {
			t.Fatalf("GET %s failed: %v", path, reqErr)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}

	metricsResp, err := client.Get("http://" + healthAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer func() { _ = metricsResp.Body.Close() }()
	if metricsResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /metrics on HealthAddr status = %d, want 404", metricsResp.StatusCode)
	}

	cancel()
	var saw503 bool
	drainDeadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(drainDeadline) {
		resp, reqErr := client.Get("http://" + healthAddr + "/readyz")
		if reqErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable {
				saw503 = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !saw503 {
		t.Fatal("expected /readyz on edge HealthAddr to return 503 during drain")
	}

	select {
	case runErr := <-runErrCh:
		if runErr != nil && !strings.Contains(runErr.Error(), "closed") {
			t.Fatalf("Server.Run returned unexpected error: %v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Server.Run did not shut down within timeout")
	}
}
