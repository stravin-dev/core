package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/chaos"
	"stravin.dev/core/internal/telemetry"
)

type spyServiceController struct {
	mu        sync.Mutex
	restarted []string
	err       error
}

func (s *spyServiceController) Restart(_ context.Context, service string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.restarted = append(s.restarted, service)
	return nil
}

func (s *spyServiceController) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.restarted))
	copy(out, s.restarted)
	return out
}

func defaultTestProxies() []chaos.Proxy {
	return []chaos.Proxy{
		{Name: "edge_http", Listen: "0.0.0.0:18080", Upstream: "stravin-edge:8080", Enabled: true},
		{Name: "edge_ssh", Listen: "0.0.0.0:12222", Upstream: "stravin-edge:2222", Enabled: true},
		{Name: "edge_to_node_grpc", Listen: "0.0.0.0:19090", Upstream: "stravin-node:9090", Enabled: true},
		{Name: "node_to_floci", Listen: "0.0.0.0:14566", Upstream: "floci:4566", Enabled: true},
	}
}

func TestNemesis_DeterministicScheduleUnderSeed(t *testing.T) {
	runWithSeed := func(seed int64) []chaos.FaultRecord {
		_, srv := newFakeToxiproxyServer(t, defaultTestProxies())
		client := chaos.NewToxiproxyClient(srv.URL, srv.Client())

		var mu sync.Mutex
		var records []chaos.FaultRecord

		nemesis, err := chaos.NewNemesis(chaos.Config{
			Seed:          seed,
			Interval:      time.Millisecond,
			FaultDuration: time.Millisecond,
			MaxFaults:     10,
		}, chaos.Dependencies{
			Proxy:  client,
			Logger: telemetry.NewLogger(&bytes.Buffer{}, slog.LevelInfo),
			OnFault: func(rec chaos.FaultRecord) {
				mu.Lock()
				records = append(records, rec)
				mu.Unlock()
			},
		})
		if err != nil {
			t.Fatalf("NewNemesis failed: %v", err)
		}
		if err := nemesis.Run(context.Background()); err != nil {
			t.Fatalf("Nemesis.Run failed: %v", err)
		}
		return records
	}

	first := runWithSeed(42)
	second := runWithSeed(42)

	if len(first) != 10 {
		t.Fatalf("expected 10 fault records, got %d", len(first))
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("expected identical fault schedules under same seed:\nfirst:  %+v\nsecond: %+v", first, second)
	}
}

func TestNemesis_ResetAllOnContextCancellation(t *testing.T) {
	_, srv := newFakeToxiproxyServer(t, defaultTestProxies())
	client := chaos.NewToxiproxyClient(srv.URL, srv.Client())

	injected := make(chan struct{}, 1)
	nemesis, err := chaos.NewNemesis(chaos.Config{
		Seed:          7,
		Interval:      time.Hour,
		FaultDuration: time.Hour,
	}, chaos.Dependencies{
		Proxy:  client,
		Logger: telemetry.NewLogger(&bytes.Buffer{}, slog.LevelInfo),
		OnFault: func(_ chaos.FaultRecord) {
			select {
			case injected <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("NewNemesis failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- nemesis.Run(ctx)
	}()

	select {
	case <-injected:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for first fault injection")
	}

	// Cancel mid-fault (simulating SIGINT/SIGTERM) and verify all toxics are cleared.
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected Run error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Nemesis.Run to exit")
	}

	proxies, err := client.ListProxies(context.Background())
	if err != nil {
		t.Fatalf("ListProxies after cancel failed: %v", err)
	}
	for _, p := range proxies {
		if len(p.Toxics) != 0 {
			t.Fatalf("expected 0 active toxics on %s after cancellation, got %d", p.Name, len(p.Toxics))
		}
	}
}

func TestGuardedServiceController_RefusesFlociAndDisallowedTargets(t *testing.T) {
	spy := &spyServiceController{}
	guarded := chaos.NewGuardedServiceController(spy, []string{"stravin-node", "stravin-edge"})

	for _, forbidden := range []string{"floci", "floci-init", "toxiproxy", "stravin-ctrl", "", "--help"} {
		if err := guarded.Restart(context.Background(), forbidden); err == nil {
			t.Fatalf("expected error restarting forbidden service %q, got nil", forbidden)
		}
	}

	if err := guarded.Restart(context.Background(), "stravin-node"); err != nil {
		t.Fatalf("expected allowed restart stravin-node to succeed, got %v", err)
	}
	got := spy.calls()
	if !reflect.DeepEqual(got, []string{"stravin-node"}) {
		t.Fatalf("expected only [stravin-node] restarted, got %v", got)
	}
}

func TestNemesis_RefusesFlociInRestartTargetsConfig(t *testing.T) {
	_, srv := newFakeToxiproxyServer(t, defaultTestProxies())
	client := chaos.NewToxiproxyClient(srv.URL, srv.Client())

	_, err := chaos.NewNemesis(chaos.Config{
		Seed:           1,
		Interval:       time.Millisecond,
		FaultDuration:  time.Millisecond,
		EnableRestarts: true,
		RestartTargets: []string{"stravin-node", "floci"},
	}, chaos.Dependencies{
		Proxy:   client,
		Service: &spyServiceController{},
	})
	if err == nil {
		t.Fatal("expected NewNemesis to reject floci in RestartTargets, got nil")
	}
}

func TestNemesis_EmitsOTelSpansAndOptionalRestarts(t *testing.T) {
	_, srv := newFakeToxiproxyServer(t, defaultTestProxies())
	client := chaos.NewToxiproxyClient(srv.URL, srv.Client())
	spySvc := &spyServiceController{}

	tp, err := telemetry.NewInMemoryProvider("stravin-chaos-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider failed: %v", err)
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()

	var logBuf bytes.Buffer
	nemesis, err := chaos.NewNemesis(chaos.Config{
		Seed:           99,
		Interval:       time.Millisecond,
		FaultDuration:  time.Millisecond,
		MaxFaults:      24,
		EnableRestarts: true,
		RestartTargets: []string{"stravin-node", "stravin-edge"},
	}, chaos.Dependencies{
		Proxy:   client,
		Service: spySvc,
		Tracer:  tp.Tracer("stravin-chaos"),
		Logger:  telemetry.NewLogger(&logBuf, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("NewNemesis failed: %v", err)
	}

	if err := nemesis.Run(context.Background()); err != nil {
		t.Fatalf("Nemesis.Run failed: %v", err)
	}

	spans := tp.SpanExporter.GetSpans()
	if len(spans) != 24 {
		t.Fatalf("expected 24 OTel fault spans, got %d", len(spans))
	}
	if len(spySvc.calls()) == 0 {
		t.Fatal("expected at least one service restart across 24 seeded faults")
	}
	for _, svc := range spySvc.calls() {
		if svc == "floci" {
			t.Fatal("floci must never be restarted")
		}
	}
}
