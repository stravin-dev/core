// Package daemon provides a minimal shared HTTP /healthz server and graceful
// SIGINT/SIGTERM lifecycle runner for Stravin Core daemons.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const defaultShutdownTimeout = 5 * time.Second

// Config configures a Stravin daemon lifecycle runner.
type Config struct {
	ServiceName     string
	ListenAddr      string
	ShutdownTimeout time.Duration
	OnReady         func(addr string)
}

// ListenAddrFromEnv returns STRAVIN_HEALTH_ADDR when set, otherwise defaultAddr.
func ListenAddrFromEnv(defaultAddr string) string {
	if v := os.Getenv("STRAVIN_HEALTH_ADDR"); v != "" {
		return v
	}
	return defaultAddr
}

// RunWithSignals runs a daemon until SIGINT or SIGTERM is received.
func RunWithSignals(serviceName, defaultAddr string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return Run(ctx, Config{
		ServiceName: serviceName,
		ListenAddr:  ListenAddrFromEnv(defaultAddr),
	})
}

// Run starts the daemon's /healthz HTTP listener and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	if cfg.ServiceName == "" {
		return errors.New("daemon: empty service name")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": cfg.ServiceName,
		})
	})

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("daemon %s listen %s: %w", cfg.ServiceName, cfg.ListenAddr, err)
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("daemon listening", "service", cfg.ServiceName, "addr", ln.Addr().String())
	if cfg.OnReady != nil {
		cfg.OnReady(ln.Addr().String())
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("daemon %s shutdown: %w", cfg.ServiceName, err)
		}
		slog.Info("daemon stopped", "service", cfg.ServiceName)
		return <-errCh
	}
}
