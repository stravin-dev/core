// Package edge implements the stateless stravin-edge Git ingress daemon:
// SSH (:2222) and Smart HTTP (:8080) termination, authentication, per-tenant
// rate limiting, home_region/write-lease enforcement, tenant-aware rendezvous
// hashing, and bidirectional gRPC wire streaming to stravin-node.
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/telemetry"
)

const defaultShutdownTimeout = 10 * time.Second

// ServerConfig configures the unified stravin-edge daemon facade.
type ServerConfig struct {
	SSHAddr         string
	HTTPAddr        string
	HealthAddr      string // Optional dedicated /healthz & /metrics listener (e.g. ":8081").
	ShutdownTimeout time.Duration
	HostKey         ssh.Signer
	Authenticator   Authenticator
	Limiter         Limiter
	Resolver        *Resolver
	Router          NodeSelector
	Proxy           *WireProxy
	Telemetry       *telemetry.Provider
	Logger          *slog.Logger
}

// Server coordinates the SSH, Smart HTTP, and optional dedicated health/metrics listeners.
type Server struct {
	cfg         ServerConfig
	metrics     *Metrics
	httpHandler *HTTPHandler
	sshServer   *SSHServer
}

// NewServer builds a Server facade from cfg.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.SSHAddr == "" {
		cfg.SSHAddr = ":2222"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	metrics, err := NewMetrics(cfg.Telemetry)
	if err != nil {
		return nil, err
	}
	httpHandler, err := NewHTTPHandler(cfg.Authenticator, cfg.Limiter, cfg.Resolver, cfg.Router, cfg.Proxy, metrics, cfg.Logger)
	if err != nil {
		return nil, err
	}
	sshServer, err := NewSSHServer(cfg.HostKey, cfg.Authenticator, cfg.Limiter, cfg.Resolver, cfg.Router, cfg.Proxy, metrics, cfg.Logger)
	if err != nil {
		return nil, err
	}

	return &Server{
		cfg:         cfg,
		metrics:     metrics,
		httpHandler: httpHandler,
		sshServer:   sshServer,
	}, nil
}

// Run starts all configured listeners and blocks until ctx is cancelled or a listener fails.
func (s *Server) Run(ctx context.Context) error {
	httpLn, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("edge: listen http %s: %w", s.cfg.HTTPAddr, err)
	}
	sshLn, err := net.Listen("tcp", s.cfg.SSHAddr)
	if err != nil {
		_ = httpLn.Close()
		return fmt.Errorf("edge: listen ssh %s: %w", s.cfg.SSHAddr, err)
	}

	var healthLn net.Listener
	var healthSrv *http.Server
	if s.cfg.HealthAddr != "" && s.cfg.HealthAddr != s.cfg.HTTPAddr {
		healthLn, err = net.Listen("tcp", s.cfg.HealthAddr)
		if err != nil {
			_ = httpLn.Close()
			_ = sshLn.Close()
			return fmt.Errorf("edge: listen health %s: %w", s.cfg.HealthAddr, err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":  "ok",
				"service": "stravin-edge",
			})
		})
		mux.Handle("/metrics", s.metrics)
		healthSrv = NewHTTPServer(s.cfg.HealthAddr, mux)
	}

	httpSrv := NewHTTPServer(s.cfg.HTTPAddr, s.httpHandler)
	errCh := make(chan error, 3)

	s.cfg.Logger.Info("stravin-edge listening",
		"http_addr", httpLn.Addr().String(),
		"ssh_addr", sshLn.Addr().String(),
		"health_addr", s.cfg.HealthAddr,
	)

	go func() {
		if serveErr := httpSrv.Serve(httpLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- fmt.Errorf("edge: http serve: %w", serveErr)
			return
		}
		errCh <- nil
	}()
	go func() {
		if serveErr := s.sshServer.Serve(sshLn); serveErr != nil {
			errCh <- fmt.Errorf("edge: ssh serve: %w", serveErr)
			return
		}
		errCh <- nil
	}()
	if healthSrv != nil && healthLn != nil {
		go func() {
			if serveErr := healthSrv.Serve(healthLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				errCh <- fmt.Errorf("edge: health serve: %w", serveErr)
				return
			}
			errCh <- nil
		}()
	}

	var runErr error
	select {
	case runErr = <-errCh:
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	var shutdownErrs []error
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}
	if err := s.sshServer.Shutdown(shutdownCtx); err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}
	if healthSrv != nil {
		if err := healthSrv.Shutdown(shutdownCtx); err != nil {
			shutdownErrs = append(shutdownErrs, err)
		}
	}
	if runErr != nil {
		return runErr
	}
	return errors.Join(shutdownErrs...)
}
