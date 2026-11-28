// Package edge implements the stateless stravin-edge Git ingress daemon:
// SSH (:2222) and Smart HTTP (:8080) termination, authentication, per-tenant
// rate limiting, home_region/write-lease enforcement, tenant-aware rendezvous
// hashing, and bidirectional gRPC wire streaming to stravin-node.
package edge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/health"
	"stravin.dev/core/internal/telemetry"
)

const defaultShutdownTimeout = 10 * time.Second

// ServerConfig configures the unified stravin-edge daemon facade.
type ServerConfig struct {
	SSHAddr                  string
	HTTPAddr                 string
	HealthAddr               string // Optional dedicated health probe listener (e.g. ":8081").
	MaxDecompressedBodyBytes int64
	ShutdownTimeout          time.Duration
	DrainDelay               time.Duration
	Probes                   *health.Registry
	HostKey                  ssh.Signer
	Authenticator            Authenticator
	Limiter                  Limiter
	Resolver                 *Resolver
	RouteResolver            RouteResolver
	MaxOrgDepth              int
	Router                   NodeSelector
	Proxy                    *WireProxy
	PackCache                *PackCacheCoordinator
	Telemetry                *telemetry.Provider
	Metrics                  *Metrics
	Logger                   *slog.Logger
}

// Server coordinates the SSH, Smart HTTP, and optional dedicated health listeners.
type Server struct {
	cfg         ServerConfig
	metrics     *Metrics
	httpHandler *HTTPHandler
	sshServer   *SSHServer
	probes      *health.Registry
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

	metrics := cfg.Metrics
	if metrics == nil {
		var err error
		metrics, err = NewMetrics(cfg.Telemetry)
		if err != nil {
			return nil, err
		}
	}
	if cfg.Resolver != nil {
		cfg.Resolver.SetMetrics(metrics)
	}
	httpHandler, err := NewHTTPHandler(cfg.Authenticator, cfg.Limiter, cfg.Resolver, cfg.Router, cfg.Proxy, metrics, cfg.Logger)
	if err != nil {
		return nil, err
	}
	httpHandler.SetMaxDecompressedBodyBytes(cfg.MaxDecompressedBodyBytes)
	httpHandler.SetPackCache(cfg.PackCache)
	httpHandler.SetRouteResolver(cfg.RouteResolver)
	httpHandler.SetMaxOrgDepth(cfg.MaxOrgDepth)
	sshServer, err := NewSSHServer(cfg.HostKey, cfg.Authenticator, cfg.Limiter, cfg.Resolver, cfg.Router, cfg.Proxy, metrics, cfg.Logger)
	if err != nil {
		return nil, err
	}
	sshServer.SetRouteResolver(cfg.RouteResolver)
	sshServer.SetMaxOrgDepth(cfg.MaxOrgDepth)

	probes := cfg.Probes
	if probes == nil {
		probes = health.New("stravin-edge")
	}
	httpHandler.SetProbes(probes)

	return &Server{
		cfg:         cfg,
		metrics:     metrics,
		httpHandler: httpHandler,
		sshServer:   sshServer,
		probes:      probes,
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
		s.probes.Register(mux)
		healthSrv = NewHTTPServer(s.cfg.HealthAddr, mux)
	}

	httpSrv := NewHTTPServer(s.cfg.HTTPAddr, s.httpHandler)
	errCh := make(chan error, 3)

	s.probes.MarkStarted()
	s.probes.MarkReady()

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

	s.probes.SetDraining()
	if s.cfg.DrainDelay > 0 {
		time.Sleep(s.cfg.DrainDelay)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	var shutdownErrs []error
	if err := s.sshServer.Shutdown(shutdownCtx); err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
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
