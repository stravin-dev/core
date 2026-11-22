package edge

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	sshExtTenantID = "tenant_id"
	sshExtActorID  = "actor_id"
)

// LoadOrGenerateHostKey loads a PEM-encoded SSH host private key from path, or
// generates an ephemeral ed25519 host key when path is empty (for local dev/tests).
func LoadOrGenerateHostKey(path string) (ssh.Signer, error) {
	if strings.TrimSpace(path) != "" {
		raw, err := os.ReadFile(path) //#nosec G304 -- path comes from operator daemon configuration
		if err != nil {
			return nil, fmt.Errorf("edge: read ssh host key: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(raw)
		if err != nil {
			return nil, fmt.Errorf("edge: parse ssh host key: %w", err)
		}
		return signer, nil
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("edge: generate ephemeral ed25519 host key: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, fmt.Errorf("edge: create ssh signer: %w", err)
	}
	return signer, nil
}

// ParseSSHExecCommand parses and validates a Git SSH exec command of the form:
//
//	git-upload-pack '/{tenant}/{repo}.git'
//	git-receive-pack '/{tenant}/{repo}.git'
//
// It strictly rejects shell metacharacters, path traversal, extra arguments, and invalid IDs.
func ParseSSHExecCommand(cmd string) (rpc.Operation, tenant.RepoKey, error) {
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return "", tenant.RepoKey{}, errors.New("edge: empty ssh exec command")
	}
	if strings.ContainsAny(cmd, ";|&`$()<>\n\r\x00\\") {
		return "", tenant.RepoKey{}, errors.New("edge: illegal characters in ssh command")
	}

	idx := strings.IndexByte(cmd, ' ')
	if idx <= 0 {
		return "", tenant.RepoKey{}, errors.New("edge: malformed ssh exec command")
	}
	op := rpc.Operation(cmd[:idx])
	if err := op.Validate(); err != nil {
		return "", tenant.RepoKey{}, err
	}

	arg := strings.TrimSpace(cmd[idx+1:])
	if len(arg) >= 2 {
		if (arg[0] == '\'' && arg[len(arg)-1] == '\'') || (arg[0] == '"' && arg[len(arg)-1] == '"') {
			arg = arg[1 : len(arg)-1]
		}
	}
	if strings.ContainsAny(arg, " \t'\"") || strings.Contains(arg, "..") {
		return "", tenant.RepoKey{}, errors.New("edge: invalid repository argument")
	}

	arg = strings.TrimPrefix(arg, "/")
	parts := strings.Split(arg, "/")
	if len(parts) != 2 {
		return "", tenant.RepoKey{}, errors.New("edge: repository path must be /{tenant}/{repo}.git")
	}

	repoRaw := strings.TrimSuffix(parts[1], ".git")
	repoKey, err := tenant.NewRepoKey(parts[0], repoRaw)
	if err != nil {
		return "", tenant.RepoKey{}, err
	}
	return op, repoKey, nil
}

// SSHServer terminates Git SSH connections on :2222 and streams git-upload-pack /
// git-receive-pack sessions to stravin-node via WireProxy.
type SSHServer struct {
	sshCfg   *ssh.ServerConfig
	auth     Authenticator
	limiter  Limiter
	resolver *Resolver
	router   NodeSelector
	proxy    *WireProxy
	metrics  *Metrics
	logger   *slog.Logger

	mu       sync.Mutex
	listener net.Listener
	closed   bool
	wg       sync.WaitGroup
}

// NewSSHServer constructs an SSHServer backed by hostKey and the provided collaborators.
func NewSSHServer(
	hostKey ssh.Signer,
	auth Authenticator,
	limiter Limiter,
	resolver *Resolver,
	router NodeSelector,
	proxy *WireProxy,
	metrics *Metrics,
	logger *slog.Logger,
) (*SSHServer, error) {
	if hostKey == nil {
		return nil, errors.New("edge: ssh host key is required")
	}
	if auth == nil {
		return nil, errors.New("edge: authenticator is required")
	}
	if resolver == nil {
		return nil, errors.New("edge: resolver is required")
	}
	if router == nil {
		return nil, errors.New("edge: node selector is required")
	}
	if proxy == nil {
		return nil, errors.New("edge: wire proxy is required")
	}
	if metrics == nil {
		var err error
		metrics, err = NewMetrics(nil)
		if err != nil {
			return nil, err
		}
	}
	if logger == nil {
		logger = slog.Default()
	}

	s := &SSHServer{
		auth:     auth,
		limiter:  limiter,
		resolver: resolver,
		router:   router,
		proxy:    proxy,
		metrics:  metrics,
		logger:   logger,
	}

	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			principal, err := s.auth.AuthenticateSSHKey(context.Background(), key)
			if err != nil {
				return nil, ErrUnauthorized
			}
			return &ssh.Permissions{
				Extensions: map[string]string{
					sshExtTenantID: string(principal.TenantID),
					sshExtActorID:  principal.ActorID,
				},
			}, nil
		},
	}
	cfg.AddHostKey(hostKey)
	s.sshCfg = cfg
	return s, nil
}

// Serve accepts incoming SSH connections on ln until Close is called or ln fails.
func (s *SSHServer) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConn(c)
		}(conn)
	}
}

// Close stops accepting new SSH connections and waits for active handlers to drain.
func (s *SSHServer) Close() error {
	s.mu.Lock()
	s.closed = true
	ln := s.listener
	s.mu.Unlock()

	var err error
	if ln != nil {
		err = ln.Close()
	}
	s.wg.Wait()
	return err
}

func (s *SSHServer) handleConn(rawConn net.Conn) {
	serverConn, chans, reqs, err := ssh.NewServerConn(rawConn, s.sshCfg)
	if err != nil {
		_ = rawConn.Close()
		return
	}
	defer func() { _ = serverConn.Close() }()

	go ssh.DiscardRequests(reqs)

	principal := principalFromSSHPerms(serverConn.Permissions)
	for newChan := range chans {
		if newChan.ChannelType() != "session" {
			_ = newChan.Reject(ssh.UnknownChannelType, "only session channels are supported")
			continue
		}
		ch, requests, err := newChan.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func(channel ssh.Channel, in <-chan *ssh.Request) {
			defer s.wg.Done()
			defer func() { _ = channel.Close() }()
			s.handleSession(principal, channel, in)
		}(ch, requests)
	}
}

func (s *SSHServer) handleSession(principal Principal, ch ssh.Channel, requests <-chan *ssh.Request) {
	for req := range requests {
		switch req.Type {
		case "exec":
			cmd, err := decodeSSHExecPayload(req.Payload)
			if err != nil {
				_ = req.Reply(false, nil)
				return
			}
			_ = req.Reply(true, nil)
			code := s.executeGitCommand(principal, cmd, ch)
			sendSSHExitStatus(ch, code)
			return
		default:
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
}

func (s *SSHServer) executeGitCommand(principal Principal, cmd string, ch ssh.Channel) uint32 {
	start := time.Now()
	op, repoKey, err := ParseSSHExecCommand(cmd)
	if err != nil {
		s.metrics.RecordRequest(context.Background(), "ssh", "unknown", "error", "invalid_request", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: invalid git command or repository path\n")
		return 1
	}

	metricOp := "fetch"
	if op == rpc.OpReceivePack {
		metricOp = "push"
	}

	ctx, span := s.metrics.StartIngressSpan(context.Background(), "ssh", metricOp, string(repoKey.TenantID), string(repoKey.RepoID), "")
	defer span.End()

	if err := AuthorizeTenant(principal, repoKey.TenantID); err != nil {
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "forbidden", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: permission denied\n")
		return 1
	}

	if s.limiter != nil {
		if err := s.limiter.AllowRequest(ctx, repoKey.TenantID); err != nil {
			s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "rate_limited", time.Since(start))
			_, _ = io.WriteString(ch.Stderr(), "fatal: rate limit exceeded\n")
			return 1
		}
		if op == rpc.OpUploadPack {
			release, err := s.limiter.AcquireClone(ctx, repoKey.TenantID)
			if err != nil {
				s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "rate_limited", time.Since(start))
				_, _ = io.WriteString(ch.Stderr(), "fatal: concurrent clone limit exceeded\n")
				return 1
			}
			defer release()
		}
	}

	var homeRegion string
	if op == rpc.OpReceivePack {
		dec, err := s.resolver.ResolveWrite(ctx, repoKey)
		if err != nil {
			s.writeSSHResolveError(ctx, ch, metricOp, err, start)
			return 1
		}
		homeRegion = string(dec.HomeRegion)
	} else {
		meta, err := s.resolver.ResolveRead(ctx, repoKey)
		if err != nil {
			s.writeSSHResolveError(ctx, ch, metricOp, err, start)
			return 1
		}
		homeRegion = string(meta.HomeRegion)
	}
	span.SetAttributes(telemetry.AttrHomeRegion.String(homeRegion))

	peer, err := s.router.SelectNode(repoKey)
	if err != nil {
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "upstream", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: service unavailable\n")
		return 1
	}

	rpcMeta := rpc.RequestMeta{
		TenantID:    repoKey.TenantID,
		RepoID:      repoKey.RepoID,
		Operation:   op,
		TraceParent: telemetry.TraceparentFromContext(ctx),
	}

	stdin := NewByteLimitingReader(ctx, ch, s.limiter, repoKey.TenantID)
	exitCode, err := s.proxy.ProxyStream(ctx, peer, rpcMeta, stdin, ch, ch.Stderr())
	if err != nil {
		if errors.Is(err, ErrRateLimited) {
			s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "rate_limited", time.Since(start))
			_, _ = io.WriteString(ch.Stderr(), "fatal: pack bandwidth limit exceeded\n")
			return 1
		}
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "upstream", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: upstream git service error\n")
		return 1
	}

	if exitCode != 0 {
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "upstream", time.Since(start))
		return uint32(exitCode)
	}

	s.metrics.RecordRequest(ctx, "ssh", metricOp, "ok", "", time.Since(start))
	return 0
}

func (s *SSHServer) writeSSHResolveError(ctx context.Context, ch ssh.Channel, metricOp string, err error, start time.Time) {
	switch {
	case errors.Is(err, ErrRepoNotFound):
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "not_found", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: repository not found\n")
	case errors.Is(err, ErrForbidden):
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "forbidden", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: permission denied\n")
	case errors.Is(err, ErrWriteUnavailable):
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "lease_unavailable", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: repository writes temporarily unavailable\n")
	default:
		s.metrics.RecordRequest(ctx, "ssh", metricOp, "error", "upstream", time.Since(start))
		_, _ = io.WriteString(ch.Stderr(), "fatal: service unavailable\n")
	}
}

func principalFromSSHPerms(perms *ssh.Permissions) Principal {
	if perms == nil || perms.Extensions == nil {
		return Principal{}
	}
	return Principal{
		TenantID: tenant.TenantID(perms.Extensions[sshExtTenantID]),
		ActorID:  perms.Extensions[sshExtActorID],
	}
}

func decodeSSHExecPayload(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", errors.New("edge: truncated ssh exec payload")
	}
	n := int(binary.BigEndian.Uint32(payload[:4]))
	if n < 0 || 4+n > len(payload) {
		return "", errors.New("edge: invalid ssh exec payload length")
	}
	return string(payload[4 : 4+n]), nil
}

func sendSSHExitStatus(ch ssh.Channel, code uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], code)
	_, _ = ch.SendRequest("exit-status", false, buf[:])
}
