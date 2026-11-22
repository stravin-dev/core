package rpc

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// HeaderInternalToken is the gRPC metadata key carrying the shared internal auth token.
	HeaderInternalToken = "x-stravin-internal-token" //nolint:gosec // gRPC metadata header name, not a secret
	// HeaderAuthorization is the standard gRPC metadata key for Bearer token auth.
	HeaderAuthorization = "authorization"
)

// TransportConfig configures authentication and TLS/mTLS for the internal GitWire gRPC boundary.
type TransportConfig struct {
	Token         string
	TLSCertFile   string
	TLSKeyFile    string
	TLSCAFile     string
	TLSServerName string
	AllowInsecure bool
}

// LoadTransportConfigFromEnv reads internal gRPC auth and TLS/mTLS settings from environment variables.
func LoadTransportConfigFromEnv() TransportConfig {
	allowRaw := strings.TrimSpace(os.Getenv("STRAVIN_GRPC_ALLOW_INSECURE"))
	return TransportConfig{
		Token:         strings.TrimSpace(os.Getenv("STRAVIN_INTERNAL_GRPC_TOKEN")),
		TLSCertFile:   strings.TrimSpace(os.Getenv("STRAVIN_GRPC_TLS_CERT")),
		TLSKeyFile:    strings.TrimSpace(os.Getenv("STRAVIN_GRPC_TLS_KEY")),
		TLSCAFile:     strings.TrimSpace(os.Getenv("STRAVIN_GRPC_TLS_CA")),
		TLSServerName: strings.TrimSpace(os.Getenv("STRAVIN_GRPC_TLS_SERVER_NAME")),
		AllowInsecure: strings.EqualFold(allowRaw, "true") || allowRaw == "1",
	}
}

// StreamAuthInterceptor returns a gRPC stream server interceptor that enforces
// constant-time comparison against secret. When secret is empty or whitespace,
// it fails closed and rejects all incoming streams with codes.Unauthenticated.
func StreamAuthInterceptor(secret string) grpc.StreamServerInterceptor {
	expected := strings.TrimSpace(secret)
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if expected == "" {
			return status.Error(codes.Unauthenticated, "rpc: internal auth token not configured")
		}
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "rpc: missing authentication metadata")
		}
		presented := extractPresentedToken(md)
		if presented == "" || subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) != 1 {
			return status.Error(codes.Unauthenticated, "rpc: invalid internal auth token")
		}
		return handler(srv, ss)
	}
}

func extractPresentedToken(md metadata.MD) string {
	if tok := strings.TrimSpace(firstValue(md, HeaderInternalToken)); tok != "" {
		return tok
	}
	authz := strings.TrimSpace(firstValue(md, HeaderAuthorization))
	if len(authz) > 7 && strings.EqualFold(authz[:7], "bearer ") {
		return strings.TrimSpace(authz[7:])
	}
	return ""
}

// ClientStreamAuthInterceptor returns a gRPC stream client interceptor that attaches
// secret to outgoing stream metadata under HeaderInternalToken.
func ClientStreamAuthInterceptor(secret string) grpc.StreamClientInterceptor {
	tok := strings.TrimSpace(secret)
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if tok != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, HeaderInternalToken, tok)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

type tokenPerRPCCredentials struct {
	token      string
	requireTLS bool
}

// NewTokenPerRPCCredentials returns gRPC PerRPCCredentials that attach secret
// under HeaderInternalToken on every RPC.
func NewTokenPerRPCCredentials(secret string, requireTLS bool) credentials.PerRPCCredentials {
	return tokenPerRPCCredentials{
		token:      strings.TrimSpace(secret),
		requireTLS: requireTLS,
	}
}

func (c tokenPerRPCCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if c.token == "" {
		return nil, errors.New("rpc: internal auth token is empty")
	}
	return map[string]string{HeaderInternalToken: c.token}, nil
}

func (c tokenPerRPCCredentials) RequireTransportSecurity() bool {
	return c.requireTLS
}

// LoadServerTLSCredentials loads server TLS credentials from certFile and keyFile.
// When caFile is non-empty, mutual TLS (RequireAndVerifyClientCert) is enabled.
func LoadServerTLSCredentials(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("rpc: load server TLS keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("rpc: load server TLS CA: %w", err)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), nil
}

// LoadClientTLSCredentials loads client TLS credentials. When certFile and keyFile
// are both provided, client certificates are presented for mTLS.
func LoadClientTLSCredentials(certFile, keyFile, caFile, serverName string) (credentials.TransportCredentials, error) {
	if (certFile != "") != (keyFile != "") {
		return nil, errors.New("rpc: both STRAVIN_GRPC_TLS_CERT and STRAVIN_GRPC_TLS_KEY must be set")
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
	}
	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("rpc: load client TLS CA: %w", err)
		}
		tlsCfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("rpc: load client TLS keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no valid PEM certificates in %s", caFile)
	}
	return pool, nil
}

// ServerOptions validates cfg and returns the grpc.ServerOption slice for stravin-node.
// It fails closed unless TLS/mTLS + auth (mTLS or shared secret) are configured,
// or STRAVIN_GRPC_ALLOW_INSECURE=true is explicitly set for local development.
// When Token is non-empty, StreamAuthInterceptor is always enforced even in AllowInsecure mode.
func ServerOptions(cfg TransportConfig) ([]grpc.ServerOption, error) {
	hasCert := cfg.TLSCertFile != ""
	hasKey := cfg.TLSKeyFile != ""
	if hasCert != hasKey {
		return nil, errors.New("rpc: both STRAVIN_GRPC_TLS_CERT and STRAVIN_GRPC_TLS_KEY must be set")
	}
	hasTLS := hasCert && hasKey
	hasMTLS := hasTLS && cfg.TLSCAFile != ""
	token := strings.TrimSpace(cfg.Token)

	var opts []grpc.ServerOption
	if hasTLS {
		creds, err := LoadServerTLSCredentials(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	} else if !cfg.AllowInsecure {
		return nil, errors.New("rpc: server TLS/mTLS (STRAVIN_GRPC_TLS_CERT/STRAVIN_GRPC_TLS_KEY) required unless STRAVIN_GRPC_ALLOW_INSECURE=true")
	}

	if token != "" {
		opts = append(opts, grpc.StreamInterceptor(StreamAuthInterceptor(token)))
	} else if !hasMTLS && !cfg.AllowInsecure {
		return nil, errors.New("rpc: STRAVIN_INTERNAL_GRPC_TOKEN or mTLS (STRAVIN_GRPC_TLS_CA) required unless STRAVIN_GRPC_ALLOW_INSECURE=true")
	}

	return opts, nil
}

// ClientDialOptions validates cfg and returns the grpc.DialOption slice for stravin-edge.
// It fails closed unless TLS credentials are configured or AllowInsecure is explicitly set.
func ClientDialOptions(cfg TransportConfig) ([]grpc.DialOption, error) {
	hasCert := cfg.TLSCertFile != ""
	hasKey := cfg.TLSKeyFile != ""
	if hasCert != hasKey {
		return nil, errors.New("rpc: both STRAVIN_GRPC_TLS_CERT and STRAVIN_GRPC_TLS_KEY must be set")
	}
	hasClientCert := hasCert && hasKey
	hasTLS := hasClientCert || cfg.TLSCAFile != ""
	token := strings.TrimSpace(cfg.Token)

	var opts []grpc.DialOption
	switch {
	case hasTLS:
		creds, err := LoadClientTLSCredentials(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile, cfg.TLSServerName)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	case cfg.AllowInsecure:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	default:
		return nil, errors.New("rpc: client TLS credentials required unless STRAVIN_GRPC_ALLOW_INSECURE=true")
	}

	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(NewTokenPerRPCCredentials(token, hasTLS)))
	} else if !hasClientCert && !cfg.AllowInsecure {
		return nil, errors.New("rpc: STRAVIN_INTERNAL_GRPC_TOKEN or mTLS client cert required unless STRAVIN_GRPC_ALLOW_INSECURE=true")
	}

	return opts, nil
}
