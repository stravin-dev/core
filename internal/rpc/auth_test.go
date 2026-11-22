package rpc_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"stravin.dev/core/internal/rpc"
)

func startAuthTestServer(t *testing.T, opts ...grpc.ServerOption) (func(context.Context, string) (net.Conn, error), *echoHandler) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer(opts...)
	handler := &echoHandler{}
	rpc.RegisterGitWireServer(srv, handler)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	return dialer, handler
}

func roundTripEcho(ctx context.Context, client rpc.GitWireClient) (string, error) {
	stream, err := client.OpenStream(ctx, rpc.RequestMeta{
		TenantID:  "acme",
		RepoID:    "core",
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		return "", err
	}
	if err := stream.Send(&rpc.Frame{Data: []byte("ping"), EOF: true}); err != nil {
		// Server may have already closed stream with auth error; read Recv to surface gRPC status.
		_, recvErr := stream.Recv()
		if recvErr != nil {
			return "", recvErr
		}
		return "", err
	}
	var out bytes.Buffer
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		out.Write(frame.Data)
		if frame.EOF {
			break
		}
	}
	return out.String(), nil
}

func TestStreamAuthInterceptor_RejectsUnauthenticated(t *testing.T) {
	cases := []struct {
		name         string
		serverSecret string
		clientOpts   []grpc.DialOption
		ctxMod       func(context.Context) context.Context
	}{
		{
			name:         "missing token",
			serverSecret: "top-secret-token",
		},
		{
			name:         "wrong token via PerRPCCredentials",
			serverSecret: "top-secret-token",
			clientOpts: []grpc.DialOption{
				grpc.WithPerRPCCredentials(rpc.NewTokenPerRPCCredentials("wrong-token", false)),
			},
		},
		{
			name:         "wrong bearer token in metadata",
			serverSecret: "top-secret-token",
			ctxMod: func(ctx context.Context) context.Context {
				return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer wrong-token")
			},
		},
		{
			name:         "empty server secret fails closed",
			serverSecret: "",
			clientOpts: []grpc.DialOption{
				grpc.WithPerRPCCredentials(rpc.NewTokenPerRPCCredentials("any-token", false)),
			},
		},
		{
			name:         "whitespace server secret fails closed",
			serverSecret: "   ",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialer, _ := startAuthTestServer(t, grpc.StreamInterceptor(rpc.StreamAuthInterceptor(tc.serverSecret)))
			opts := append([]grpc.DialOption{
				grpc.WithContextDialer(dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.WireCodec())),
			}, tc.clientOpts...)

			cc, err := grpc.NewClient("passthrough:///bufnet", opts...)
			if err != nil {
				t.Fatalf("grpc.NewClient: %v", err)
			}
			t.Cleanup(func() { _ = cc.Close() })

			ctx := context.Background()
			if tc.ctxMod != nil {
				ctx = tc.ctxMod(ctx)
			}
			_, err = roundTripEcho(ctx, rpc.NewGRPCGitWireClient(cc))
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("status.Code(err) = %v (err=%v), want Unauthenticated", status.Code(err), err)
			}
		})
	}
}

func TestStreamAuthInterceptor_AllowsValidToken(t *testing.T) {
	const secret = "super-secret-internal-token"
	dialer, _ := startAuthTestServer(t, grpc.StreamInterceptor(rpc.StreamAuthInterceptor(secret)))

	t.Run("PerRPCCredentials", func(t *testing.T) {
		cc, err := grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(rpc.NewTokenPerRPCCredentials(secret, false)),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.WireCodec())),
		)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		got, err := roundTripEcho(context.Background(), rpc.NewGRPCGitWireClient(cc))
		if err != nil {
			t.Fatalf("roundTripEcho: %v", err)
		}
		if got != "ack:ping" {
			t.Fatalf("got %q, want %q", got, "ack:ping")
		}
	})

	t.Run("ClientStreamAuthInterceptor", func(t *testing.T) {
		cc, err := grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStreamInterceptor(rpc.ClientStreamAuthInterceptor(secret)),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.WireCodec())),
		)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		got, err := roundTripEcho(context.Background(), rpc.NewGRPCGitWireClient(cc))
		if err != nil {
			t.Fatalf("roundTripEcho: %v", err)
		}
		if got != "ack:ping" {
			t.Fatalf("got %q, want %q", got, "ack:ping")
		}
	})

	t.Run("AuthorizationBearerHeader", func(t *testing.T) {
		cc, err := grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.WireCodec())),
		)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+secret)
		got, err := roundTripEcho(ctx, rpc.NewGRPCGitWireClient(cc))
		if err != nil {
			t.Fatalf("roundTripEcho: %v", err)
		}
		if got != "ack:ping" {
			t.Fatalf("got %q, want %q", got, "ack:ping")
		}
	})
}

func TestTransportOptions_FailClosed(t *testing.T) {
	if _, err := rpc.ServerOptions(rpc.TransportConfig{}); err == nil {
		t.Fatal("ServerOptions(empty) expected error, got nil")
	}
	if _, err := rpc.ServerOptions(rpc.TransportConfig{Token: "only-token"}); err == nil {
		t.Fatal("ServerOptions(token without TLS or AllowInsecure) expected error, got nil")
	}
	if _, err := rpc.ServerOptions(rpc.TransportConfig{TLSCertFile: "/tmp/cert.pem"}); err == nil {
		t.Fatal("ServerOptions(cert without key) expected error, got nil")
	}
	if _, err := rpc.ClientDialOptions(rpc.TransportConfig{}); err == nil {
		t.Fatal("ClientDialOptions(empty) expected error, got nil")
	}
	if _, err := rpc.ClientDialOptions(rpc.TransportConfig{Token: "only-token"}); err == nil {
		t.Fatal("ClientDialOptions(token without TLS or AllowInsecure) expected error, got nil")
	}
	if _, err := rpc.ClientDialOptions(rpc.TransportConfig{TLSCertFile: "/tmp/cert.pem"}); err == nil {
		t.Fatal("ClientDialOptions(cert without key) expected error, got nil")
	}
}

type testPKI struct {
	caFile         string
	serverCertFile string
	serverKeyFile  string
	clientCertFile string
	clientKeyFile  string
}

func generateTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "stravin-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	caFile := writePEMFile(t, dir, "ca.pem", "CERTIFICATE", caDER)

	issue := func(name string, serial int64, extUsage x509.ExtKeyUsage) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: name},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{extUsage},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create %s cert: %v", name, err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("marshal %s key: %v", name, err)
		}
		return writePEMFile(t, dir, name+".crt", "CERTIFICATE", der),
			writePEMFile(t, dir, name+".key", "EC PRIVATE KEY", keyDER)
	}

	srvCert, srvKey := issue("stravin-node", 2, x509.ExtKeyUsageServerAuth)
	cliCert, cliKey := issue("stravin-edge", 3, x509.ExtKeyUsageClientAuth)

	return testPKI{
		caFile:         caFile,
		serverCertFile: srvCert,
		serverKeyFile:  srvKey,
		clientCertFile: cliCert,
		clientKeyFile:  cliKey,
	}
}

func writePEMFile(t *testing.T, dir, name, pemType string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data := pem.EncodeToMemory(&pem.Block{Type: pemType, Bytes: der})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestTLSMTLS_RoundTrip(t *testing.T) {
	pki := generateTestPKI(t)

	srvCfg := rpc.TransportConfig{
		Token:       "mtls-shared-secret",
		TLSCertFile: pki.serverCertFile,
		TLSKeyFile:  pki.serverKeyFile,
		TLSCAFile:   pki.caFile,
	}
	srvOpts, err := rpc.ServerOptions(srvCfg)
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer(srvOpts...)
	rpc.RegisterGitWireServer(srv, &echoHandler{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	t.Run("valid mTLS and token succeeds", func(t *testing.T) {
		cliOpts, err := rpc.ClientDialOptions(rpc.TransportConfig{
			Token:       "mtls-shared-secret",
			TLSCertFile: pki.clientCertFile,
			TLSKeyFile:  pki.clientKeyFile,
			TLSCAFile:   pki.caFile,
		})
		if err != nil {
			t.Fatalf("ClientDialOptions: %v", err)
		}
		cc, err := grpc.NewClient(lis.Addr().String(), cliOpts...)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		got, err := roundTripEcho(context.Background(), rpc.NewGRPCGitWireClient(cc))
		if err != nil {
			t.Fatalf("roundTripEcho: %v", err)
		}
		if got != "ack:ping" {
			t.Fatalf("got %q, want %q", got, "ack:ping")
		}
	})

	t.Run("valid mTLS with wrong token rejected", func(t *testing.T) {
		cliOpts, err := rpc.ClientDialOptions(rpc.TransportConfig{
			Token:       "wrong-secret",
			TLSCertFile: pki.clientCertFile,
			TLSKeyFile:  pki.clientKeyFile,
			TLSCAFile:   pki.caFile,
		})
		if err != nil {
			t.Fatalf("ClientDialOptions: %v", err)
		}
		cc, err := grpc.NewClient(lis.Addr().String(), cliOpts...)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		_, err = roundTripEcho(context.Background(), rpc.NewGRPCGitWireClient(cc))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("status.Code(err) = %v (err=%v), want Unauthenticated", status.Code(err), err)
		}
	})

	t.Run("missing client cert rejected by mTLS handshake", func(t *testing.T) {
		cliOpts, err := rpc.ClientDialOptions(rpc.TransportConfig{
			Token:     "mtls-shared-secret",
			TLSCAFile: pki.caFile,
		})
		if err != nil {
			t.Fatalf("ClientDialOptions: %v", err)
		}
		cc, err := grpc.NewClient(lis.Addr().String(), cliOpts...)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := roundTripEcho(ctx, rpc.NewGRPCGitWireClient(cc)); err == nil {
			t.Fatal("expected TLS handshake failure without client certificate, got nil")
		}
	})
}
