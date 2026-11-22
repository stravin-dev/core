package node

import (
	"context"
	"strings"
	"testing"
)

func TestRunDaemon_FailClosedWithoutGRPCAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := RunDaemon(ctx, DaemonConfig{
		Region:     "us-east-1",
		GRPCAddr:   "127.0.0.1:0",
		HealthAddr: "127.0.0.1:0",
		CacheDir:   t.TempDir(),
	})
	if err == nil {
		t.Fatal("RunDaemon with empty GRPCTransport expected error, got nil")
	}
	if !strings.Contains(err.Error(), "grpc") {
		t.Fatalf("expected grpc transport config error, got: %v", err)
	}
}

func TestLoadDaemonConfigFromEnv_GRPCTransport(t *testing.T) {
	t.Setenv("STRAVIN_INTERNAL_GRPC_TOKEN", "dev-secret")
	t.Setenv("STRAVIN_GRPC_TLS_CERT", "/etc/tls/node.crt")
	t.Setenv("STRAVIN_GRPC_TLS_KEY", "/etc/tls/node.key")
	t.Setenv("STRAVIN_GRPC_TLS_CA", "/etc/tls/ca.crt")
	t.Setenv("STRAVIN_GRPC_ALLOW_INSECURE", "true")

	cfg := LoadDaemonConfigFromEnv()
	if cfg.GRPCTransport.Token != "dev-secret" {
		t.Fatalf("Token = %q, want %q", cfg.GRPCTransport.Token, "dev-secret")
	}
	if cfg.GRPCTransport.TLSCertFile != "/etc/tls/node.crt" {
		t.Fatalf("TLSCertFile = %q, want %q", cfg.GRPCTransport.TLSCertFile, "/etc/tls/node.crt")
	}
	if cfg.GRPCTransport.TLSKeyFile != "/etc/tls/node.key" {
		t.Fatalf("TLSKeyFile = %q, want %q", cfg.GRPCTransport.TLSKeyFile, "/etc/tls/node.key")
	}
	if cfg.GRPCTransport.TLSCAFile != "/etc/tls/ca.crt" {
		t.Fatalf("TLSCAFile = %q, want %q", cfg.GRPCTransport.TLSCAFile, "/etc/tls/ca.crt")
	}
	if !cfg.GRPCTransport.AllowInsecure {
		t.Fatal("AllowInsecure = false, want true")
	}
}
