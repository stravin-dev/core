package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
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

func TestLoadDaemonConfigFromEnvGitExecDefaults(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "")
	t.Setenv("STRAVIN_GITEXEC_CGROUP_ROOT", "")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "")

	cfg := LoadDaemonConfigFromEnv()

	if !cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected stravin-node default GitExec.Sandbox.EnableNamespaces = true")
	}
	if !cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected stravin-node default GitExec.Sandbox.EnableCgroup = true")
	}
	if cfg.GitExec.Sandbox.MemoryMaxBytes <= 0 {
		t.Errorf("expected positive default MemoryMaxBytes, got %d", cfg.GitExec.Sandbox.MemoryMaxBytes)
	}
	if cfg.GitExec.Sandbox.CPUQuota == "" {
		t.Error("expected non-empty default CPUQuota")
	}
	if cfg.GitExec.Sandbox.PidsMax <= 0 {
		t.Errorf("expected positive default PidsMax, got %d", cfg.GitExec.Sandbox.PidsMax)
	}
	if cfg.GitExec.DefaultTimeout != defaultGitExecTimeout {
		t.Errorf("DefaultTimeout = %v, want %v", cfg.GitExec.DefaultTimeout, defaultGitExecTimeout)
	}
}

func TestLoadDaemonConfigFromEnvGitExecOverrides(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "false")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "false")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "268435456")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "100000 100000")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "32")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "90s")

	cfg := LoadDaemonConfigFromEnv()

	if cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected EnableNamespaces = false when overridden")
	}
	if cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected EnableCgroup = false when overridden")
	}
	if cfg.GitExec.Sandbox.MemoryMaxBytes != 268435456 {
		t.Errorf("MemoryMaxBytes = %d, want 268435456", cfg.GitExec.Sandbox.MemoryMaxBytes)
	}
	if cfg.GitExec.Sandbox.CPUQuota != "100000 100000" {
		t.Errorf("CPUQuota = %q, want %q", cfg.GitExec.Sandbox.CPUQuota, "100000 100000")
	}
	if cfg.GitExec.Sandbox.PidsMax != 32 {
		t.Errorf("PidsMax = %d, want 32", cfg.GitExec.Sandbox.PidsMax)
	}
	if cfg.GitExec.DefaultTimeout != 90*time.Second {
		t.Errorf("DefaultTimeout = %v, want 90s", cfg.GitExec.DefaultTimeout)
	}
}

func TestResolveGitExecConfigZeroValueUsesSecureDefaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	resolved := resolveGitExecConfig(gitexec.Config{}, logger)

	if !resolved.Sandbox.EnableNamespaces || !resolved.Sandbox.EnableCgroup {
		t.Errorf("expected secure sandbox defaults for zero-value Config, got %+v", resolved.Sandbox)
	}
	if resolved.DefaultTimeout != defaultGitExecTimeout {
		t.Errorf("DefaultTimeout = %v, want %v", resolved.DefaultTimeout, defaultGitExecTimeout)
	}
	if resolved.Logger != logger {
		t.Error("expected logger to be wired into resolved gitexec.Config")
	}
}

type stubS3API struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (s *stubS3API) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[aws.ToString(in.Key)] = b
	return &awss3.PutObjectOutput{}, nil
}

func (s *stubS3API) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	s.mu.Lock()
	b := append([]byte(nil), s.objects[aws.ToString(in.Key)]...)
	s.mu.Unlock()
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (s *stubS3API) HeadObject(_ context.Context, _ *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{}, nil
}

func (s *stubS3API) DeleteObject(_ context.Context, _ *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	return &awss3.DeleteObjectOutput{}, nil
}

func (s *stubS3API) ListObjectsV2(_ context.Context, _ *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{}, nil
}

type stubKMSAPI struct {
	dek []byte
}

func (k *stubKMSAPI) GenerateDataKey(_ context.Context, _ *kms.GenerateDataKeyInput, _ ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error) {
	return &kms.GenerateDataKeyOutput{
		Plaintext:      append([]byte(nil), k.dek...),
		CiphertextBlob: []byte("node-wrapped-dek"),
	}, nil
}

func (k *stubKMSAPI) Decrypt(_ context.Context, _ *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	return &kms.DecryptOutput{Plaintext: append([]byte(nil), k.dek...)}, nil
}

func TestLoadDaemonConfigFromEnv_KMSKeyID(t *testing.T) {
	t.Setenv("STRAVIN_KMS_KEY_ID", "")
	if cfg := LoadDaemonConfigFromEnv(); cfg.KMSKeyID != "" {
		t.Fatalf("KMSKeyID = %q, want empty", cfg.KMSKeyID)
	}

	t.Setenv("STRAVIN_KMS_KEY_ID", "alias/stravin-local-dev")
	if cfg := LoadDaemonConfigFromEnv(); cfg.KMSKeyID != "alias/stravin-local-dev" {
		t.Fatalf("KMSKeyID = %q, want %q", cfg.KMSKeyID, "alias/stravin-local-dev")
	}
}

func TestBuildS3Store_KMSEnvelopeWiringAndFailClosed(t *testing.T) {
	t.Run("wires_kms_cipher_when_key_set", func(t *testing.T) {
		s3API := &stubS3API{}
		kmsAPI := &stubKMSAPI{dek: bytes.Repeat([]byte{0x11}, 32)}
		store, err := buildS3Store(s3API, "git-objects-us-east-1", kmsAPI, "alias/stravin-local-dev")
		if err != nil {
			t.Fatalf("buildS3Store: %v", err)
		}

		rk, err := tenant.NewRepoKey("acme", "core")
		if err != nil {
			t.Fatalf("NewRepoKey: %v", err)
		}
		packSHA := strings.Repeat("a", 64)
		plain := []byte("PACK\x00\x00\x00\x02node-secret-pack")
		if err := store.PutPack(context.Background(), rk, packSHA, plain, []byte("IDX")); err != nil {
			t.Fatalf("PutPack: %v", err)
		}
		packKey, _ := s3.PackKey(rk, packSHA)
		raw := s3API.objects[packKey]
		if bytes.HasPrefix(raw, []byte("PACK")) || bytes.Contains(raw, plain) {
			t.Fatal("expected envelope-encrypted pack in S3, found plaintext")
		}
		got, err := store.GetPack(context.Background(), rk, packSHA)
		if err != nil || !bytes.Equal(got, plain) {
			t.Fatalf("GetPack = %q, err=%v", got, err)
		}
	})

	t.Run("fails_closed_on_invalid_kms_config", func(t *testing.T) {
		if _, err := buildS3Store(&stubS3API{}, "git-objects-us-east-1", nil, "alias/stravin-local-dev"); !errors.Is(err, crypto.ErrInvalidInput) {
			t.Fatalf("nil kms client err = %v, want crypto.ErrInvalidInput", err)
		}
		kmsAPI := &stubKMSAPI{dek: bytes.Repeat([]byte{0x11}, 32)}
		if _, err := buildS3Store(&stubS3API{}, "git-objects-us-east-1", kmsAPI, "   "); !errors.Is(err, crypto.ErrInvalidInput) {
			t.Fatalf("whitespace kms key err = %v, want crypto.ErrInvalidInput", err)
		}
	})
}

func TestRunDaemon_FailClosedOnInvalidKMSKeyID(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	err := RunDaemon(context.Background(), DaemonConfig{
		Region:     "us-east-1",
		GRPCAddr:   "127.0.0.1:0",
		HealthAddr: "127.0.0.1:0",
		CacheDir:   t.TempDir(),
		KMSKeyID:   "   ",
		GRPCTransport: rpc.TransportConfig{
			Token:         "test-token",
			AllowInsecure: true,
		},
	})
	if !errors.Is(err, crypto.ErrInvalidInput) {
		t.Fatalf("RunDaemon(whitespace KMSKeyID) err = %v, want crypto.ErrInvalidInput", err)
	}
}

func TestLoadDaemonConfigFromEnv_TelemetryShutdownTimeout(t *testing.T) {
	t.Setenv("STRAVIN_OTEL_SHUTDOWN_TIMEOUT", "")
	if got := LoadDaemonConfigFromEnv().TelemetryShutdownTimeout; got != 5*time.Second {
		t.Fatalf("default TelemetryShutdownTimeout = %v, want 5s", got)
	}

	t.Setenv("STRAVIN_OTEL_SHUTDOWN_TIMEOUT", "11s")
	if got := LoadDaemonConfigFromEnv().TelemetryShutdownTimeout; got != 11*time.Second {
		t.Fatalf("override TelemetryShutdownTimeout = %v, want 11s", got)
	}
}
