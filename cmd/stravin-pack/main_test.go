package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/compaction"
	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

func TestLoadConfig_CRRReplicationGateDefaultsAndOverrides(t *testing.T) {
	t.Run("disabled_by_default_for_single_region", func(t *testing.T) {
		t.Setenv("STRAVIN_PACK_REQUIRE_CRR_REPLICATION", "")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_THRESHOLD", "")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_COOLDOWN", "")

		cfg := loadConfig()
		if cfg.RequireCRRReplication {
			t.Fatal("RequireCRRReplication must default to false")
		}
		if cfg.CRRBreakerThreshold != compaction.DefaultCRRBreakerThreshold {
			t.Fatalf("CRRBreakerThreshold = %d, want %d", cfg.CRRBreakerThreshold, compaction.DefaultCRRBreakerThreshold)
		}
		if cfg.CRRBreakerCooldown != compaction.DefaultCRRBreakerCooldown {
			t.Fatalf("CRRBreakerCooldown = %v, want %v", cfg.CRRBreakerCooldown, compaction.DefaultCRRBreakerCooldown)
		}
	})

	t.Run("enabled_with_custom_breaker_settings", func(t *testing.T) {
		t.Setenv("STRAVIN_PACK_REQUIRE_CRR_REPLICATION", "true")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_THRESHOLD", "8")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_COOLDOWN", "20m")

		cfg := loadConfig()
		if !cfg.RequireCRRReplication {
			t.Fatal("expected RequireCRRReplication = true")
		}
		if cfg.CRRBreakerThreshold != 8 {
			t.Fatalf("CRRBreakerThreshold = %d, want 8", cfg.CRRBreakerThreshold)
		}
		if cfg.CRRBreakerCooldown != 20*time.Minute {
			t.Fatalf("CRRBreakerCooldown = %v, want 20m", cfg.CRRBreakerCooldown)
		}
	})
}

func TestLoadConfigGitExecDefaults(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "")
	t.Setenv("STRAVIN_GITEXEC_CGROUP_ROOT", "")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "")

	cfg := loadConfig()

	if !cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected stravin-pack default GitExec.Sandbox.EnableNamespaces = true")
	}
	if !cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected stravin-pack default GitExec.Sandbox.EnableCgroup = true")
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

func TestLoadConfigGitExecOverrides(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "false")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "false")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "1073741824")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "150000 100000")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "128")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "15m")

	cfg := loadConfig()

	if cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected EnableNamespaces = false when overridden")
	}
	if cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected EnableCgroup = false when overridden")
	}
	if cfg.GitExec.Sandbox.MemoryMaxBytes != 1073741824 {
		t.Errorf("MemoryMaxBytes = %d, want 1073741824", cfg.GitExec.Sandbox.MemoryMaxBytes)
	}
	if cfg.GitExec.Sandbox.CPUQuota != "150000 100000" {
		t.Errorf("CPUQuota = %q, want %q", cfg.GitExec.Sandbox.CPUQuota, "150000 100000")
	}
	if cfg.GitExec.Sandbox.PidsMax != 128 {
		t.Errorf("PidsMax = %d, want 128", cfg.GitExec.Sandbox.PidsMax)
	}
	if cfg.GitExec.DefaultTimeout != 15*time.Minute {
		t.Errorf("DefaultTimeout = %v, want 15m", cfg.GitExec.DefaultTimeout)
	}
}

func TestResolveGitExecConfigZeroValueUsesSecureDefaults(t *testing.T) {
	resolved := resolveGitExecConfig(gitexec.Config{})

	if !resolved.Sandbox.EnableNamespaces || !resolved.Sandbox.EnableCgroup {
		t.Errorf("expected secure sandbox defaults for zero-value Config, got %+v", resolved.Sandbox)
	}
	if resolved.DefaultTimeout != defaultGitExecTimeout {
		t.Errorf("DefaultTimeout = %v, want %v", resolved.DefaultTimeout, defaultGitExecTimeout)
	}
}

type stubPackS3API struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (s *stubPackS3API) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
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

func (s *stubPackS3API) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	s.mu.Lock()
	b := append([]byte(nil), s.objects[aws.ToString(in.Key)]...)
	s.mu.Unlock()
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (s *stubPackS3API) HeadObject(_ context.Context, _ *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{}, nil
}

func (s *stubPackS3API) DeleteObject(_ context.Context, _ *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	return &awss3.DeleteObjectOutput{}, nil
}

func (s *stubPackS3API) ListObjectsV2(_ context.Context, _ *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{}, nil
}

type stubPackKMSAPI struct {
	dek []byte
}

func (k *stubPackKMSAPI) GenerateDataKey(_ context.Context, _ *kms.GenerateDataKeyInput, _ ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error) {
	return &kms.GenerateDataKeyOutput{
		Plaintext:      append([]byte(nil), k.dek...),
		CiphertextBlob: []byte("pack-wrapped-dek"),
	}, nil
}

func (k *stubPackKMSAPI) Decrypt(_ context.Context, _ *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	return &kms.DecryptOutput{Plaintext: append([]byte(nil), k.dek...)}, nil
}

func TestLoadConfig_KMSKeyID(t *testing.T) {
	t.Setenv("STRAVIN_KMS_KEY_ID", "")
	if cfg := loadConfig(); cfg.KMSKeyID != "" {
		t.Fatalf("KMSKeyID = %q, want empty", cfg.KMSKeyID)
	}

	t.Setenv("STRAVIN_KMS_KEY_ID", "alias/stravin-local-dev")
	if cfg := loadConfig(); cfg.KMSKeyID != "alias/stravin-local-dev" {
		t.Fatalf("KMSKeyID = %q, want %q", cfg.KMSKeyID, "alias/stravin-local-dev")
	}
}

func TestBuildS3Store_KMSEnvelopeWiringAndFailClosed(t *testing.T) {
	t.Run("wires_kms_cipher_when_key_set", func(t *testing.T) {
		s3API := &stubPackS3API{}
		kmsAPI := &stubPackKMSAPI{dek: bytes.Repeat([]byte{0x22}, 32)}
		store, err := buildS3Store(s3API, "git-objects-us-east-1", kmsAPI, "alias/stravin-local-dev")
		if err != nil {
			t.Fatalf("buildS3Store: %v", err)
		}

		rk, err := tenant.NewRepoKey("acme", "core")
		if err != nil {
			t.Fatalf("NewRepoKey: %v", err)
		}
		packSHA := strings.Repeat("b", 64)
		plain := []byte("PACK\x00\x00\x00\x02compacted-pack")
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
		if _, err := buildS3Store(&stubPackS3API{}, "git-objects-us-east-1", nil, "alias/stravin-local-dev"); !errors.Is(err, crypto.ErrInvalidInput) {
			t.Fatalf("nil kms client err = %v, want crypto.ErrInvalidInput", err)
		}
		kmsAPI := &stubPackKMSAPI{dek: bytes.Repeat([]byte{0x22}, 32)}
		if _, err := buildS3Store(&stubPackS3API{}, "git-objects-us-east-1", kmsAPI, "   "); !errors.Is(err, crypto.ErrInvalidInput) {
			t.Fatalf("whitespace kms key err = %v, want crypto.ErrInvalidInput", err)
		}
	})
}

func TestRun_FailClosedOnInvalidKMSKeyID(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	err := run(context.Background(), packConfig{
		HealthAddr: "127.0.0.1:0",
		Region:     "us-east-1",
		DDBTable:   "stravin-core",
		S3Bucket:   "git-objects-us-east-1",
		KMSKeyID:   "   ",
	})
	if !errors.Is(err, crypto.ErrInvalidInput) {
		t.Fatalf("run(whitespace KMSKeyID) err = %v, want crypto.ErrInvalidInput", err)
	}
}
