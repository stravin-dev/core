package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

func TestLoadConfig_DefaultsAndOverrides(t *testing.T) {
	t.Setenv("STRAVIN_CTRL_ADDR", "")
	t.Setenv("STRAVIN_CTRL_LISTEN_ADDR", "")
	t.Setenv("STRAVIN_HEALTH_ADDR", "")
	t.Setenv("STRAVIN_CTRL_ADMIN_TOKEN", "")

	if defaultListenAddr != "127.0.0.1:8083" {
		t.Fatalf("defaultListenAddr = %q, want %q", defaultListenAddr, "127.0.0.1:8083")
	}

	cfg := loadConfig()
	if cfg.ListenAddr != "127.0.0.1:8083" {
		t.Fatalf("default ListenAddr = %q, want %q", cfg.ListenAddr, "127.0.0.1:8083")
	}
	if cfg.AdminToken != "" {
		t.Fatalf("default AdminToken = %q, want empty", cfg.AdminToken)
	}

	t.Setenv("STRAVIN_CTRL_LISTEN_ADDR", ":8083")
	t.Setenv("STRAVIN_CTRL_ADMIN_TOKEN", "  secret-token  ")
	cfg = loadConfig()
	if cfg.ListenAddr != ":8083" {
		t.Fatalf("STRAVIN_CTRL_LISTEN_ADDR ListenAddr = %q, want %q", cfg.ListenAddr, ":8083")
	}
	if cfg.AdminToken != "secret-token" {
		t.Fatalf("AdminToken = %q, want %q", cfg.AdminToken, "secret-token")
	}

	t.Setenv("STRAVIN_CTRL_ADDR", "127.0.0.1:9083")
	cfg = loadConfig()
	if cfg.ListenAddr != "127.0.0.1:9083" {
		t.Fatalf("STRAVIN_CTRL_ADDR ListenAddr = %q, want %q", cfg.ListenAddr, "127.0.0.1:9083")
	}
	if cfg.RequirePromotionGuard {
		t.Fatalf("default RequirePromotionGuard = true, want false")
	}
}

func TestBuildPromotionGuard(t *testing.T) {
	t.Run("disabled returns nil guard", func(t *testing.T) {
		g, err := buildPromotionGuard(ctrlConfig{RequirePromotionGuard: false})
		if err != nil || g != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", g, err)
		}
	})

	t.Run("enabled without operator token or witness endpoints fails closed", func(t *testing.T) {
		_, err := buildPromotionGuard(ctrlConfig{RequirePromotionGuard: true})
		if err == nil {
			t.Fatal("expected error when RequirePromotionGuard=true with no guards configured")
		}
	})

	t.Run("enabled with operator token and witness endpoints builds composite guard", func(t *testing.T) {
		g, err := buildPromotionGuard(ctrlConfig{
			RequirePromotionGuard:  true,
			PromotionGuardPolicy:   "require_all",
			PromotionApprovalToken: "op-token",
			WitnessEndpoints:       []string{"http://w1.internal/witness", "http://w2.internal/witness"},
			WitnessQuorum:          2,
		})
		if err != nil || g == nil {
			t.Fatalf("got (%v, %v), want non-nil guard and nil error", g, err)
		}
	})
}

func TestRun_RequiresAdminToken(t *testing.T) {
	err := run(context.Background(), ctrlConfig{
		ListenAddr: "127.0.0.1:0",
		Region:     "us-east-1",
	})
	if err == nil {
		t.Fatal("expected run() to fail when AdminToken is empty, got nil")
	}
	if !strings.Contains(err.Error(), "STRAVIN_CTRL_ADMIN_TOKEN") {
		t.Fatalf("unexpected error = %q, want mention of STRAVIN_CTRL_ADMIN_TOKEN", err.Error())
	}
}

func TestRecoveryBootHelpers(t *testing.T) {
	if !shouldRunRecoveryBoot(lease.ModeDraining) || !shouldRunRecoveryBoot(lease.ModeReadOnly) {
		t.Fatal("expected recovery boot enabled for DRAINING and READ_ONLY modes")
	}
	if shouldRunRecoveryBoot(lease.ModeActive) {
		t.Fatal("expected recovery boot disabled for ACTIVE mode")
	}

	rk, err := parseRecoveryRepoKey("acme/core")
	if err != nil || rk.TenantID != "acme" || rk.RepoID != "core" {
		t.Fatalf("parseRecoveryRepoKey(acme/core) = (%+v, %v)", rk, err)
	}
	if _, err := parseRecoveryRepoKey("missing-slash"); err == nil {
		t.Fatal("expected error for malformed recovery repo key")
	}
}

type stubCtrlS3API struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (s *stubCtrlS3API) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
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

func (s *stubCtrlS3API) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	s.mu.Lock()
	b := append([]byte(nil), s.objects[aws.ToString(in.Key)]...)
	s.mu.Unlock()
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (s *stubCtrlS3API) HeadObject(_ context.Context, _ *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{}, nil
}

func (s *stubCtrlS3API) DeleteObject(_ context.Context, _ *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	return &awss3.DeleteObjectOutput{}, nil
}

func (s *stubCtrlS3API) ListObjectsV2(_ context.Context, _ *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{}, nil
}

type stubCtrlKMSAPI struct {
	dek []byte
}

func (k *stubCtrlKMSAPI) GenerateDataKey(_ context.Context, _ *kms.GenerateDataKeyInput, _ ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error) {
	return &kms.GenerateDataKeyOutput{
		Plaintext:      append([]byte(nil), k.dek...),
		CiphertextBlob: []byte("ctrl-wrapped-dek"),
	}, nil
}

func (k *stubCtrlKMSAPI) Decrypt(_ context.Context, _ *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
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
		s3API := &stubCtrlS3API{}
		kmsAPI := &stubCtrlKMSAPI{dek: bytes.Repeat([]byte{0x33}, 32)}
		store, err := buildS3Store(s3API, "git-objects-us-east-1", kmsAPI, "alias/stravin-local-dev")
		if err != nil {
			t.Fatalf("buildS3Store: %v", err)
		}

		rk, err := tenant.NewRepoKey("acme", "core")
		if err != nil {
			t.Fatalf("NewRepoKey: %v", err)
		}
		packSHA := strings.Repeat("c", 64)
		plain := []byte("PACK\x00\x00\x00\x02ctrl-scrub-pack")
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
		if _, err := buildS3Store(&stubCtrlS3API{}, "git-objects-us-east-1", nil, "alias/stravin-local-dev"); !errors.Is(err, crypto.ErrInvalidInput) {
			t.Fatalf("nil kms client err = %v, want crypto.ErrInvalidInput", err)
		}
		kmsAPI := &stubCtrlKMSAPI{dek: bytes.Repeat([]byte{0x33}, 32)}
		if _, err := buildS3Store(&stubCtrlS3API{}, "git-objects-us-east-1", kmsAPI, "   "); !errors.Is(err, crypto.ErrInvalidInput) {
			t.Fatalf("whitespace kms key err = %v, want crypto.ErrInvalidInput", err)
		}
	})
}

func TestRun_FailClosedOnInvalidKMSKeyID(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	err := run(context.Background(), ctrlConfig{
		ListenAddr: "127.0.0.1:0",
		AdminToken: "test-admin-token",
		Region:     "us-east-1",
		DDBTable:   "stravin-core",
		S3Bucket:   "git-objects-us-east-1",
		KMSKeyID:   "   ",
	})
	if !errors.Is(err, crypto.ErrInvalidInput) {
		t.Fatalf("run(whitespace KMSKeyID) err = %v, want crypto.ErrInvalidInput", err)
	}
}
