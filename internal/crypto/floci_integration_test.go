//go:build integration

package crypto

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"stravin.dev/core/internal/tenant"
)

type countingKMS struct {
	inner    *kms.Client
	genCalls atomic.Int64
	decCalls atomic.Int64
}

func (c *countingKMS) GenerateDataKey(ctx context.Context, in *kms.GenerateDataKeyInput, optFns ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error) {
	c.genCalls.Add(1)
	return c.inner.GenerateDataKey(ctx, in, optFns...)
}

func (c *countingKMS) Decrypt(ctx context.Context, in *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	c.decCalls.Add(1)
	return c.inner.Decrypt(ctx, in, optFns...)
}

func newFlociKMSClient(t *testing.T) (*kms.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci KMS integration test: AWS_ENDPOINT_URL not set")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	keyID := os.Getenv("STRAVIN_KMS_KEY_ID")
	if keyID == "" {
		keyID = "alias/stravin-local-dev"
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	client := kms.NewFromConfig(cfg, func(o *kms.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return client, keyID
}

func TestFlociIntegration_KMSEnvelopeEncryptionAndDEKCache(t *testing.T) {
	kmsClient, keyID := newFlociKMSClient(t)
	ctx := context.Background()

	writerKMS := &countingKMS{inner: kmsClient}
	writer, err := NewKMSEnvelopeCipher(writerKMS, keyID)
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher(writer): %v", err)
	}

	tid := tenant.TenantID("local")
	p1 := []byte("stravin-webhook-signing-secret-1")
	p2 := []byte("stravin-webhook-signing-secret-2")

	ct1, err := writer.Encrypt(ctx, tid, p1)
	if err != nil {
		t.Fatalf("Encrypt #1: %v", err)
	}
	ct2, err := writer.Encrypt(ctx, tid, p2)
	if err != nil {
		t.Fatalf("Encrypt #2: %v", err)
	}
	if got := writerKMS.genCalls.Load(); got != 1 {
		t.Fatalf("GenerateDataKey calls = %d, want 1 (cached DEK)", got)
	}

	// Same instance decrypts from DEK cache without calling KMS Decrypt.
	got1, err := writer.Decrypt(ctx, tid, ct1)
	if err != nil || !bytes.Equal(got1, p1) {
		t.Fatalf("writer.Decrypt(ct1) = %q, err=%v", got1, err)
	}
	if got := writerKMS.decCalls.Load(); got != 0 {
		t.Fatalf("writer Decrypt KMS calls = %d, want 0 (cached DEK)", got)
	}

	// Fresh instance has cold cache: calls real Floci KMS Decrypt once, then caches DEK.
	readerKMS := &countingKMS{inner: kmsClient}
	reader, err := NewKMSEnvelopeCipher(readerKMS, keyID)
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher(reader): %v", err)
	}

	got1Cold, err := reader.Decrypt(ctx, tid, ct1)
	if err != nil || !bytes.Equal(got1Cold, p1) {
		t.Fatalf("reader.Decrypt(ct1) = %q, err=%v", got1Cold, err)
	}
	got2Warm, err := reader.Decrypt(ctx, tid, ct2)
	if err != nil || !bytes.Equal(got2Warm, p2) {
		t.Fatalf("reader.Decrypt(ct2) = %q, err=%v", got2Warm, err)
	}
	if got := readerKMS.decCalls.Load(); got != 1 {
		t.Fatalf("reader KMS Decrypt calls = %d, want 1 (second call cached)", got)
	}

	// Cross-tenant decrypt must fail.
	if _, err := reader.Decrypt(ctx, tenant.TenantID("other-tenant"), ct1); err == nil {
		t.Fatal("expected cross-tenant Decrypt to fail against Floci KMS")
	}

	// Tampered ciphertext must fail.
	tampered := append([]byte(nil), ct1...)
	tampered[len(tampered)-1] ^= 0xff
	if _, err := reader.Decrypt(ctx, tid, tampered); err == nil {
		t.Fatal("expected tampered ciphertext Decrypt to fail")
	}
}
