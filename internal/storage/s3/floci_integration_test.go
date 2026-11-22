//go:build integration

package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/crypto"
)

func newFlociS3AndKMS(t *testing.T) (*awss3.Client, *kms.Client, string, string) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci S3 integration test: AWS_ENDPOINT_URL not set")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	bucket := os.Getenv("STRAVIN_S3_BUCKET")
	if bucket == "" {
		bucket = "git-objects-us-east-1"
	}
	kmsKeyID := os.Getenv("STRAVIN_KMS_KEY_ID")
	if kmsKeyID == "" {
		kmsKeyID = "alias/stravin-local-dev"
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	if os.Getenv(EnvForcePathStyle) == "" {
		t.Setenv(EnvForcePathStyle, "true")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	s3Client := NewAWSClient(cfg, endpoint, ForcePathStyleFromEnv())
	kmsClient := kms.NewFromConfig(cfg, func(o *kms.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return s3Client, kmsClient, bucket, kmsKeyID
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestFlociIntegration_S3PackPlaneAndEncryptedEnvelope(t *testing.T) {
	s3Client, kmsClient, bucket, kmsKeyID := newFlociS3AndKMS(t)
	ctx := context.Background()

	store, err := NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	nonce := fmt.Sprintf("it-%d", time.Now().UnixNano())
	rk := mustRepoKey(t, "local", nonce)

	packBytes := []byte("PACK\x00\x00\x00\x02floci-pack-" + nonce)
	idxBytes := []byte("\xfftOc\x00\x00\x00\x02floci-idx-" + nonce)
	packSHA := sha256Hex(packBytes)

	// 1. PutPack -> GetPack / GetIndex / HeadObject / ListRepoPacks.
	if err := store.PutPack(ctx, rk, packSHA, packBytes, idxBytes); err != nil {
		t.Fatalf("PutPack: %v", err)
	}
	packKey, _ := PackKey(rk, packSHA)
	idxKey, _ := IndexKey(rk, packSHA)
	t.Cleanup(func() {
		_ = store.DeleteObject(context.Background(), packKey)
		_ = store.DeleteObject(context.Background(), idxKey)
	})

	gotPack, err := store.GetPack(ctx, rk, packSHA)
	if err != nil || !bytes.Equal(gotPack, packBytes) {
		t.Fatalf("GetPack = %q, err=%v", gotPack, err)
	}
	gotIdx, err := store.GetIndex(ctx, rk, packSHA)
	if err != nil || !bytes.Equal(gotIdx, idxBytes) {
		t.Fatalf("GetIndex = %q, err=%v", gotIdx, err)
	}

	meta, err := store.HeadObject(ctx, packKey)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if meta.SizeBytes != int64(len(packBytes)) {
		t.Fatalf("HeadObject SizeBytes = %d, want %d", meta.SizeBytes, len(packBytes))
	}

	packs, err := store.ListRepoPacks(ctx, rk)
	if err != nil {
		t.Fatalf("ListRepoPacks: %v", err)
	}
	if len(packs) != 1 || packs[0].PackSHA256 != packSHA {
		t.Fatalf("ListRepoPacks = %+v, want 1 pack %s", packs, packSHA)
	}

	// 2. Aux (.midx, .graph) and Wasm hook round-trip.
	midxBytes := []byte("MIDX-floci-" + nonce)
	graphBytes := []byte("CGPH-floci-" + nonce)
	if err := store.PutAuxMIDX(ctx, rk, packSHA, "epoch-1", midxBytes); err != nil {
		t.Fatalf("PutAuxMIDX: %v", err)
	}
	if err := store.PutAuxGraph(ctx, rk, packSHA, "epoch-1", graphBytes); err != nil {
		t.Fatalf("PutAuxGraph: %v", err)
	}
	midxKey, _ := AuxMIDXKey(rk, packSHA, "epoch-1")
	graphKey, _ := AuxGraphKey(rk, packSHA, "epoch-1")
	t.Cleanup(func() {
		_ = store.DeleteObject(context.Background(), midxKey)
		_ = store.DeleteObject(context.Background(), graphKey)
	})

	if got, err := store.GetAuxMIDX(ctx, rk, packSHA, "epoch-1"); err != nil || !bytes.Equal(got, midxBytes) {
		t.Fatalf("GetAuxMIDX = %q, err=%v", got, err)
	}
	if got, err := store.GetAuxGraph(ctx, rk, packSHA, "epoch-1"); err != nil || !bytes.Equal(got, graphBytes) {
		t.Fatalf("GetAuxGraph = %q, err=%v", got, err)
	}

	wasmBytes := []byte("\x00asm\x01\x00\x00\x00" + nonce)
	wasmSHA := sha256Hex(wasmBytes)
	wasmKey, _ := WasmHookKey(wasmSHA)
	if err := store.PutWasmHook(ctx, wasmSHA, wasmBytes); err != nil {
		t.Fatalf("PutWasmHook: %v", err)
	}
	t.Cleanup(func() {
		_ = store.DeleteObject(context.Background(), wasmKey)
	})
	if got, err := store.GetWasmHook(ctx, wasmSHA); err != nil || !bytes.Equal(got, wasmBytes) {
		t.Fatalf("GetWasmHook = %q, err=%v", got, err)
	}

	// 3. DeleteObject + 404 ErrNotFound checks.
	if err := store.DeleteObject(ctx, packKey); err != nil {
		t.Fatalf("DeleteObject(packKey): %v", err)
	}
	if _, err := store.HeadObject(ctx, packKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("HeadObject after delete = %v, want ErrNotFound", err)
	}
	if _, err := store.GetPack(ctx, rk, packSHA); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetPack after delete = %v, want ErrNotFound", err)
	}
	if err := store.DeleteObject(ctx, packKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteObject missing = %v, want ErrNotFound", err)
	}
	missingKey := "00/local/" + nonce + "/packs/" + strings.Repeat("f", 64) + ".pack"
	if _, err := store.GetObject(ctx, missingKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetObject missing = %v, want ErrNotFound", err)
	}

	// 4. Optional KMS envelope encryption integration.
	cipher, err := crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher: %v", err)
	}
	encStore, err := NewStore(s3Client, bucket, WithEnvelopeCipher(cipher))
	if err != nil {
		t.Fatalf("NewStore(encrypted): %v", err)
	}

	secretPack := []byte("PACK-secret-payload-" + nonce)
	secretIdx := []byte("IDX-secret-payload-" + nonce)
	secretSHA := sha256Hex(secretPack)
	secretPackKey, _ := PackKey(rk, secretSHA)
	secretIdxKey, _ := IndexKey(rk, secretSHA)
	t.Cleanup(func() {
		_ = store.DeleteObject(context.Background(), secretPackKey)
		_ = store.DeleteObject(context.Background(), secretIdxKey)
	})

	if err := encStore.PutPack(ctx, rk, secretSHA, secretPack, secretIdx); err != nil {
		t.Fatalf("encStore.PutPack: %v", err)
	}

	// Raw S3 bytes must be encrypted, not plaintext.
	rawObj, err := store.GetObject(ctx, secretPackKey)
	if err != nil {
		t.Fatalf("raw GetObject(secretPackKey): %v", err)
	}
	if bytes.Contains(rawObj, secretPack) {
		t.Fatal("expected encrypted pack in Floci S3, found plaintext")
	}

	decPack, err := encStore.GetPack(ctx, rk, secretSHA)
	if err != nil || !bytes.Equal(decPack, secretPack) {
		t.Fatalf("encStore.GetPack = %q, err=%v", decPack, err)
	}
	decIdx, err := encStore.GetIndex(ctx, rk, secretSHA)
	if err != nil || !bytes.Equal(decIdx, secretIdx) {
		t.Fatalf("encStore.GetIndex = %q, err=%v", decIdx, err)
	}
}
