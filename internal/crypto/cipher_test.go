package crypto

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"stravin.dev/core/internal/tenant"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fakeKMS struct {
	mu            sync.Mutex
	genCalls      atomic.Int64
	decCalls      atomic.Int64
	keys          map[[32]byte]fakeKMSRecord
	failGen       error
	failDec       error
	requireCtxKey string
}

type fakeKMSRecord struct {
	plaintext []byte
	tenantID  string
}

func newFakeKMS() *fakeKMS {
	return &fakeKMS{
		keys:          make(map[[32]byte]fakeKMSRecord),
		requireCtxKey: "tenant_id",
	}
}

func (f *fakeKMS) GenerateDataKey(_ context.Context, in *kms.GenerateDataKeyInput, _ ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error) {
	f.genCalls.Add(1)
	if f.failGen != nil {
		return nil, f.failGen
	}
	if aws.ToString(in.KeyId) == "" {
		return nil, errors.New("missing KeyId")
	}
	tenantID := in.EncryptionContext[f.requireCtxKey]
	if tenantID == "" {
		return nil, errors.New("missing tenant_id encryption context")
	}

	plain := make([]byte, 32)
	if _, err := rand.Read(plain); err != nil {
		return nil, err
	}
	blob := make([]byte, 48)
	if _, err := rand.Read(blob); err != nil {
		return nil, err
	}
	h := sha256.Sum256(blob)

	f.mu.Lock()
	f.keys[h] = fakeKMSRecord{
		plaintext: append([]byte(nil), plain...),
		tenantID:  tenantID,
	}
	f.mu.Unlock()

	return &kms.GenerateDataKeyOutput{
		KeyId:          in.KeyId,
		Plaintext:      plain,
		CiphertextBlob: blob,
	}, nil
}

func (f *fakeKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	f.decCalls.Add(1)
	if f.failDec != nil {
		return nil, f.failDec
	}
	h := sha256.Sum256(in.CiphertextBlob)
	f.mu.Lock()
	rec, ok := f.keys[h]
	f.mu.Unlock()
	if !ok {
		return nil, errors.New("InvalidCiphertextException: unknown blob")
	}
	if in.EncryptionContext[f.requireCtxKey] != rec.tenantID {
		return nil, errors.New("InvalidCiphertextException: encryption context mismatch")
	}
	return &kms.DecryptOutput{
		Plaintext: append([]byte(nil), rec.plaintext...),
	}, nil
}

func TestNewKMSEnvelopeCipherValidation(t *testing.T) {
	fk := newFakeKMS()
	if _, err := NewKMSEnvelopeCipher(nil, "alias/test"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil client err = %v, want ErrInvalidInput", err)
	}
	if _, err := NewKMSEnvelopeCipher(fk, "   "); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty keyID err = %v, want ErrInvalidInput", err)
	}
	if _, err := NewKMSEnvelopeCipher(fk, "alias/test", WithTTL(0)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("zero TTL err = %v, want ErrInvalidInput", err)
	}
	if _, err := NewKMSEnvelopeCipher(fk, "alias/test", WithTTL(-time.Second)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("negative TTL err = %v, want ErrInvalidInput", err)
	}
}

func TestKMSEnvelopeCipherRoundTripAndNonceUniqueness(t *testing.T) {
	fk := newFakeKMS()
	cipher, err := NewKMSEnvelopeCipher(fk, "alias/stravin-local-dev")
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher: %v", err)
	}

	ctx := context.Background()
	tid := tenant.TenantID("acme")

	payloads := [][]byte{
		{},
		[]byte("webhook-signing-secret"),
		bytes.Repeat([]byte{0xde, 0xad, 0xbe, 0xef}, 4096),
	}

	for i, plain := range payloads {
		ct1, err := cipher.Encrypt(ctx, tid, plain)
		if err != nil {
			t.Fatalf("case %d Encrypt #1: %v", i, err)
		}
		ct2, err := cipher.Encrypt(ctx, tid, plain)
		if err != nil {
			t.Fatalf("case %d Encrypt #2: %v", i, err)
		}
		if bytes.Equal(ct1, ct2) {
			t.Fatalf("case %d: repeated Encrypt produced identical ciphertext (nonce reuse)", i)
		}

		got, err := cipher.Decrypt(ctx, tid, ct1)
		if err != nil {
			t.Fatalf("case %d Decrypt: %v", i, err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("case %d Decrypt mismatch: got %d bytes, want %d bytes", i, len(got), len(plain))
		}
	}
}

func TestKMSEnvelopeCipherTamperAndTenantIsolation(t *testing.T) {
	fk := newFakeKMS()
	cipher, err := NewKMSEnvelopeCipher(fk, "alias/stravin-local-dev")
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher: %v", err)
	}
	ctx := context.Background()

	ct, err := cipher.Encrypt(ctx, "tenant-a", []byte("top-secret-pack"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Cross-tenant decryption must fail.
	if _, err := cipher.Decrypt(ctx, "tenant-b", ct); err == nil {
		t.Fatal("expected cross-tenant Decrypt to fail")
	}

	// Flipping any byte across header, nonce, ciphertext, or GCM tag must fail.
	positions := []int{0, 1, 3, len(ct) - 1, len(ct) - 17}
	for _, pos := range positions {
		mutated := append([]byte(nil), ct...)
		mutated[pos] ^= 0x01
		if _, err := cipher.Decrypt(ctx, "tenant-a", mutated); err == nil {
			t.Fatalf("expected tampered ciphertext at byte %d to fail", pos)
		}
	}

	// Truncated envelopes must return ErrInvalidCiphertext.
	for _, short := range [][]byte{{}, {0x01}, {0x01, 0x00, 0x10}} {
		if _, err := cipher.Decrypt(ctx, "tenant-a", short); !errors.Is(err, ErrInvalidCiphertext) {
			t.Fatalf("short len=%d err = %v, want ErrInvalidCiphertext", len(short), err)
		}
	}

	// Invalid tenant ID must fail.
	if _, err := cipher.Encrypt(ctx, "", []byte("x")); err == nil {
		t.Fatal("expected empty tenant Encrypt to fail")
	}
	if _, err := cipher.Decrypt(ctx, "../bad", ct); err == nil {
		t.Fatal("expected invalid tenant Decrypt to fail")
	}
}

func TestKMSEnvelopeCipherDEKCacheAndTTLExpiry(t *testing.T) {
	fk := newFakeKMS()
	clk := newFakeClock(time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC))
	cipher, err := NewKMSEnvelopeCipher(fk, "alias/stravin-local-dev", WithTTL(5*time.Minute), WithClock(clk))
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher: %v", err)
	}
	ctx := context.Background()

	ct1, err := cipher.Encrypt(ctx, "acme", []byte("one"))
	if err != nil {
		t.Fatalf("Encrypt #1: %v", err)
	}
	ct2, err := cipher.Encrypt(ctx, "acme", []byte("two"))
	if err != nil {
		t.Fatalf("Encrypt #2: %v", err)
	}
	if got := fk.genCalls.Load(); got != 1 {
		t.Fatalf("GenerateDataKey calls = %d, want 1 (cached DEK)", got)
	}

	// Decrypting locally encrypted payloads should hit the DEK cache (0 KMS Decrypt calls).
	if _, err := cipher.Decrypt(ctx, "acme", ct1); err != nil {
		t.Fatalf("Decrypt #1: %v", err)
	}
	if _, err := cipher.Decrypt(ctx, "acme", ct2); err != nil {
		t.Fatalf("Decrypt #2: %v", err)
	}
	if got := fk.decCalls.Load(); got != 0 {
		t.Fatalf("Decrypt KMS calls = %d, want 0 (cached DEK)", got)
	}

	// Another tenant generates its own DEK.
	if _, err := cipher.Encrypt(ctx, "beta", []byte("three")); err != nil {
		t.Fatalf("Encrypt beta: %v", err)
	}
	if got := fk.genCalls.Load(); got != 2 {
		t.Fatalf("GenerateDataKey calls after second tenant = %d, want 2", got)
	}

	// Advance clock past TTL: Encrypt must call GenerateDataKey again, and Decrypt must call KMS Decrypt once then re-cache.
	clk.Advance(5*time.Minute + time.Second)
	if _, err := cipher.Encrypt(ctx, "acme", []byte("four")); err != nil {
		t.Fatalf("Encrypt after TTL: %v", err)
	}
	if got := fk.genCalls.Load(); got != 3 {
		t.Fatalf("GenerateDataKey calls after TTL = %d, want 3", got)
	}

	plain1, err := cipher.Decrypt(ctx, "acme", ct1)
	if err != nil || string(plain1) != "one" {
		t.Fatalf("Decrypt ct1 after TTL = %q, err=%v", string(plain1), err)
	}
	if got := fk.decCalls.Load(); got != 1 {
		t.Fatalf("Decrypt KMS calls after TTL = %d, want 1", got)
	}
	// Second decrypt within new TTL hits decCache.
	if _, err := cipher.Decrypt(ctx, "acme", ct2); err != nil {
		t.Fatalf("Decrypt ct2 after re-cache: %v", err)
	}
	if got := fk.decCalls.Load(); got != 1 {
		t.Fatalf("Decrypt KMS calls after re-cache = %d, want 1", got)
	}
}

func TestKMSEnvelopeCipherConcurrentSafety(t *testing.T) {
	fk := newFakeKMS()
	cipher, err := NewKMSEnvelopeCipher(fk, "alias/stravin-local-dev")
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher: %v", err)
	}
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := tenant.TenantID(fmt.Sprintf("tenant-%d", id%4))
			msg := []byte(fmt.Sprintf("payload-%d", id))
			for iter := 0; iter < 10; iter++ {
				ct, err := cipher.Encrypt(ctx, tid, msg)
				if err != nil {
					errs <- err
					return
				}
				got, err := cipher.Decrypt(ctx, tid, ct)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(got, msg) {
					errs <- fmt.Errorf("mismatch: got %q want %q", got, msg)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent error: %v", err)
	}
}

func TestNoopCipher(t *testing.T) {
	var c EnvelopeCipher = NoopCipher{}
	ctx := context.Background()
	in := []byte("plain-bytes")
	ct, err := c.Encrypt(ctx, "acme", in)
	if err != nil {
		t.Fatalf("NoopCipher.Encrypt: %v", err)
	}
	pt, err := c.Decrypt(ctx, "acme", ct)
	if err != nil {
		t.Fatalf("NoopCipher.Decrypt: %v", err)
	}
	if !bytes.Equal(pt, in) {
		t.Fatalf("NoopCipher round-trip mismatch: got %q want %q", pt, in)
	}
}
