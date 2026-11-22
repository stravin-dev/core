// Package crypto implements AWS KMS envelope encryption with AES-256-GCM and a
// thread-safe per-tenant TTL Data Encryption Key (DEK) cache. It is decoupled
// from S3 so both internal/storage/s3 and cmd/stravin-hook can share it.
package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultDEKTTL is the default in-memory cache duration for per-tenant DEKs.
	DefaultDEKTTL = 5 * time.Minute

	// DefaultMaxCacheEntries bounds the decrypted DEK cache size.
	DefaultMaxCacheEntries = 4096

	envelopeVersion1    byte = 1
	envelopeHeaderFixed      = 3 // 1-byte version + 2-byte big-endian encrypted DEK length
	aes256KeyBytes           = 32
	gcmNonceBytes            = 12
	tenantContextKey         = "tenant_id"
)

var (
	ErrInvalidInput      = errors.New("crypto: invalid input")
	ErrInvalidCiphertext = errors.New("crypto: invalid envelope ciphertext")
	ErrDecryptionFailed  = errors.New("crypto: decryption failed")
)

// KMSAPI is the narrow AWS SDK v2 KMS client interface used for envelope encryption.
// *kms.Client satisfies this interface directly.
type KMSAPI interface {
	GenerateDataKey(ctx context.Context, params *kms.GenerateDataKeyInput, optFns ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error)
	Decrypt(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

var _ KMSAPI = (*kms.Client)(nil)

// NewKMSClient builds an AWS SDK v2 KMS client with an optional endpoint override.
func NewKMSClient(cfg aws.Config, endpoint string) *kms.Client {
	return kms.NewFromConfig(cfg, func(o *kms.Options) {
		if trimmed := strings.TrimSpace(endpoint); trimmed != "" {
			o.BaseEndpoint = aws.String(trimmed)
		}
	})
}

// EnvelopeCipher defines the GoF Strategy interface for tenant-scoped envelope encryption.
type EnvelopeCipher interface {
	Encrypt(ctx context.Context, tenantID tenant.TenantID, plaintext []byte) ([]byte, error)
	Decrypt(ctx context.Context, tenantID tenant.TenantID, ciphertext []byte) ([]byte, error)
}

// NoopCipher is a pass-through EnvelopeCipher strategy for unencrypted workflows.
type NoopCipher struct{}

// Encrypt returns a defensive copy of plaintext after validating tenantID.
func (NoopCipher) Encrypt(_ context.Context, tenantID tenant.TenantID, plaintext []byte) ([]byte, error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}
	return append([]byte(nil), plaintext...), nil
}

// Decrypt returns a defensive copy of ciphertext after validating tenantID.
func (NoopCipher) Decrypt(_ context.Context, tenantID tenant.TenantID, ciphertext []byte) ([]byte, error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}
	return append([]byte(nil), ciphertext...), nil
}

// Clock provides the current wall time for deterministic DEK cache TTL tests.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// Option configures a KMSEnvelopeCipher.
type Option func(*KMSEnvelopeCipher)

// WithTTL sets the per-tenant DEK cache TTL.
func WithTTL(d time.Duration) Option {
	return func(c *KMSEnvelopeCipher) {
		c.ttl = d
	}
}

// WithClock overrides the clock used for DEK cache expiration.
func WithClock(clock Clock) Option {
	return func(c *KMSEnvelopeCipher) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// WithMaxCacheEntries sets the upper bound on cached decrypted DEKs.
func WithMaxCacheEntries(n int) Option {
	return func(c *KMSEnvelopeCipher) {
		if n > 0 {
			c.maxCacheEntries = n
		}
	}
}

type cachedEncDEK struct {
	plaintext []byte
	encrypted []byte
	expiresAt time.Time
}

type decCacheKey struct {
	tenantID tenant.TenantID
	blobHash [32]byte
}

type cachedDecDEK struct {
	plaintext []byte
	expiresAt time.Time
}

// KMSEnvelopeCipher implements EnvelopeCipher using AWS KMS GenerateDataKey/Decrypt,
// local AES-256-GCM authenticated encryption, and a thread-safe TTL DEK cache.
type KMSEnvelopeCipher struct {
	client          KMSAPI
	keyID           string
	ttl             time.Duration
	clock           Clock
	maxCacheEntries int

	mu       sync.RWMutex
	encCache map[tenant.TenantID]cachedEncDEK
	decCache map[decCacheKey]cachedDecDEK
}

var (
	_ EnvelopeCipher = (*KMSEnvelopeCipher)(nil)
	_ EnvelopeCipher = NoopCipher{}
)

// NewKMSEnvelopeCipher constructs a KMSEnvelopeCipher bound to a KMS CMK ID or alias.
func NewKMSEnvelopeCipher(client KMSAPI, keyID string, opts ...Option) (*KMSEnvelopeCipher, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil kms client", ErrInvalidInput)
	}
	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return nil, fmt.Errorf("%w: empty kms key id", ErrInvalidInput)
	}
	c := &KMSEnvelopeCipher{
		client:          client,
		keyID:           keyID,
		ttl:             DefaultDEKTTL,
		clock:           realClock{},
		maxCacheEntries: DefaultMaxCacheEntries,
		encCache:        make(map[tenant.TenantID]cachedEncDEK),
		decCache:        make(map[decCacheKey]cachedDecDEK),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	if c.ttl <= 0 {
		return nil, fmt.Errorf("%w: ttl must be positive", ErrInvalidInput)
	}
	return c, nil
}

// Encrypt seals plaintext under a cached per-tenant AES-256-GCM DEK wrapped by KMS.
func (c *KMSEnvelopeCipher) Encrypt(ctx context.Context, tenantID tenant.TenantID, plaintext []byte) ([]byte, error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}

	plainDEK, encDEK, err := c.getOrGenerateDEK(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	aead, err := newAESGCM(plainDEK)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcmNonceBytes)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: generate nonce: %w", err)
	}

	headerLen := envelopeHeaderFixed + len(encDEK)
	out := make([]byte, headerLen+gcmNonceBytes, headerLen+gcmNonceBytes+len(plaintext)+aead.Overhead())
	out[0] = envelopeVersion1
	binary.BigEndian.PutUint16(out[1:3], uint16(len(encDEK)))
	copy(out[3:headerLen], encDEK)
	copy(out[headerLen:headerLen+gcmNonceBytes], nonce)

	aad := buildAAD(out[:headerLen], tenantID)
	out = aead.Seal(out, nonce, plaintext, aad)
	return out, nil
}

// Decrypt unwraps the envelope DEK (via cache or KMS Decrypt) and authenticates/decrypts ciphertext.
func (c *KMSEnvelopeCipher) Decrypt(ctx context.Context, tenantID tenant.TenantID, ciphertext []byte) ([]byte, error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}
	if len(ciphertext) < envelopeHeaderFixed {
		return nil, fmt.Errorf("%w: truncated header", ErrInvalidCiphertext)
	}
	if ciphertext[0] != envelopeVersion1 {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidCiphertext, ciphertext[0])
	}

	encDEKLen := int(binary.BigEndian.Uint16(ciphertext[1:3]))
	headerLen := envelopeHeaderFixed + encDEKLen
	if encDEKLen == 0 || len(ciphertext) < headerLen+gcmNonceBytes {
		return nil, fmt.Errorf("%w: truncated envelope", ErrInvalidCiphertext)
	}

	header := ciphertext[:headerLen]
	encDEK := ciphertext[envelopeHeaderFixed:headerLen]
	nonce := ciphertext[headerLen : headerLen+gcmNonceBytes]
	body := ciphertext[headerLen+gcmNonceBytes:]

	plainDEK, err := c.getOrDecryptDEK(ctx, tenantID, encDEK)
	if err != nil {
		return nil, err
	}

	aead, err := newAESGCM(plainDEK)
	if err != nil {
		return nil, err
	}

	aad := buildAAD(header, tenantID)
	plaintext, err := aead.Open(nil, nonce, body, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDecryptionFailed, err)
	}
	return plaintext, nil
}

// InvalidateTenant evicts cached encryption and decryption DEKs for tenantID.
func (c *KMSEnvelopeCipher) InvalidateTenant(tenantID tenant.TenantID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.encCache, tenantID)
	for k := range c.decCache {
		if k.tenantID == tenantID {
			delete(c.decCache, k)
		}
	}
}

func (c *KMSEnvelopeCipher) getOrGenerateDEK(ctx context.Context, tenantID tenant.TenantID) ([]byte, []byte, error) {
	now := c.clock.Now()

	c.mu.RLock()
	if entry, ok := c.encCache[tenantID]; ok && now.Before(entry.expiresAt) {
		plain := append([]byte(nil), entry.plaintext...)
		enc := append([]byte(nil), entry.encrypted...)
		c.mu.RUnlock()
		return plain, enc, nil
	}
	c.mu.RUnlock()

	out, err := c.client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:             aws.String(c.keyID),
		KeySpec:           types.DataKeySpecAes256,
		EncryptionContext: map[string]string{tenantContextKey: string(tenantID)},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: kms GenerateDataKey: %w", err)
	}
	if len(out.Plaintext) != aes256KeyBytes {
		return nil, nil, fmt.Errorf("%w: unexpected DEK length %d", ErrInvalidInput, len(out.Plaintext))
	}
	if len(out.CiphertextBlob) == 0 || len(out.CiphertextBlob) > math.MaxUint16 {
		return nil, nil, fmt.Errorf("%w: invalid KMS ciphertext blob length %d", ErrInvalidInput, len(out.CiphertextBlob))
	}

	plain := append([]byte(nil), out.Plaintext...)
	enc := append([]byte(nil), out.CiphertextBlob...)
	exp := c.clock.Now().Add(c.ttl)
	dk := decCacheKey{tenantID: tenantID, blobHash: sha256.Sum256(enc)}

	c.mu.Lock()
	c.encCache[tenantID] = cachedEncDEK{plaintext: plain, encrypted: enc, expiresAt: exp}
	c.putDecCacheLocked(dk, plain, exp)
	c.mu.Unlock()

	return append([]byte(nil), plain...), append([]byte(nil), enc...), nil
}

func (c *KMSEnvelopeCipher) getOrDecryptDEK(ctx context.Context, tenantID tenant.TenantID, encDEK []byte) ([]byte, error) {
	now := c.clock.Now()
	dk := decCacheKey{tenantID: tenantID, blobHash: sha256.Sum256(encDEK)}

	c.mu.RLock()
	if entry, ok := c.decCache[dk]; ok && now.Before(entry.expiresAt) {
		plain := append([]byte(nil), entry.plaintext...)
		c.mu.RUnlock()
		return plain, nil
	}
	c.mu.RUnlock()

	out, err := c.client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob:    encDEK,
		EncryptionContext: map[string]string{tenantContextKey: string(tenantID)},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: kms Decrypt: %w", ErrDecryptionFailed, err)
	}
	if len(out.Plaintext) != aes256KeyBytes {
		return nil, fmt.Errorf("%w: unexpected decrypted DEK length %d", ErrDecryptionFailed, len(out.Plaintext))
	}

	plain := append([]byte(nil), out.Plaintext...)
	exp := c.clock.Now().Add(c.ttl)

	c.mu.Lock()
	c.putDecCacheLocked(dk, plain, exp)
	c.mu.Unlock()

	return append([]byte(nil), plain...), nil
}

func (c *KMSEnvelopeCipher) putDecCacheLocked(dk decCacheKey, plain []byte, exp time.Time) {
	if len(c.decCache) >= c.maxCacheEntries {
		now := c.clock.Now()
		for k, v := range c.decCache {
			if !now.Before(v.expiresAt) {
				delete(c.decCache, k)
			}
		}
		if len(c.decCache) >= c.maxCacheEntries {
			for k := range c.decCache {
				delete(c.decCache, k)
				break
			}
		}
	}
	c.decCache[dk] = cachedDecDEK{plaintext: plain, expiresAt: exp}
}

func newAESGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}
	return aead, nil
}

func buildAAD(header []byte, tenantID tenant.TenantID) []byte {
	aad := make([]byte, len(header)+len(tenantID))
	copy(aad, header)
	copy(aad[len(header):], tenantID)
	return aad
}
