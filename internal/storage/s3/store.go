// Package s3 implements Stravin Core's immutable, content-addressed S3 pack,
// auxiliary index, and Wasm hook object plane using AWS SDK v2.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/tenant"
)

const (
	// EnvForcePathStyle enables S3 path-style addressing when set to a truthy boolean.
	EnvForcePathStyle = "STRAVIN_S3_FORCE_PATH_STYLE"

	// EnvKMSKeyID configures optional per-tenant KMS envelope encryption for S3 objects.
	EnvKMSKeyID = "STRAVIN_KMS_KEY_ID"

	// ReplicationStatus constants mirror AWS S3 HeadObject x-amz-replication-status values.
	ReplicationStatusCompleted = "COMPLETED"
	ReplicationStatusComplete  = "COMPLETE"
	ReplicationStatusPending   = "PENDING"
	ReplicationStatusFailed    = "FAILED"
	ReplicationStatusReplica   = "REPLICA"

	contentTypeOctetStream = "application/octet-stream"
	contentTypeWasm        = "application/wasm"
)

// API is the narrow AWS SDK v2 S3 client interface used by Store.
// *awss3.Client satisfies this interface directly.
type API interface {
	PutObject(ctx context.Context, params *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *awss3.HeadObjectInput, optFns ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *awss3.DeleteObjectInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *awss3.ListObjectsV2Input, optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

var _ API = (*awss3.Client)(nil)

// PresignAPI is the narrow AWS SDK v2 S3 presign client interface used for
// generating time-bounded presigned GET URLs (e.g. Git Protocol v2 bundle-uri).
// *awss3.PresignClient satisfies this interface directly.
type PresignAPI interface {
	PresignGetObject(ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

var _ PresignAPI = (*awss3.PresignClient)(nil)

const defaultBundlePresignExpiry = 15 * time.Minute

// ObjectMeta describes metadata returned by HeadObject.
type ObjectMeta struct {
	Key               string
	SizeBytes         int64
	ETag              string
	ContentType       string
	LastModified      time.Time
	ReplicationStatus string
}

// PackObjectInfo describes a packfile discovered by ListRepoPacks.
type PackObjectInfo struct {
	Key          string
	PackSHA256   string
	SizeBytes    int64
	ETag         string
	LastModified time.Time
}

// ObjectStore defines the GoF Repository/Adapter interface for Stravin's S3 object plane.
type ObjectStore interface {
	PutPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string, packData, idxData []byte) error
	GetPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error)
	GetIndex(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error)
	PutAuxMIDX(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error
	GetAuxMIDX(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string) ([]byte, error)
	PutAuxGraph(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error
	GetAuxGraph(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string) ([]byte, error)
	PutWasmHook(ctx context.Context, wasmSHA256 string, wasmBytes []byte) error
	GetWasmHook(ctx context.Context, wasmSHA256 string) ([]byte, error)
	HeadObject(ctx context.Context, key string) (ObjectMeta, error)
	DeleteObject(ctx context.Context, key string) error
	ListRepoPacks(ctx context.Context, repo tenant.RepoKey) ([]PackObjectInfo, error)
}

// Option configures a Store.
type Option func(*Store)

// WithEnvelopeCipher enables optional tenant-scoped envelope encryption for
// repository objects (.pack, .idx, .midx, .graph).
func WithEnvelopeCipher(c crypto.EnvelopeCipher) Option {
	return func(s *Store) {
		s.cipher = c
	}
}

// WithPresignClient overrides the S3 presign client used by PresignBundleURL.
func WithPresignClient(p PresignAPI) Option {
	return func(s *Store) {
		s.presigner = p
	}
}

// WithKMSEnvelopeCipher constructs a KMS envelope cipher when kmsKeyID is
// non-empty and returns a Store Option wiring it into NewStore. It returns
// (nil, nil) when kmsKeyID is empty, and returns an error when kmsKeyID is
// non-empty and cipher initialization fails so callers fail closed.
func WithKMSEnvelopeCipher(kmsClient crypto.KMSAPI, kmsKeyID string, opts ...crypto.Option) (Option, error) {
	if kmsKeyID == "" {
		return nil, nil
	}
	cipher, err := crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID, opts...)
	if err != nil {
		return nil, err
	}
	return WithEnvelopeCipher(cipher), nil
}

// ForcePathStyleFromEnv parses STRAVIN_S3_FORCE_PATH_STYLE.
func ForcePathStyleFromEnv() bool {
	v, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(EnvForcePathStyle)))
	return err == nil && v
}

// KMSKeyIDFromEnv reads STRAVIN_KMS_KEY_ID. Non-empty whitespace-only values
// are preserved so startup fails closed instead of silently disabling KMS.
func KMSKeyIDFromEnv() string {
	raw := os.Getenv(EnvKMSKeyID)
	if trimmed := strings.TrimSpace(raw); trimmed != "" {
		return trimmed
	}
	return raw
}

// NewAWSClient builds a standard AWS SDK v2 S3 client configured with optional
// endpoint override and UsePathStyle (for STRAVIN_S3_FORCE_PATH_STYLE=true).
func NewAWSClient(cfg aws.Config, endpoint string, forcePathStyle bool) *awss3.Client {
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = forcePathStyle
		if strings.TrimSpace(endpoint) != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
}

// Store is the AWS SDK v2 S3 adapter implementing ObjectStore.
type Store struct {
	client    API
	presigner PresignAPI
	bucket    string
	cipher    crypto.EnvelopeCipher
}

var _ ObjectStore = (*Store)(nil)

// NewStore constructs a Store bound to an S3 client and bucket name.
func NewStore(client API, bucket string, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil s3 client", ErrInvalidInput)
	}
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, fmt.Errorf("%w: empty bucket name", ErrInvalidInput)
	}
	s := &Store{
		client: client,
		bucket: bucket,
	}
	if awsClient, ok := client.(*awss3.Client); ok {
		s.presigner = awss3.NewPresignClient(awsClient)
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s, nil
}

// Bucket returns the configured S3 bucket name.
func (s *Store) Bucket() string {
	return s.bucket
}

// PutPack uploads both .pack and .idx objects concurrently and returns nil only
// after both S3 PutObject calls complete with HTTP 200 OK (durability before
// visibility). Context cancellation or a failure on either upload cancels the
// sibling upload cleanly.
func (s *Store) PutPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string, packData, idxData []byte) error {
	packKey, err := PackKey(repo, packSHA256)
	if err != nil {
		return err
	}
	idxKey, err := IndexKey(repo, packSHA256)
	if err != nil {
		return err
	}

	packPayload, err := s.encryptIfConfigured(ctx, repo.TenantID, packData)
	if err != nil {
		return err
	}
	idxPayload, err := s.encryptIfConfigured(ctx, repo.TenantID, idxData)
	if err != nil {
		return err
	}

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		recordErr(s.PutObject(uploadCtx, packKey, packPayload, contentTypeOctetStream))
	}()
	go func() {
		defer wg.Done()
		recordErr(s.PutObject(uploadCtx, idxKey, idxPayload, contentTypeOctetStream))
	}()

	wg.Wait()
	return firstErr
}

// GetPack fetches and optionally decrypts a repository packfile.
func (s *Store) GetPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error) {
	key, err := PackKey(repo, packSHA256)
	if err != nil {
		return nil, err
	}
	return s.getTenantObject(ctx, repo.TenantID, key)
}

// GetIndex fetches and optionally decrypts a repository packfile index (.idx).
func (s *Store) GetIndex(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error) {
	key, err := IndexKey(repo, packSHA256)
	if err != nil {
		return nil, err
	}
	return s.getTenantObject(ctx, repo.TenantID, key)
}

// PutAuxMIDX uploads a multi-pack-index (.midx) auxiliary artifact.
func (s *Store) PutAuxMIDX(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error {
	key, err := AuxMIDXKey(repo, hashPrefixOrSHA, manifestEpoch)
	if err != nil {
		return err
	}
	return s.putTenantObject(ctx, repo.TenantID, key, data, contentTypeOctetStream)
}

// GetAuxMIDX fetches a multi-pack-index (.midx) auxiliary artifact.
func (s *Store) GetAuxMIDX(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string) ([]byte, error) {
	key, err := AuxMIDXKey(repo, hashPrefixOrSHA, manifestEpoch)
	if err != nil {
		return nil, err
	}
	return s.getTenantObject(ctx, repo.TenantID, key)
}

// PutAuxGraph uploads a commit-graph (.graph) auxiliary artifact.
func (s *Store) PutAuxGraph(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error {
	key, err := AuxGraphKey(repo, hashPrefixOrSHA, manifestEpoch)
	if err != nil {
		return err
	}
	return s.putTenantObject(ctx, repo.TenantID, key, data, contentTypeOctetStream)
}

// GetAuxGraph fetches a commit-graph (.graph) auxiliary artifact.
func (s *Store) GetAuxGraph(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string) ([]byte, error) {
	key, err := AuxGraphKey(repo, hashPrefixOrSHA, manifestEpoch)
	if err != nil {
		return nil, err
	}
	return s.getTenantObject(ctx, repo.TenantID, key)
}

// PutWasmHook uploads an immutable content-addressed Wasm hook binary under hooks/{wasm_sha256}.wasm.
func (s *Store) PutWasmHook(ctx context.Context, wasmSHA256 string, wasmBytes []byte) error {
	key, err := WasmHookKey(wasmSHA256)
	if err != nil {
		return err
	}
	return s.PutObject(ctx, key, wasmBytes, contentTypeWasm)
}

// GetWasmHook fetches an immutable content-addressed Wasm hook binary.
func (s *Store) GetWasmHook(ctx context.Context, wasmSHA256 string) ([]byte, error) {
	key, err := WasmHookKey(wasmSHA256)
	if err != nil {
		return nil, err
	}
	return s.GetObject(ctx, key)
}

// PutBundle uploads a pre-built Git bundle for Protocol v2 bundle-uri cold-clone
// offload. Bundles are stored as raw Git bundle bytes so clients can fetch them
// directly over a presigned S3 GET URL.
func (s *Store) PutBundle(ctx context.Context, repo tenant.RepoKey, data []byte) error {
	key, err := BundleKey(repo)
	if err != nil {
		return err
	}
	return s.PutObject(ctx, key, data, contentTypeOctetStream)
}

// GetBundle downloads the latest Git bundle for repo.
func (s *Store) GetBundle(ctx context.Context, repo tenant.RepoKey) ([]byte, error) {
	key, err := BundleKey(repo)
	if err != nil {
		return nil, err
	}
	return s.GetObject(ctx, key)
}

// HasBundle reports whether a Git bundle exists in S3 for repo.
func (s *Store) HasBundle(ctx context.Context, repo tenant.RepoKey) (bool, error) {
	key, err := BundleKey(repo)
	if err != nil {
		return false, err
	}
	_, err = s.HeadObject(ctx, key)
	if err != nil {
		if isNotFoundError(err) || errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// PresignBundleURL returns a time-bounded presigned S3 GET URL for repo's
// latest Git bundle, or ErrNotFound if no bundle has been published.
func (s *Store) PresignBundleURL(ctx context.Context, repo tenant.RepoKey, expiry time.Duration) (string, error) {
	if s.presigner == nil {
		return "", fmt.Errorf("%w: s3 presign client not configured", ErrInvalidInput)
	}
	key, err := BundleKey(repo)
	if err != nil {
		return "", err
	}
	if _, err := s.HeadObject(ctx, key); err != nil {
		return "", err
	}
	if expiry <= 0 {
		expiry = defaultBundlePresignExpiry
	}
	presigned, err := s.presigner.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, awss3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("s3: presign bundle %s: %w", key, err)
	}
	return presigned.URL, nil
}

// PutObject uploads raw bytes to key and returns nil only after S3 confirms HTTP 200 OK.
func (s *Store) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%w: empty object key", ErrInvalidInput)
	}
	if contentType == "" {
		contentType = contentTypeOctetStream
	}
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("s3: put %s: %w", key, err)
	}
	return nil
}

// GetObject downloads raw bytes for key, mapping 404/NoSuchKey to ErrNotFound.
func (s *Store) GetObject(ctx context.Context, key string) ([]byte, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("%w: empty object key", ErrInvalidInput)
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("s3: get %s: %w", key, err)
	}
	defer func() {
		_ = out.Body.Close()
	}()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: read %s: %w", key, err)
	}
	return data, nil
}

// HeadObject inspects object metadata, mapping 404/NotFound to ErrNotFound.
func (s *Store) HeadObject(ctx context.Context, key string) (ObjectMeta, error) {
	if strings.TrimSpace(key) == "" {
		return ObjectMeta{}, fmt.Errorf("%w: empty object key", ErrInvalidInput)
	}
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return ObjectMeta{}, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return ObjectMeta{}, fmt.Errorf("s3: head %s: %w", key, err)
	}
	return ObjectMeta{
		Key:               key,
		SizeBytes:         aws.ToInt64(out.ContentLength),
		ETag:              aws.ToString(out.ETag),
		ContentType:       aws.ToString(out.ContentType),
		LastModified:      aws.ToTime(out.LastModified),
		ReplicationStatus: string(out.ReplicationStatus),
	}, nil
}

// DeleteObject removes key after verifying it exists, returning ErrNotFound if absent.
func (s *Store) DeleteObject(ctx context.Context, key string) error {
	if _, err := s.HeadObject(ctx, key); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return fmt.Errorf("s3: delete %s: %w", key, err)
	}
	return nil
}

// ListRepoPacks lists all .pack objects belonging to repo across all 256 hex shard prefixes.
func (s *Store) ListRepoPacks(ctx context.Context, repo tenant.RepoKey) ([]PackObjectInfo, error) {
	if err := repo.Validate(); err != nil {
		return nil, err
	}

	var (
		packs []PackObjectInfo
		token *string
	)
	for {
		out, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: list packs for %s: %w", repo, err)
		}
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			sha, ok := parseRepoPackKey(key, repo)
			if !ok {
				continue
			}
			packs = append(packs, PackObjectInfo{
				Key:          key,
				PackSHA256:   sha,
				SizeBytes:    aws.ToInt64(obj.Size),
				ETag:         aws.ToString(obj.ETag),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return packs, nil
}

func (s *Store) putTenantObject(ctx context.Context, tenantID tenant.TenantID, key string, data []byte, contentType string) error {
	payload, err := s.encryptIfConfigured(ctx, tenantID, data)
	if err != nil {
		return err
	}
	return s.PutObject(ctx, key, payload, contentType)
}

func (s *Store) getTenantObject(ctx context.Context, tenantID tenant.TenantID, key string) ([]byte, error) {
	raw, err := s.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if s.cipher == nil {
		return raw, nil
	}
	return s.cipher.Decrypt(ctx, tenantID, raw)
}

func (s *Store) encryptIfConfigured(ctx context.Context, tenantID tenant.TenantID, data []byte) ([]byte, error) {
	if s.cipher == nil {
		return data, nil
	}
	return s.cipher.Encrypt(ctx, tenantID, data)
}
