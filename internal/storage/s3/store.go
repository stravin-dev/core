// Package s3 implements Stravin Core's immutable, content-addressed S3 pack,
// auxiliary index, and Wasm hook object plane using AWS SDK v2.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/tenant"
)

const (
	// EnvForcePathStyle enables S3 path-style addressing when set to a truthy boolean.
	EnvForcePathStyle = "STRAVIN_S3_FORCE_PATH_STYLE"

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

// ForcePathStyleFromEnv parses STRAVIN_S3_FORCE_PATH_STYLE.
func ForcePathStyleFromEnv() bool {
	v, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(EnvForcePathStyle)))
	return err == nil && v
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
	client API
	bucket string
	cipher crypto.EnvelopeCipher
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
