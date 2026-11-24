package edge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"stravin.dev/core/internal/storage/s3"
)

const defaultS3PackCachePrefix = "pack-cache/v1"

// S3PackCacheConfig configures an S3-backed external pack response cache
// (backed by Floci S3 in local Docker Compose).
type S3PackCacheConfig struct {
	Client        s3.API
	Bucket        string
	KeyPrefix     string
	MaxEntryBytes int
	TTL           time.Duration
	Now           func() time.Time
	Metrics       *Metrics
}

// S3PackCache stores large full-clone pack responses in S3 (Floci locally)
// with TTL enforcement based on object LastModified timestamps.
type S3PackCache struct {
	client        s3.API
	bucket        string
	keyPrefix     string
	maxEntryBytes int
	ttl           time.Duration
	now           func() time.Time
	metrics       *Metrics
}

var _ PackResponseCache = (*S3PackCache)(nil)

// NewS3PackCache constructs an S3PackCache.
func NewS3PackCache(cfg S3PackCacheConfig) (*S3PackCache, error) {
	if cfg.Client == nil {
		return nil, errors.New("edge: s3 client is required")
	}
	bucket := strings.TrimSpace(cfg.Bucket)
	if bucket == "" {
		return nil, errors.New("edge: s3 bucket is required")
	}
	prefix := strings.Trim(strings.TrimSpace(cfg.KeyPrefix), "/")
	if prefix == "" {
		prefix = defaultS3PackCachePrefix
	}
	maxEntry := cfg.MaxEntryBytes
	if maxEntry <= 0 {
		maxEntry = DefaultPackCacheS3MaxEntryBytes
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultPackCacheTTL
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &S3PackCache{
		client:        cfg.Client,
		bucket:        bucket,
		keyPrefix:     prefix,
		maxEntryBytes: maxEntry,
		ttl:           ttl,
		now:           nowFn,
		metrics:       cfg.Metrics,
	}, nil
}

func (c *S3PackCache) objectKey(key string) string {
	shard := "00"
	if len(key) >= 2 {
		shard = key[:2]
	}
	return fmt.Sprintf("%s/%s/%s.bin", c.keyPrefix, shard, key)
}

// Get fetches a cached pack response from S3, treating missing or expired
// objects as cache misses.
func (c *S3PackCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	start := time.Now()
	defer func() {
		c.metrics.RecordPackCacheOpDuration(ctx, "s3", "get", time.Since(start))
	}()
	objKey := c.objectKey(key)
	out, err := c.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(objKey),
	})
	if err != nil {
		if isS3NotFound(err) {
			c.metrics.RecordPackCacheLookup(ctx, "s3", "miss")
			return nil, false, nil
		}
		c.metrics.RecordPackCacheLookup(ctx, "s3", "error")
		return nil, false, fmt.Errorf("edge: s3 pack cache get %s: %w", objKey, err)
	}
	defer func() { _ = out.Body.Close() }()

	if c.ttl > 0 && out.LastModified != nil && c.now().Sub(*out.LastModified) > c.ttl {
		c.metrics.RecordPackCacheLookup(ctx, "s3", "miss")
		return nil, false, nil
	}

	data, err := io.ReadAll(io.LimitReader(out.Body, int64(c.maxEntryBytes)+1))
	if err != nil {
		c.metrics.RecordPackCacheLookup(ctx, "s3", "error")
		return nil, false, fmt.Errorf("edge: s3 pack cache read %s: %w", objKey, err)
	}
	if len(data) == 0 || len(data) > c.maxEntryBytes {
		c.metrics.RecordPackCacheLookup(ctx, "s3", "miss")
		return nil, false, nil
	}
	c.metrics.RecordPackCacheLookup(ctx, "s3", "hit")
	c.metrics.RecordPackCacheBytes(ctx, "s3", "hit_served", int64(len(data)))
	return data, true, nil
}

// Set writes a cached pack response to S3.
func (c *S3PackCache) Set(ctx context.Context, key string, data []byte, _ time.Duration) error {
	if c == nil || len(data) == 0 {
		return nil
	}
	if len(data) > c.maxEntryBytes {
		c.metrics.RecordPackCacheWrite(ctx, "s3", "too_large")
		return ErrPackCacheEntryTooLarge
	}
	start := time.Now()
	defer func() {
		c.metrics.RecordPackCacheOpDuration(ctx, "s3", "set", time.Since(start))
	}()
	objKey := c.objectKey(key)
	_, err := c.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(objKey),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		c.metrics.RecordPackCacheWrite(ctx, "s3", "error")
		return fmt.Errorf("edge: s3 pack cache put %s: %w", objKey, err)
	}
	c.metrics.RecordPackCacheWrite(ctx, "s3", "ok")
	c.metrics.RecordPackCacheBytes(ctx, "s3", "written", int64(len(data)))
	return nil
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, s3.ErrNotFound) {
		return true
	}
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound" || code == "404"
	}
	return false
}
