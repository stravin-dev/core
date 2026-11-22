package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"stravin.dev/core/internal/tenant"
)

type fakeS3Object struct {
	data        []byte
	contentType string
	etag        string
	modifiedAt  time.Time
}

type fakeS3API struct {
	mu      sync.Mutex
	objects map[string]fakeS3Object

	putHook func(ctx context.Context, in *awss3.PutObjectInput) error
	pageCap int
}

func newFakeS3API() *fakeS3API {
	return &fakeS3API{
		objects: make(map[string]fakeS3Object),
	}
}

func (f *fakeS3API) PutObject(ctx context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if f.putHook != nil {
		if err := f.putHook(ctx, in); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.objects[aws.ToString(in.Key)] = fakeS3Object{
		data:        data,
		contentType: aws.ToString(in.ContentType),
		etag:        `"etag-1"`,
		modifiedAt:  time.Now().UTC(),
	}
	f.mu.Unlock()
	return &awss3.PutObjectOutput{ETag: aws.String(`"etag-1"`)}, nil
}

func (f *fakeS3API) GetObject(ctx context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	obj, ok := f.objects[aws.ToString(in.Key)]
	f.mu.Unlock()
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("The specified key does not exist.")}
	}
	cp := append([]byte(nil), obj.data...)
	return &awss3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(cp)),
		ContentLength: aws.Int64(int64(len(cp))),
		ContentType:   aws.String(obj.contentType),
		ETag:          aws.String(obj.etag),
	}, nil
}

func (f *fakeS3API) HeadObject(ctx context.Context, in *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	obj, ok := f.objects[aws.ToString(in.Key)]
	f.mu.Unlock()
	if !ok {
		return nil, &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{
					Response: &http.Response{StatusCode: http.StatusNotFound},
				},
				Err: &smithy.GenericAPIError{Code: "NotFound", Message: "Not Found"},
			},
		}
	}
	return &awss3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(obj.data))),
		ContentType:   aws.String(obj.contentType),
		ETag:          aws.String(obj.etag),
		LastModified:  aws.Time(obj.modifiedAt),
	}, nil
}

func (f *fakeS3API) DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	delete(f.objects, aws.ToString(in.Key))
	f.mu.Unlock()
	return &awss3.DeleteObjectOutput{}, nil
}

func (f *fakeS3API) ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := aws.ToString(in.Prefix)
	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	start := 0
	if in.ContinuationToken != nil {
		for i, k := range keys {
			if k == *in.ContinuationToken {
				start = i
				break
			}
		}
	}
	end := len(keys)
	var nextToken *string
	truncated := false
	if f.pageCap > 0 && end-start > f.pageCap {
		end = start + f.pageCap
		nextToken = aws.String(keys[end])
		truncated = true
	}

	var contents []types.Object
	for _, k := range keys[start:end] {
		obj := f.objects[k]
		contents = append(contents, types.Object{
			Key:          aws.String(k),
			Size:         aws.Int64(int64(len(obj.data))),
			ETag:         aws.String(obj.etag),
			LastModified: aws.Time(obj.modifiedAt),
		})
	}
	f.mu.Unlock()

	return &awss3.ListObjectsV2Output{
		Contents:              contents,
		IsTruncated:           aws.Bool(truncated),
		NextContinuationToken: nextToken,
	}, nil
}

type xorCipher struct {
	mask byte
}

func (c xorCipher) Encrypt(_ context.Context, tid tenant.TenantID, plaintext []byte) ([]byte, error) {
	if err := tid.Validate(); err != nil {
		return nil, err
	}
	out := make([]byte, len(plaintext)+len(tid)+1)
	copy(out, tid)
	out[len(tid)] = ':'
	for i, b := range plaintext {
		out[len(tid)+1+i] = b ^ c.mask
	}
	return out, nil
}

func (c xorCipher) Decrypt(_ context.Context, tid tenant.TenantID, ciphertext []byte) ([]byte, error) {
	prefix := string(tid) + ":"
	if !strings.HasPrefix(string(ciphertext), prefix) {
		return nil, errors.New("tenant mismatch")
	}
	body := ciphertext[len(prefix):]
	out := make([]byte, len(body))
	for i, b := range body {
		out[i] = b ^ c.mask
	}
	return out, nil
}

func TestNewStoreValidation(t *testing.T) {
	fake := newFakeS3API()
	if _, err := NewStore(nil, "bucket"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil client err = %v, want ErrInvalidInput", err)
	}
	if _, err := NewStore(fake, "   "); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty bucket err = %v, want ErrInvalidInput", err)
	}
}

func TestStorePutGetHeadDeleteAndListPacks(t *testing.T) {
	fake := newFakeS3API()
	fake.pageCap = 2 // exercise pagination
	store, err := NewStore(fake, "git-objects-us-east-1")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	ctx := context.Background()
	rk := mustRepoKey(t, "acme", "core")
	otherRepo := mustRepoKey(t, "beta", "core")

	packBytes := []byte("PACK\x00\x00\x00\x02sample-pack")
	idxBytes := []byte("\xfftOc\x00\x00\x00\x02sample-idx")

	if err := store.PutPack(ctx, rk, testPackSHA, packBytes, idxBytes); err != nil {
		t.Fatalf("PutPack: %v", err)
	}
	// Upload a second pack in another repo to ensure ListRepoPacks isolates by tenant/repo.
	secondSHA := "b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef012"
	if err := store.PutPack(ctx, otherRepo, secondSHA, []byte("other-pack"), []byte("other-idx")); err != nil {
		t.Fatalf("PutPack otherRepo: %v", err)
	}

	gotPack, err := store.GetPack(ctx, rk, testPackSHA)
	if err != nil || !bytes.Equal(gotPack, packBytes) {
		t.Fatalf("GetPack = %q, err=%v", gotPack, err)
	}
	gotIdx, err := store.GetIndex(ctx, rk, testPackSHA)
	if err != nil || !bytes.Equal(gotIdx, idxBytes) {
		t.Fatalf("GetIndex = %q, err=%v", gotIdx, err)
	}

	packKey, _ := PackKey(rk, testPackSHA)
	meta, err := store.HeadObject(ctx, packKey)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if meta.SizeBytes != int64(len(packBytes)) || meta.Key != packKey {
		t.Fatalf("unexpected HeadObject meta: %+v", meta)
	}

	packs, err := store.ListRepoPacks(ctx, rk)
	if err != nil {
		t.Fatalf("ListRepoPacks: %v", err)
	}
	if len(packs) != 1 || packs[0].PackSHA256 != testPackSHA || packs[0].Key != packKey {
		t.Fatalf("ListRepoPacks = %+v, want 1 pack %s", packs, testPackSHA)
	}

	// Aux files and Wasm hook.
	if err := store.PutAuxMIDX(ctx, rk, testPackSHA, "epoch-1", []byte("midx-data")); err != nil {
		t.Fatalf("PutAuxMIDX: %v", err)
	}
	gotMIDX, err := store.GetAuxMIDX(ctx, rk, testPackSHA, "epoch-1")
	if err != nil || string(gotMIDX) != "midx-data" {
		t.Fatalf("GetAuxMIDX = %q, err=%v", gotMIDX, err)
	}

	if err := store.PutAuxGraph(ctx, rk, testPackSHA, "epoch-1", []byte("graph-data")); err != nil {
		t.Fatalf("PutAuxGraph: %v", err)
	}
	gotGraph, err := store.GetAuxGraph(ctx, rk, testPackSHA, "epoch-1")
	if err != nil || string(gotGraph) != "graph-data" {
		t.Fatalf("GetAuxGraph = %q, err=%v", gotGraph, err)
	}

	wasmBytes := []byte("\x00asm\x01\x00\x00\x00")
	if err := store.PutWasmHook(ctx, testWasmSHA, wasmBytes); err != nil {
		t.Fatalf("PutWasmHook: %v", err)
	}
	gotWasm, err := store.GetWasmHook(ctx, testWasmSHA)
	if err != nil || !bytes.Equal(gotWasm, wasmBytes) {
		t.Fatalf("GetWasmHook = %q, err=%v", gotWasm, err)
	}

	// Delete existing object succeeds; deleting again returns ErrNotFound.
	if err := store.DeleteObject(ctx, packKey); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if err := store.DeleteObject(ctx, packKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteObject missing = %v, want ErrNotFound", err)
	}
	if _, err := store.HeadObject(ctx, packKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("HeadObject missing = %v, want ErrNotFound", err)
	}
	if _, err := store.GetPack(ctx, rk, testPackSHA); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetPack missing = %v, want ErrNotFound", err)
	}
	if _, err := store.GetWasmHook(ctx, strings.Repeat("0", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWasmHook missing = %v, want ErrNotFound", err)
	}
}

func TestPutPackConcurrentAndContextCancellation(t *testing.T) {
	fake := newFakeS3API()
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	fake.putHook = func(ctx context.Context, _ *awss3.PutObjectInput) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	store, err := NewStore(fake, "git-objects-us-east-1")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	rk := mustRepoKey(t, "acme", "core")

	done := make(chan error, 1)
	go func() {
		done <- store.PutPack(ctx, rk, testPackSHA, []byte("pack"), []byte("idx"))
	}()

	// Both .pack and .idx PutObject calls must start concurrently.
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for concurrent PutObject #%d", i+1)
		}
	}

	// Simulate Wasm hook rejection cancelling upload context.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("PutPack after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PutPack did not return promptly on context cancel")
	}

	// Verify sibling cancellation when one upload fails.
	siblingCancelled := make(chan struct{}, 1)
	fake.putHook = func(ctx context.Context, in *awss3.PutObjectInput) error {
		if strings.HasSuffix(aws.ToString(in.Key), ".idx") {
			return errors.New("simulated s3 500")
		}
		<-ctx.Done()
		siblingCancelled <- struct{}{}
		return ctx.Err()
	}

	err = store.PutPack(context.Background(), rk, testPackSHA, []byte("pack"), []byte("idx"))
	if err == nil || !strings.Contains(err.Error(), "simulated s3 500") {
		t.Fatalf("PutPack partial failure err = %v, want simulated s3 500", err)
	}
	select {
	case <-siblingCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("sibling PutObject was not cancelled when peer failed")
	}
	close(release)
}

func TestStoreWithEnvelopeCipher(t *testing.T) {
	fake := newFakeS3API()
	store, err := NewStore(fake, "git-objects-us-east-1", WithEnvelopeCipher(xorCipher{mask: 0x5a}))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	ctx := context.Background()
	rk := mustRepoKey(t, "acme", "core")
	packPlain := []byte("sensitive-git-pack")
	idxPlain := []byte("sensitive-git-idx")

	if err := store.PutPack(ctx, rk, testPackSHA, packPlain, idxPlain); err != nil {
		t.Fatalf("PutPack: %v", err)
	}

	// Raw S3 object must not contain plaintext.
	packKey, _ := PackKey(rk, testPackSHA)
	raw := fake.objects[packKey].data
	if bytes.Contains(raw, packPlain) {
		t.Fatalf("expected S3 object to be encrypted, but found plaintext")
	}

	gotPack, err := store.GetPack(ctx, rk, testPackSHA)
	if err != nil || !bytes.Equal(gotPack, packPlain) {
		t.Fatalf("GetPack decrypted = %q, err=%v", gotPack, err)
	}
	gotIdx, err := store.GetIndex(ctx, rk, testPackSHA)
	if err != nil || !bytes.Equal(gotIdx, idxPlain) {
		t.Fatalf("GetIndex decrypted = %q, err=%v", gotIdx, err)
	}
}

func TestForcePathStyleFromEnv(t *testing.T) {
	t.Setenv(EnvForcePathStyle, "true")
	if !ForcePathStyleFromEnv() {
		t.Fatal("expected ForcePathStyleFromEnv() = true when env=true")
	}
	t.Setenv(EnvForcePathStyle, "false")
	if ForcePathStyleFromEnv() {
		t.Fatal("expected ForcePathStyleFromEnv() = false when env=false")
	}
	t.Setenv(EnvForcePathStyle, "")
	if ForcePathStyleFromEnv() {
		t.Fatal("expected ForcePathStyleFromEnv() = false when env empty")
	}
}
