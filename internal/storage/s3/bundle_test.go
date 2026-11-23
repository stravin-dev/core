package s3

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/tenant"
)

type fakePresignAPI struct {
	lastBucket string
	lastKey    string
	url        string
	err        error
}

func (f *fakePresignAPI) PresignGetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	f.lastBucket = aws.ToString(in.Bucket)
	f.lastKey = aws.ToString(in.Key)
	if f.err != nil {
		return nil, f.err
	}
	return &v4.PresignedHTTPRequest{URL: f.url}, nil
}

func TestBundleKeyAndStoreRoundTripAndPresign(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatal(err)
	}

	key, err := BundleKey(rk)
	if err != nil {
		t.Fatalf("BundleKey: %v", err)
	}
	if key != "bundles/acme/core/latest.bundle" {
		t.Fatalf("BundleKey = %q, want bundles/acme/core/latest.bundle", key)
	}

	api := newFakeS3API()
	presigner := &fakePresignAPI{
		url: "http://127.0.0.1:4566/git-objects/bundles/acme/core/latest.bundle?X-Amz-Signature=deadbeef",
	}
	store, err := NewStore(api, "git-objects", WithPresignClient(presigner))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	// Absent bundle -> HasBundle=false, PresignBundleURL=ErrNotFound.
	has, err := store.HasBundle(ctx, rk)
	if err != nil || has {
		t.Fatalf("HasBundle(absent) = (%v, %v), want (false, nil)", has, err)
	}
	if _, err := store.PresignBundleURL(ctx, rk, 15*time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("PresignBundleURL(absent) err = %v, want ErrNotFound", err)
	}

	// Upload bundle and verify GetBundle, HasBundle, and PresignBundleURL.
	bundleBytes := []byte("# v2 git bundle\nfake-bundle-payload")
	if err := store.PutBundle(ctx, rk, bundleBytes); err != nil {
		t.Fatalf("PutBundle: %v", err)
	}
	has, err = store.HasBundle(ctx, rk)
	if err != nil || !has {
		t.Fatalf("HasBundle(present) = (%v, %v), want (true, nil)", has, err)
	}
	got, err := store.GetBundle(ctx, rk)
	if err != nil || !bytes.Equal(got, bundleBytes) {
		t.Fatalf("GetBundle = (%q, %v), want %q", got, err, bundleBytes)
	}
	gotURL, err := store.PresignBundleURL(ctx, rk, 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignBundleURL: %v", err)
	}
	if gotURL != presigner.url || presigner.lastBucket != "git-objects" || presigner.lastKey != key {
		t.Fatalf("presign mismatch: url=%q bucket=%q key=%q", gotURL, presigner.lastBucket, presigner.lastKey)
	}
}
