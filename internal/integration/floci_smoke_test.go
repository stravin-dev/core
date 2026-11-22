//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultFlociEndpoint = "http://localhost:4566"
	defaultRegion        = "us-east-1"
	defaultS3Bucket      = "git-objects-us-east-1"
	defaultDDBTable      = "stravin-core"
	defaultKMSAlias      = "alias/stravin-local-dev"
	dummyCredential      = "test"
)

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func requireLocalFlociConfig(t *testing.T) aws.Config {
	t.Helper()

	endpoint := envOrDefault("AWS_ENDPOINT_URL", defaultFlociEndpoint)
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse AWS_ENDPOINT_URL %q: %v", endpoint, err)
	}
	host := parsed.Hostname()
	if host != "localhost" && host != "127.0.0.1" && host != "floci" && host != "host.docker.internal" {
		t.Fatalf("refusing to run integration test against non-local endpoint %q", endpoint)
	}

	accessKey := envOrDefault("AWS_ACCESS_KEY_ID", dummyCredential)
	secretKey := envOrDefault("AWS_SECRET_ACCESS_KEY", dummyCredential)
	if accessKey != dummyCredential || secretKey != dummyCredential {
		t.Fatalf("refusing to run integration test with non-dummy AWS credentials (expected %q)", dummyCredential)
	}

	region := envOrDefault("AWS_REGION", defaultRegion)
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			Source:          "floci-integration-test",
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		t.Fatalf("load AWS config for Floci: %v", err)
	}
	return cfg
}

func TestFlociS3Smoke(t *testing.T) {
	cfg := requireLocalFlociConfig(t)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	bucket := envOrDefault("STRAVIN_S3_BUCKET", defaultS3Bucket)
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("HeadBucket(%q): %v", bucket, err)
	}

	key := fmt.Sprintf("integration/smoke-%d.bin", time.Now().UnixNano())
	payload := []byte("stravin-floci-s3-smoke")

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("PutObject(%q): %v", key, err)
	}

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = client.DeleteObject(cleanCtx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	})

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject(%q): %v", key, err)
	}
	defer func() {
		_ = out.Body.Close()
	}()

	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read GetObject body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("GetObject payload = %q, want %q", got, payload)
	}
}

func TestFlociDynamoDBSmoke(t *testing.T) {
	cfg := requireLocalFlociConfig(t)
	client := dynamodb.NewFromConfig(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	table := envOrDefault("STRAVIN_DDB_TABLE", defaultDDBTable)
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		t.Fatalf("DescribeTable(%q): %v", table, err)
	}
	if desc.Table == nil || desc.Table.TableStatus != ddbtypes.TableStatusActive {
		t.Fatalf("DescribeTable(%q) status = %v, want ACTIVE", table, desc.Table)
	}
	if aws.ToString(desc.Table.LatestStreamArn) == "" {
		t.Fatalf("DescribeTable(%q) missing LatestStreamArn", table)
	}

	pk := fmt.Sprintf("T#local#REPO#ci-smoke-%d", time.Now().UnixNano())
	sk := "SMOKE"
	wantVal := "ok"

	if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]ddbtypes.AttributeValue{
			"PK":    &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK":    &ddbtypes.AttributeValueMemberS{Value: sk},
			"state": &ddbtypes.AttributeValueMemberS{Value: wantVal},
		},
	}); err != nil {
		t.Fatalf("PutItem(%q): %v", pk, err)
	}

	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = client.DeleteItem(cleanCtx, &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key: map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
				"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
			},
		})
	})

	got, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	})
	if err != nil {
		t.Fatalf("GetItem(%q): %v", pk, err)
	}
	attr, ok := got.Item["state"].(*ddbtypes.AttributeValueMemberS)
	if !ok || attr.Value != wantVal {
		t.Fatalf("GetItem state = %#v, want %q", got.Item["state"], wantVal)
	}
}

func TestFlociKMSSmoke(t *testing.T) {
	cfg := requireLocalFlociConfig(t)
	client := kms.NewFromConfig(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	keyAlias := envOrDefault("STRAVIN_KMS_ALIAS", defaultKMSAlias)
	gdk, err := client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   aws.String(keyAlias),
		KeySpec: kmstypes.DataKeySpecAes256,
	})
	if err != nil {
		t.Fatalf("GenerateDataKey(%q): %v", keyAlias, err)
	}
	if len(gdk.Plaintext) != 32 || len(gdk.CiphertextBlob) == 0 {
		t.Fatalf("GenerateDataKey(%q) returned invalid key lengths: plaintext=%d ciphertext=%d",
			keyAlias, len(gdk.Plaintext), len(gdk.CiphertextBlob))
	}

	dec, err := client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: gdk.CiphertextBlob,
	})
	if err != nil {
		t.Fatalf("Decrypt(%q): %v", keyAlias, err)
	}
	if !bytes.Equal(dec.Plaintext, gdk.Plaintext) {
		t.Fatalf("Decrypt plaintext mismatch against GenerateDataKey")
	}
}
