//go:build multiregion

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"stravin.dev/core/internal/flocitest/ddbsync"
	"stravin.dev/core/internal/flocitest/s3sync"
)

type regionClients struct {
	region      string
	endpoint    string
	bucket      string
	table       string
	edgeHTTPURL string
	nodeURL     string
	ctrlURL     string

	s3Client  *awss3.Client
	ddbClient *dynamodb.Client
}

type multiRegionEnv struct {
	regionA   *regionClients
	regionB   *regionClients
	authToken string
}

func newMultiRegionEnv(t *testing.T) *multiRegionEnv {
	t.Helper()

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	regionA := newRegionClients(t, regionClients{
		region:      "us-east-1",
		endpoint:    mrEnvOrDefault("STRAVIN_MR_FLOCI_A_ENDPOINT", "http://127.0.0.1:4566"),
		bucket:      mrEnvOrDefault("STRAVIN_MR_S3_BUCKET_A", "git-objects-us-east-1"),
		table:       mrEnvOrDefault("STRAVIN_DDB_TABLE", "stravin-core"),
		edgeHTTPURL: strings.TrimRight(mrEnvOrDefault("STRAVIN_MR_EDGE_A_HTTP_URL", "http://127.0.0.1:8080"), "/"),
		nodeURL:     strings.TrimRight(mrEnvOrDefault("STRAVIN_MR_NODE_A_URL", "http://127.0.0.1:8091"), "/"),
		ctrlURL:     strings.TrimRight(mrEnvOrDefault("STRAVIN_MR_CTRL_A_URL", "http://127.0.0.1:8083"), "/"),
	})

	regionB := newRegionClients(t, regionClients{
		region:      "us-west-2",
		endpoint:    mrEnvOrDefault("STRAVIN_MR_FLOCI_B_ENDPOINT", "http://127.0.0.1:4567"),
		bucket:      mrEnvOrDefault("STRAVIN_MR_S3_BUCKET_B", "git-objects-us-west-2"),
		table:       mrEnvOrDefault("STRAVIN_DDB_TABLE", "stravin-core"),
		edgeHTTPURL: strings.TrimRight(mrEnvOrDefault("STRAVIN_MR_EDGE_B_HTTP_URL", "http://127.0.0.1:8081"), "/"),
		nodeURL:     strings.TrimRight(mrEnvOrDefault("STRAVIN_MR_NODE_B_URL", "http://127.0.0.1:8092"), "/"),
		ctrlURL:     strings.TrimRight(mrEnvOrDefault("STRAVIN_MR_CTRL_B_URL", "http://127.0.0.1:8085"), "/"),
	})

	return &multiRegionEnv{
		regionA:   regionA,
		regionB:   regionB,
		authToken: mrEnvOrDefault("STRAVIN_E2E_TOKEN", "stravin-local-dev-token"),
	}
}

func newRegionClients(t *testing.T, rc regionClients) *regionClients {
	t.Helper()
	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(rc.region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig(%s): %v", rc.region, err)
	}

	rc.s3Client = awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(rc.endpoint)
		o.UsePathStyle = true
	})
	rc.ddbClient = dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(rc.endpoint)
	})
	return &rc
}

func (rc *regionClients) smartHTTPRepoURL(token, tenantID, repoID string) string {
	trimmed := strings.TrimPrefix(rc.edgeHTTPURL, "http://")
	return fmt.Sprintf("http://dev:%s@%s/%s/%s.git", token, trimmed, tenantID, repoID)
}

func TestMultiRegion_S3BidirectionalSyncAndLoopGuard(t *testing.T) {
	env := newMultiRegionEnv(t)
	ctx := context.Background()
	ts := time.Now().UnixNano()

	keyAB := fmt.Sprintf("e2e/mr-s3-a-to-b-%d.txt", ts)
	payloadAB := []byte(fmt.Sprintf("from-region-a-%d", ts))
	if _, err := env.regionA.s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(env.regionA.bucket),
		Key:         aws.String(keyAB),
		Body:        bytes.NewReader(payloadAB),
		ContentType: aws.String("text/plain"),
	}); err != nil {
		t.Fatalf("PutObject region A (%s): %v", keyAB, err)
	}

	gotBody, gotMeta := waitForS3Object(t, env.regionB, keyAB, 20*time.Second)
	if !bytes.Equal(gotBody, payloadAB) {
		t.Fatalf("region B payload = %q, want %q", gotBody, payloadAB)
	}
	if origin := lookupS3Meta(gotMeta, s3sync.ReplicatedFromMetadataKey); origin != env.regionA.region {
		t.Fatalf("region B metadata %q = %q, want %q", s3sync.ReplicatedFromMetadataKey, origin, env.regionA.region)
	}

	keyBA := fmt.Sprintf("e2e/mr-s3-b-to-a-%d.txt", ts)
	payloadBA := []byte(fmt.Sprintf("from-region-b-%d", ts))
	if _, err := env.regionB.s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(env.regionB.bucket),
		Key:         aws.String(keyBA),
		Body:        bytes.NewReader(payloadBA),
		ContentType: aws.String("text/plain"),
	}); err != nil {
		t.Fatalf("PutObject region B (%s): %v", keyBA, err)
	}

	gotBodyBA, gotMetaBA := waitForS3Object(t, env.regionA, keyBA, 20*time.Second)
	if !bytes.Equal(gotBodyBA, payloadBA) {
		t.Fatalf("region A payload = %q, want %q", gotBodyBA, payloadBA)
	}
	if origin := lookupS3Meta(gotMetaBA, s3sync.ReplicatedFromMetadataKey); origin != env.regionB.region {
		t.Fatalf("region A metadata %q = %q, want %q", s3sync.ReplicatedFromMetadataKey, origin, env.regionB.region)
	}

	// Verify loop guard prevents ping-pong overwrite back to origin region.
	time.Sleep(750 * time.Millisecond)

	headA, err := env.regionA.s3Client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(env.regionA.bucket),
		Key:    aws.String(keyAB),
	})
	if err != nil {
		t.Fatalf("HeadObject region A (%s): %v", keyAB, err)
	}
	if origin := lookupS3Meta(headA.Metadata, s3sync.ReplicatedFromMetadataKey); origin != "" {
		t.Fatalf("loop detected: region A source object %s got stamped %q=%q", keyAB, s3sync.ReplicatedFromMetadataKey, origin)
	}

	headB, err := env.regionB.s3Client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(env.regionB.bucket),
		Key:    aws.String(keyBA),
	})
	if err != nil {
		t.Fatalf("HeadObject region B (%s): %v", keyBA, err)
	}
	if origin := lookupS3Meta(headB.Metadata, s3sync.ReplicatedFromMetadataKey); origin != "" {
		t.Fatalf("loop detected: region B source object %s got stamped %q=%q", keyBA, s3sync.ReplicatedFromMetadataKey, origin)
	}
}

func TestMultiRegion_DDBBidirectionalSyncLWWAndDelete(t *testing.T) {
	env := newMultiRegionEnv(t)
	ctx := context.Background()
	ts := time.Now().UnixNano()

	pkAB := fmt.Sprintf("E2E#MR#AB#%d", ts)
	sk := "ITEM"
	if _, err := env.regionA.ddbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(env.regionA.table),
		Item: map[string]ddbtypes.AttributeValue{
			"PK":      &ddbtypes.AttributeValueMemberS{Value: pkAB},
			"SK":      &ddbtypes.AttributeValueMemberS{Value: sk},
			"payload": &ddbtypes.AttributeValueMemberS{Value: "from-a"},
		},
	}); err != nil {
		t.Fatalf("PutItem region A (%s): %v", pkAB, err)
	}

	itemB := waitForDDBItem(t, env.regionB, pkAB, sk, 20*time.Second)
	assertDDBStringAttr(t, itemB, "payload", "from-a")
	assertDDBStringAttr(t, itemB, ddbsync.AttrRepOrigin, env.regionA.region)
	tsB := mustDDBNumberAttr(t, itemB, ddbsync.AttrRepTimestamp)

	pkBA := fmt.Sprintf("E2E#MR#BA#%d", ts)
	if _, err := env.regionB.ddbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(env.regionB.table),
		Item: map[string]ddbtypes.AttributeValue{
			"PK":      &ddbtypes.AttributeValueMemberS{Value: pkBA},
			"SK":      &ddbtypes.AttributeValueMemberS{Value: sk},
			"payload": &ddbtypes.AttributeValueMemberS{Value: "from-b"},
		},
	}); err != nil {
		t.Fatalf("PutItem region B (%s): %v", pkBA, err)
	}

	itemA := waitForDDBItem(t, env.regionA, pkBA, sk, 20*time.Second)
	assertDDBStringAttr(t, itemA, "payload", "from-b")
	assertDDBStringAttr(t, itemA, ddbsync.AttrRepOrigin, env.regionB.region)
	tsA := mustDDBNumberAttr(t, itemA, ddbsync.AttrRepTimestamp)

	// Confirm zero loop amplification: replicated timestamps remain stable.
	time.Sleep(750 * time.Millisecond)

	itemBAfter := waitForDDBItem(t, env.regionB, pkAB, sk, 5*time.Second)
	if got := mustDDBNumberAttr(t, itemBAfter, ddbsync.AttrRepTimestamp); got != tsB {
		t.Fatalf("loop amplification detected on %s: _floci_rep_ts changed from %s to %s", pkAB, tsB, got)
	}
	itemAAfter := waitForDDBItem(t, env.regionA, pkBA, sk, 5*time.Second)
	if got := mustDDBNumberAttr(t, itemAAfter, ddbsync.AttrRepTimestamp); got != tsA {
		t.Fatalf("loop amplification detected on %s: _floci_rep_ts changed from %s to %s", pkBA, tsA, got)
	}

	// Bidirectional DeleteItem replication.
	if _, err := env.regionA.ddbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(env.regionA.table),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pkAB},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	}); err != nil {
		t.Fatalf("DeleteItem region A (%s): %v", pkAB, err)
	}
	waitForDDBItemGone(t, env.regionB, pkAB, sk, 20*time.Second)

	if _, err := env.regionB.ddbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(env.regionB.table),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pkBA},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	}); err != nil {
		t.Fatalf("DeleteItem region B (%s): %v", pkBA, err)
	}
	waitForDDBItemGone(t, env.regionA, pkBA, sk, 20*time.Second)
}

func TestMultiRegion_DaemonsHealthyAndGitPushAToFetchB(t *testing.T) {
	env := newMultiRegionEnv(t)
	ctx := context.Background()

	// 1. Verify all 6 regional Stravin daemons report healthy.
	for name, baseURL := range map[string]string{
		"stravin-edge-a": env.regionA.edgeHTTPURL,
		"stravin-node-a": env.regionA.nodeURL,
		"stravin-ctrl-a": env.regionA.ctrlURL,
		"stravin-edge-b": env.regionB.edgeHTTPURL,
		"stravin-node-b": env.regionB.nodeURL,
		"stravin-ctrl-b": env.regionB.ctrlURL,
	} {
		checkHealthzURL(t, name, baseURL)
	}

	// 2. Seed a unique repo META in Region A and push a commit to Region A.
	const tenantID = "local"
	repoID := fmt.Sprintf("mr-git-%d", time.Now().UnixNano())
	if _, err := env.regionA.ddbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(env.regionA.table),
		Item: map[string]ddbtypes.AttributeValue{
			"PK":              &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("T#%s#REPO#%s", tenantID, repoID)},
			"SK":              &ddbtypes.AttributeValueMemberS{Value: "META"},
			"home_region":     &ddbtypes.AttributeValueMemberS{Value: env.regionA.region},
			"failover_epoch":  &ddbtypes.AttributeValueMemberN{Value: "1"},
			"ref_shards":      &ddbtypes.AttributeValueMemberN{Value: "1"},
			"manifest_shards": &ddbtypes.AttributeValueMemberN{Value: "1"},
			"write_state":     &ddbtypes.AttributeValueMemberS{Value: "ACTIVE"},
		},
	}); err != nil {
		t.Fatalf("PutItem META region A (%s/%s): %v", tenantID, repoID, err)
	}

	pushDir := t.TempDir()
	remoteA := env.regionA.smartHTTPRepoURL(env.authToken, tenantID, repoID)
	mrMustRunGit(t, pushDir, "init", "-b", "main")
	mrMustRunGit(t, pushDir, "remote", "add", "origin", remoteA)
	const wantContent = "hello from us-east-1 replicated to us-west-2\n"
	if err := os.WriteFile(filepath.Join(pushDir, "README.md"), []byte(wantContent), 0o600); err != nil {
		t.Fatalf("write README.md: %v", err)
	}
	mrMustRunGit(t, pushDir, "add", "README.md")
	mrMustRunGit(t, pushDir, "commit", "-m", "multi-region replication commit")
	pushedSHA := mrMustRunGit(t, pushDir, "rev-parse", "HEAD")

	mrMustRunGit(t, pushDir, "push", "origin", "HEAD:refs/heads/main")

	// 3. Wait for both DDB metadata (META + REF + PACK) and S3 pack/idx to replicate
	// to Region B, then clone cleanly from Region B's stravin-edge-b.
	remoteB := env.regionB.smartHTTPRepoURL(env.authToken, tenantID, repoID)
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	var lastOut string
	for time.Now().Before(deadline) {
		cloneDir := filepath.Join(t.TempDir(), "clone-b")
		out, err := mrRunGit(t, "", "clone", "--branch", "main", remoteB, cloneDir)
		if err == nil {
			gotSHA := mrMustRunGit(t, cloneDir, "rev-parse", "HEAD")
			if gotSHA == pushedSHA {
				data, readErr := os.ReadFile(filepath.Join(cloneDir, "README.md"))
				if readErr != nil {
					t.Fatalf("read cloned README.md: %v", readErr)
				}
				if string(data) != wantContent {
					t.Fatalf("cloned README.md = %q, want %q", string(data), wantContent)
				}
				return
			}
			lastErr = fmt.Errorf("cloned SHA = %s, want %s", gotSHA, pushedSHA)
			lastOut = out
		} else {
			lastErr = err
			lastOut = out
		}
		time.Sleep(400 * time.Millisecond)
	}
	t.Fatalf("timed out cloning %s/%s from Region B (%s): lastErr=%v\noutput:\n%s",
		tenantID, repoID, env.regionB.edgeHTTPURL, lastErr, lastOut)
}

func waitForS3Object(t *testing.T, rc *regionClients, key string, timeout time.Duration) ([]byte, map[string]string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := rc.s3Client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(rc.bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			body, readErr := io.ReadAll(out.Body)
			_ = out.Body.Close()
			if readErr == nil {
				return body, out.Metadata
			}
			lastErr = readErr
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for s3://%s/%s in %s: %v", rc.bucket, key, rc.region, lastErr)
	return nil, nil
}

func waitForDDBItem(t *testing.T, rc *regionClients, pk, sk string, timeout time.Duration) map[string]ddbtypes.AttributeValue {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := rc.ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(rc.table),
			Key: map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
				"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
			},
			ConsistentRead: aws.Bool(true),
		})
		if err == nil && len(out.Item) > 0 {
			return out.Item
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for DDB item (%s, %s) in %s: lastErr=%v", pk, sk, rc.region, lastErr)
	return nil
}

func waitForDDBItemGone(t *testing.T, rc *regionClients, pk, sk string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := rc.ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(rc.table),
			Key: map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
				"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
			},
			ConsistentRead: aws.Bool(true),
		})
		if err == nil && len(out.Item) == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for DDB item (%s, %s) deletion in %s", pk, sk, rc.region)
}

func lookupS3Meta(meta map[string]string, key string) string {
	for k, v := range meta {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func assertDDBStringAttr(t *testing.T, item map[string]ddbtypes.AttributeValue, attr, want string) {
	t.Helper()
	v, ok := item[attr].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("DDB attribute %q missing or not string: %#v", attr, item[attr])
	}
	if v.Value != want {
		t.Fatalf("DDB attribute %q = %q, want %q", attr, v.Value, want)
	}
}

func mustDDBNumberAttr(t *testing.T, item map[string]ddbtypes.AttributeValue, attr string) string {
	t.Helper()
	v, ok := item[attr].(*ddbtypes.AttributeValueMemberN)
	if !ok || strings.TrimSpace(v.Value) == "" {
		t.Fatalf("DDB attribute %q missing or not number: %#v", attr, item[attr])
	}
	return v.Value
}

func checkHealthzURL(t *testing.T, name, baseURL string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET %s (%s/healthz): %v", name, baseURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s (%s/healthz) status = %d, want 200", name, baseURL, resp.StatusCode)
	}
}

func mrRunGit(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=Stravin Multi-Region E2E",
		"GIT_AUTHOR_EMAIL=mr-e2e@stravin.dev",
		"GIT_COMMITTER_NAME=Stravin Multi-Region E2E",
		"GIT_COMMITTER_EMAIL=mr-e2e@stravin.dev",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mrMustRunGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := mrRunGit(t, dir, args...)
	if err != nil {
		t.Fatalf("git %s failed: %v\noutput:\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(out)
}

func mrEnvOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
