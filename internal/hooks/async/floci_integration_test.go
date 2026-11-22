//go:build integration

package async

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

func newFlociIntegrationClients(t *testing.T) (*dynamodb.Client, *dynamodbstreams.Client, *kms.Client, string, string) {
	t.Helper()
	endpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))
	if endpoint == "" {
		t.Skip("skipping Floci integration test: AWS_ENDPOINT_URL not set")
	}

	region := strings.TrimSpace(os.Getenv("AWS_REGION"))
	if region == "" {
		region = "us-east-1"
	}
	table := strings.TrimSpace(os.Getenv("STRAVIN_DDB_TABLE"))
	if table == "" {
		table = "stravin-core"
	}
	kmsKeyID := strings.TrimSpace(os.Getenv("STRAVIN_KMS_KEY_ID"))
	if kmsKeyID == "" {
		kmsKeyID = "alias/stravin-local-dev"
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	streamsClient := dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	kmsClient := kms.NewFromConfig(cfg, func(o *kms.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return ddbClient, streamsClient, kmsClient, table, kmsKeyID
}

func TestFlociIntegration_StreamsOutboxSignedWebhookAndDLQ(t *testing.T) {
	ddbClient, streamsClient, kmsClient, table, kmsKeyID := newFlociIntegrationClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo, err := ddb.NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	cipher, err := crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher: %v", err)
	}

	tel, err := telemetry.NewInMemoryProvider("stravin-hook-floci-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	// Originating push traceparent.
	originCtx, originSpan := tel.Tracer("stravin-node").Start(ctx, "node.push")
	originSC := originSpan.SpanContext()
	originTraceparent := telemetry.TraceparentFromContext(originCtx)
	originSpan.End()
	tel.SpanExporter.Reset()

	nonce := fmt.Sprintf("hook-it-%d", time.Now().UnixNano())
	rk := tenant.RepoKey{TenantID: "local", RepoID: tenant.RepoID(nonce)}
	secret := []byte("floci-kms-webhook-secret-" + nonce)

	encSecret, err := cipher.Encrypt(ctx, rk.TenantID, secret)
	if err != nil {
		t.Fatalf("KMS Encrypt webhook secret: %v", err)
	}

	var (
		mu             sync.Mutex
		deliveredBody  []byte
		deliveredSig   string
		deliveredEvent string
		okHits         atomic.Int32
		failHits       atomic.Int32
	)
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		deliveredBody = body
		deliveredSig = r.Header.Get(HeaderSignature256)
		deliveredEvent = r.Header.Get(HeaderEvent)
		mu.Unlock()
		okHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	if err := repo.PutWebhooks(ctx, rk, []ddb.WebhookConfig{
		{
			WebhookID:       "wh-ok",
			URL:             okSrv.URL,
			EncryptedSecret: encSecret,
			KMSKeyID:        kmsKeyID,
			RefGlob:         "refs/heads/*",
			Enabled:         true,
			Retry:           ddb.WebhookRetryConfig{MaxAttempts: 2, InitialBackoffMS: 10, MaxBackoffMS: 50},
		},
		{
			WebhookID:       "wh-fail",
			URL:             failSrv.URL,
			EncryptedSecret: encSecret,
			KMSKeyID:        kmsKeyID,
			RefGlob:         "refs/heads/*",
			Enabled:         true,
			Retry:           ddb.WebhookRetryConfig{MaxAttempts: 2, InitialBackoffMS: 10, MaxBackoffMS: 50},
		},
	}); err != nil {
		t.Fatalf("PutWebhooks: %v", err)
	}

	// Commit an atomic push that writes a Ref History outbox record with traceparent.
	wantOID := "1111111111111111111111111111111111111111"
	wantPackID := "pack-" + nonce
	if _, err := repo.CommitPush(ctx, ddb.CommitPushInput{
		Repo:           rk,
		RefShards:      4,
		ManifestShards: 2,
		Epoch:          1,
		OriginRegion:   "us-east-1",
		TraceParent:    originTraceparent,
		Updates: []ddb.RefUpdate{
			{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: wantOID},
		},
		Pack: &ddb.PackUpload{
			PackID:    wantPackID,
			SizeBytes: 512,
		},
	}); err != nil {
		t.Fatalf("CommitPush: %v", err)
	}

	memDLQ := NewMemoryDLQSink()
	ddbDLQ, err := NewDynamoDBDLQSink(ddbClient, table)
	if err != nil {
		t.Fatalf("NewDynamoDBDLQSink: %v", err)
	}
	dlqSink := NewMultiDLQSink(memDLQ, ddbDLQ)

	// Fresh KMS cipher simulates stravin-hook cold-decrypting via Floci KMS.
	hookCipher, err := crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		t.Fatalf("NewKMSEnvelopeCipher(hook): %v", err)
	}

	dispatcher, err := NewWebhookDispatcher(DispatcherConfig{
		Webhooks:  repo,
		Cipher:    hookCipher,
		DLQ:       dlqSink,
		Telemetry: tel,
	})
	if err != nil {
		t.Fatalf("NewWebhookDispatcher: %v", err)
	}

	consumer, err := NewConsumer(ConsumerConfig{
		TableName:   table,
		ConsumerID:  "floci-it-" + nonce,
		Tables:      ddbClient,
		Streams:     streamsClient,
		Checkpoints: repo,
		Observers:   []EventObserver{dispatcher},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	// Poll until our specific repo's webhook is delivered.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := consumer.PollOnce(ctx); err != nil {
			t.Fatalf("PollOnce: %v", err)
		}
		if okHits.Load() >= 1 && failHits.Load() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if okHits.Load() < 1 {
		t.Fatalf("expected signed webhook delivery to okSrv, got %d hits", okHits.Load())
	}
	if failHits.Load() < 2 {
		t.Fatalf("expected 2 retry attempts to failSrv, got %d", failHits.Load())
	}

	mu.Lock()
	bodyCopy := append([]byte(nil), deliveredBody...)
	sigCopy := deliveredSig
	eventCopy := deliveredEvent
	mu.Unlock()

	if eventCopy != EventPush {
		t.Errorf("X-Stravin-Event = %q, want %q", eventCopy, EventPush)
	}
	if !VerifySignature(secret, bodyCopy, sigCopy) {
		t.Errorf("HMAC-SHA256 verification failed for sig=%q body=%s", sigCopy, string(bodyCopy))
	}

	var payload WebhookPayload
	if err := json.Unmarshal(bodyCopy, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.TenantID != "local" || payload.RepoID != nonce || payload.Ref != "refs/heads/main" ||
		payload.OID != wantOID || payload.PackID != wantPackID {
		t.Errorf("unexpected webhook payload: %+v", payload)
	}

	// Verify OTel span link back to originating push trace.
	var linked bool
	for _, sp := range tel.SpanExporter.GetSpans() {
		if sp.Name != SpanAsyncDispatch {
			continue
		}
		for _, link := range sp.Links {
			if link.SpanContext.TraceID() == originSC.TraceID() && link.SpanContext.SpanID() == originSC.SpanID() {
				linked = true
			}
		}
	}
	if !linked {
		t.Fatal("expected hook.async.dispatch span linked to originating push traceparent")
	}

	// Verify DLQ entry persisted both in memory and in Floci DynamoDB.
	var foundMemDLQ bool
	for _, entry := range memDLQ.Entries() {
		if entry.RepoID == nonce && entry.WebhookID == "wh-fail" && entry.Attempts == 2 {
			foundMemDLQ = true
			break
		}
	}
	if !foundMemDLQ {
		t.Fatalf("expected wh-fail DLQ entry in MemoryDLQSink, got %+v", memDLQ.Entries())
	}

	qOut, err := ddbClient.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :sk)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("T#local#REPO#%s#DLQ", nonce)},
			":sk": &ddbtypes.AttributeValueMemberS{Value: "HOOK#wh-fail#"},
		},
	})
	if err != nil {
		t.Fatalf("Query DynamoDB DLQ: %v", err)
	}
	if len(qOut.Items) == 0 {
		t.Fatal("expected persisted DLQ item in Floci DynamoDB")
	}
}
