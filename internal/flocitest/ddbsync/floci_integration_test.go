//go:build integration

package ddbsync

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

func newFlociClientPair(t *testing.T, endpoint, region string) (*dynamodb.Client, *dynamodbstreams.Client) {
	t.Helper()
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Source:          "floci-ddbsync-integration",
		}, nil
	})
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig(%s, %s): %v", region, endpoint, err)
	}
	return dynamodb.NewFromConfig(cfg), dynamodbstreams.NewFromConfig(cfg)
}

func createStreamTable(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string) {
	t.Helper()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: ddbtypes.KeyTypeRange},
		},
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewImage,
		},
	})
	if err != nil {
		t.Fatalf("CreateTable(%q): %v", tableName, err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteTable(cleanCtx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

func TestFlociIntegration_BidirectionalReplicationInsertModifyRemove(t *testing.T) {
	defaultEndpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))
	sourceEndpoint := strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_ENDPOINT"))
	targetEndpoint := strings.TrimSpace(os.Getenv("FLOCI_SYNC_TARGET_ENDPOINT"))
	if sourceEndpoint == "" {
		sourceEndpoint = defaultEndpoint
	}
	if targetEndpoint == "" {
		targetEndpoint = defaultEndpoint
	}
	if sourceEndpoint == "" || targetEndpoint == "" {
		t.Skip("skipping Floci integration test: AWS_ENDPOINT_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ddbA, streamsA := newFlociClientPair(t, sourceEndpoint, "us-east-1")
	ddbB, streamsB := newFlociClientPair(t, targetEndpoint, "us-west-2")

	nonce := time.Now().UnixNano()
	tableA := fmt.Sprintf("stravin-ddbsync-test-a-%d", nonce)
	tableB := fmt.Sprintf("stravin-ddbsync-test-b-%d", nonce)
	createStreamTable(t, ctx, ddbA, tableA)
	createStreamTable(t, ctx, ddbB, tableB)

	sharedSuppressor := NewDeleteSuppressor(5*time.Second, nil)

	repAtoB, err := NewReplicator(Config{
		SourceRegion:     "us-east-1",
		SourceTable:      tableA,
		TargetRegion:     "us-west-2",
		TargetTable:      tableB,
		PollInterval:     50 * time.Millisecond,
		DeleteSuppressor: sharedSuppressor,
		Tables:           ddbA,
		Streams:          streamsA,
		Target:           ddbB,
	})
	if err != nil {
		t.Fatalf("NewReplicator A->B: %v", err)
	}

	repBtoA, err := NewReplicator(Config{
		SourceRegion:     "us-west-2",
		SourceTable:      tableB,
		TargetRegion:     "us-east-1",
		TargetTable:      tableA,
		PollInterval:     50 * time.Millisecond,
		DeleteSuppressor: sharedSuppressor,
		Tables:           ddbB,
		Streams:          streamsB,
		Target:           ddbA,
	})
	if err != nil {
		t.Fatalf("NewReplicator B->A: %v", err)
	}

	// 1. INSERT into Table A -> replicated to Table B.
	key := map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: "REPO#sync-it"},
		"SK": &ddbtypes.AttributeValueMemberS{Value: "REF#main"},
	}
	if _, err := ddbA.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableA),
		Item: map[string]ddbtypes.AttributeValue{
			"PK":  key["PK"],
			"SK":  key["SK"],
			"oid": &ddbtypes.AttributeValueMemberS{Value: "1111111111111111111111111111111111111111"},
		},
	}); err != nil {
		t.Fatalf("PutItem A (insert): %v", err)
	}

	pollUntil(t, ctx, func() bool {
		if err := repAtoB.PollOnce(ctx); err != nil {
			t.Fatalf("repAtoB.PollOnce: %v", err)
		}
		if err := repBtoA.PollOnce(ctx); err != nil {
			t.Fatalf("repBtoA.PollOnce: %v", err)
		}
		out, err := ddbB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tableB), Key: key})
		if err != nil || len(out.Item) == 0 {
			return false
		}
		oid, _ := out.Item["oid"].(*ddbtypes.AttributeValueMemberS)
		origin, _ := out.Item[AttrRepOrigin].(*ddbtypes.AttributeValueMemberS)
		_, hasTS := out.Item[AttrRepTimestamp].(*ddbtypes.AttributeValueMemberN)
		return oid != nil && oid.Value == "1111111111111111111111111111111111111111" &&
			origin != nil && origin.Value == "us-east-1" && hasTS
	})

	// 2. MODIFY in Table A -> replicated to Table B.
	if _, err := ddbA.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableA),
		Item: map[string]ddbtypes.AttributeValue{
			"PK":  key["PK"],
			"SK":  key["SK"],
			"oid": &ddbtypes.AttributeValueMemberS{Value: "2222222222222222222222222222222222222222"},
		},
	}); err != nil {
		t.Fatalf("PutItem A (modify): %v", err)
	}

	pollUntil(t, ctx, func() bool {
		if err := repAtoB.PollOnce(ctx); err != nil {
			t.Fatalf("repAtoB.PollOnce (modify): %v", err)
		}
		if err := repBtoA.PollOnce(ctx); err != nil {
			t.Fatalf("repBtoA.PollOnce (modify): %v", err)
		}
		out, err := ddbB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tableB), Key: key})
		if err != nil || len(out.Item) == 0 {
			return false
		}
		oid, _ := out.Item["oid"].(*ddbtypes.AttributeValueMemberS)
		return oid != nil && oid.Value == "2222222222222222222222222222222222222222"
	})

	// 3. REMOVE in Table A -> deleted in Table B without bounce loop.
	if _, err := ddbA.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableA),
		Key:       key,
	}); err != nil {
		t.Fatalf("DeleteItem A: %v", err)
	}

	pollUntil(t, ctx, func() bool {
		if err := repAtoB.PollOnce(ctx); err != nil {
			t.Fatalf("repAtoB.PollOnce (remove): %v", err)
		}
		if err := repBtoA.PollOnce(ctx); err != nil {
			t.Fatalf("repBtoA.PollOnce (remove): %v", err)
		}
		out, err := ddbB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tableB), Key: key})
		return err == nil && len(out.Item) == 0
	})
}

func pollUntil(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for replication: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for DynamoDB replication condition")
}
