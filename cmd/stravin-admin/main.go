// Command stravin-admin seeds and configures Stravin repositories, Tier 1 Wasm
// hooks, and Tier 2 KMS-encrypted webhooks against DynamoDB, S3, and KMS.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"stravin.dev/core/internal/admin"
	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
)

type serviceFactory func(ctx context.Context, kmsKeyID string) (*admin.Service, error)

func main() {
	if err := run(context.Background(), os.Args[1:], newAWSService, os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "stravin-admin: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, factory serviceFactory, stdout, stderr io.Writer) error {
	if len(args) < 1 {
		printUsage(stderr)
		return errors.New("missing command")
	}

	switch args[0] {
	case "repo":
		if len(args) < 2 || args[1] != "create" {
			return errors.New("usage: stravin-admin repo create --tenant <t> --repo <r> [flags]")
		}
		return runRepoCreate(ctx, args[2:], factory, stdout, stderr)
	case "hook":
		if len(args) < 2 || args[1] != "put" {
			return errors.New("usage: stravin-admin hook put --tenant <t> --repo <r> --hook-id <id> --wasm <path> [flags]")
		}
		return runHookPut(ctx, args[2:], factory, stdout, stderr)
	case "webhook":
		if len(args) < 2 || args[1] != "put" {
			return errors.New("usage: stravin-admin webhook put --tenant <t> --repo <r> --webhook-id <id> --url <url> --secret <secret> [flags]")
		}
		return runWebhookPut(ctx, args[2:], factory, stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stdout)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  stravin-admin repo create    --tenant <t> --repo <r> [--region us-east-1] [--ref-shards 1] [--manifest-shards 1] [--default-branch refs/heads/main] [--idempotent]")
	_, _ = fmt.Fprintln(w, "  stravin-admin hook put       --tenant <t> --repo <r> --hook-id <id> --wasm <file.wasm> [--ref-glob refs/heads/*] [--timeout-ms 500] [--memory-mb 64] [--fail-mode closed] [--append]")
	_, _ = fmt.Fprintln(w, "  stravin-admin webhook put    --tenant <t> --repo <r> --webhook-id <id> --url <url> --secret <secret> [--ref-glob refs/heads/*] [--kms-key-id alias/stravin-local-dev] [--max-attempts 3] [--append]")
}

func runRepoCreate(ctx context.Context, args []string, factory serviceFactory, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("repo create", flag.ContinueOnError)
	fs.SetOutput(stderr)

	tenantID := fs.String("tenant", "", "tenant ID")
	repoID := fs.String("repo", "", "repository ID")
	region := fs.String("region", envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", "us-east-1")), "home region")
	epoch := fs.Uint64("epoch", 1, "initial failover epoch")
	refShards := fs.Int("ref-shards", 1, "number of ref shards")
	manifestShards := fs.Int("manifest-shards", 1, "number of manifest shards")
	defaultBranch := fs.String("default-branch", "refs/heads/main", "default branch ref")
	idempotent := fs.Bool("idempotent", false, "succeed if repo already exists")

	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := factory(ctx, "")
	if err != nil {
		return err
	}
	res, err := svc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       *tenantID,
		RepoID:         *repoID,
		HomeRegion:     *region,
		FailoverEpoch:  lease.Epoch(*epoch),
		RefShards:      *refShards,
		ManifestShards: *manifestShards,
		DefaultBranch:  *defaultBranch,
		Idempotent:     *idempotent,
	})
	if err != nil {
		return err
	}
	return writeJSON(stdout, res)
}

func runHookPut(ctx context.Context, args []string, factory serviceFactory, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("hook put", flag.ContinueOnError)
	fs.SetOutput(stderr)

	tenantID := fs.String("tenant", "", "tenant ID")
	repoID := fs.String("repo", "", "repository ID")
	hookID := fs.String("hook-id", "", "hook identifier")
	wasmPath := fs.String("wasm", "", "path to compiled .wasm file")
	refGlob := fs.String("ref-glob", "refs/heads/*", "ref glob pattern")
	timeoutMS := fs.Int("timeout-ms", 500, "hook timeout in milliseconds")
	memoryMB := fs.Int("memory-mb", 64, "hook linear memory limit in MB")
	failMode := fs.String("fail-mode", string(ddb.FailModeClosed), "fail mode (closed or open)")
	appendFlag := fs.Bool("append", false, "append/upsert into existing repo hooks")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*wasmPath) == "" {
		return errors.New("--wasm path is required")
	}
	wasmBytes, err := os.ReadFile(*wasmPath) //#nosec G304 -- CLI operator supplies local .wasm artifact path
	if err != nil {
		return fmt.Errorf("read wasm file %q: %w", *wasmPath, err)
	}

	svc, err := factory(ctx, "")
	if err != nil {
		return err
	}
	res, err := svc.ConfigureWasmHook(ctx, admin.ConfigureWasmHookInput{
		TenantID:      *tenantID,
		RepoID:        *repoID,
		HookID:        *hookID,
		WasmBytes:     wasmBytes,
		RefGlob:       *refGlob,
		TimeoutMS:     *timeoutMS,
		MemoryLimitMB: *memoryMB,
		FailMode:      ddb.FailMode(*failMode),
		Append:        *appendFlag,
	})
	if err != nil {
		return err
	}
	return writeJSON(stdout, res)
}

func runWebhookPut(ctx context.Context, args []string, factory serviceFactory, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("webhook put", flag.ContinueOnError)
	fs.SetOutput(stderr)

	defaultKMS := envOrDefault("STRAVIN_KMS_KEY_ID", envOrDefault("STRAVIN_KMS_ALIAS", "alias/stravin-local-dev"))
	tenantID := fs.String("tenant", "", "tenant ID")
	repoID := fs.String("repo", "", "repository ID")
	webhookID := fs.String("webhook-id", "", "webhook identifier")
	targetURL := fs.String("url", "", "webhook target HTTP(S) URL")
	secret := fs.String("secret", "", "plaintext webhook HMAC secret (encrypted via KMS)")
	kmsKeyID := fs.String("kms-key-id", defaultKMS, "KMS key ID or alias")
	refGlob := fs.String("ref-glob", "refs/heads/*", "ref glob pattern")
	maxAttempts := fs.Int("max-attempts", 3, "maximum delivery attempts")
	initBackoff := fs.Int("initial-backoff-ms", 100, "initial retry backoff in ms")
	maxBackoff := fs.Int("max-backoff-ms", 2000, "maximum retry backoff in ms")
	dlqURL := fs.String("dlq-url", "", "optional DLQ identifier")
	disabled := fs.Bool("disabled", false, "create webhook in disabled state")
	appendFlag := fs.Bool("append", false, "append/upsert into existing repo webhooks")

	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := factory(ctx, *kmsKeyID)
	if err != nil {
		return err
	}
	res, err := svc.ConfigureWebhook(ctx, admin.ConfigureWebhookInput{
		TenantID:         *tenantID,
		RepoID:           *repoID,
		WebhookID:        *webhookID,
		URL:              *targetURL,
		Secret:           *secret,
		KMSKeyID:         *kmsKeyID,
		RefGlob:          *refGlob,
		Enabled:          !*disabled,
		MaxAttempts:      *maxAttempts,
		InitialBackoffMS: *initBackoff,
		MaxBackoffMS:     *maxBackoff,
		DLQURL:           *dlqURL,
		Append:           *appendFlag,
	})
	if err != nil {
		return err
	}
	return writeJSON(stdout, res)
}

func newAWSService(ctx context.Context, kmsKeyID string) (*admin.Service, error) {
	region := envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", "us-east-1"))
	tableName := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	bucketName := envOrDefault("STRAVIN_S3_BUCKET", "git-objects-"+region)
	endpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, tableName)
	if err != nil {
		return nil, fmt.Errorf("init ddb repository: %w", err)
	}

	s3Client := s3.NewAWSClient(awsCfg, endpoint, s3.ForcePathStyleFromEnv() || endpoint != "")
	s3Store, err := s3.NewStore(s3Client, bucketName)
	if err != nil {
		return nil, fmt.Errorf("init s3 store: %w", err)
	}

	var cipher admin.SecretCipher
	if strings.TrimSpace(kmsKeyID) != "" {
		kmsClient := kms.NewFromConfig(awsCfg, func(o *kms.Options) {
			if endpoint != "" {
				o.BaseEndpoint = aws.String(endpoint)
			}
		})
		cipher, err = crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID)
		if err != nil {
			return nil, fmt.Errorf("init kms cipher: %w", err)
		}
	}

	return admin.NewService(ddbRepo, s3Store, cipher)
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
