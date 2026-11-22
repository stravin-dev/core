//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"stravin.dev/core/internal/admin"
	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
)

type stackEnv struct {
	awsEndpoint    string
	region         string
	ddbTable       string
	s3Bucket       string
	kmsKeyID       string
	edgeHTTPURL    string
	edgeSSHAddr    string
	packURL        string
	ctrlURL        string
	ctrlAdminToken string
	hookURL        string
	authToken      string
	sshKeyPath     string
	webhookHost    string

	ddbClient *dynamodb.Client
	ddbRepo   *ddb.Repository
	s3Store   *s3.Store
	cipher    *crypto.KMSEnvelopeCipher
	adminSvc  *admin.Service
}

func newStackEnv(t *testing.T) *stackEnv {
	t.Helper()

	endpoint := envOrDefault("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
	region := envOrDefault("AWS_REGION", "us-east-1")
	table := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	bucket := envOrDefault("STRAVIN_S3_BUCKET", "git-objects-us-east-1")
	kmsKeyID := envOrDefault("STRAVIN_KMS_KEY_ID", "alias/stravin-local-dev")

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("ddb.NewRepository: %v", err)
	}

	s3Client := s3.NewAWSClient(awsCfg, endpoint, true)
	s3Store, err := s3.NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("s3.NewStore: %v", err)
	}

	kmsClient := kms.NewFromConfig(awsCfg, func(o *kms.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	cipher, err := crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		t.Fatalf("crypto.NewKMSEnvelopeCipher: %v", err)
	}

	adminSvc, err := admin.NewService(ddbRepo, s3Store, cipher)
	if err != nil {
		t.Fatalf("admin.NewService: %v", err)
	}

	return &stackEnv{
		awsEndpoint:    endpoint,
		region:         region,
		ddbTable:       table,
		s3Bucket:       bucket,
		kmsKeyID:       kmsKeyID,
		edgeHTTPURL:    strings.TrimRight(envOrDefault("STRAVIN_EDGE_HTTP_URL", "http://127.0.0.1:8080"), "/"),
		edgeSSHAddr:    envOrDefault("STRAVIN_EDGE_SSH_ADDR", "127.0.0.1:2222"),
		packURL:        strings.TrimRight(envOrDefault("STRAVIN_PACK_URL", "http://127.0.0.1:8082"), "/"),
		ctrlURL:        strings.TrimRight(envOrDefault("STRAVIN_CTRL_URL", "http://127.0.0.1:8083"), "/"),
		ctrlAdminToken: envOrDefault("STRAVIN_CTRL_ADMIN_TOKEN", "stravin-local-ctrl-admin-token"),
		hookURL:        strings.TrimRight(envOrDefault("STRAVIN_HOOK_URL", "http://127.0.0.1:8084"), "/"),
		authToken:      envOrDefault("STRAVIN_E2E_TOKEN", "stravin-local-dev-token"),
		sshKeyPath:     strings.TrimSpace(os.Getenv("STRAVIN_E2E_SSH_KEY")),
		webhookHost:    envOrDefault("STRAVIN_E2E_WEBHOOK_HOST", "host.docker.internal"),
		ddbClient:      ddbClient,
		ddbRepo:        ddbRepo,
		s3Store:        s3Store,
		cipher:         cipher,
		adminSvc:       adminSvc,
	}
}

func (e *stackEnv) smartHTTPRepoURL(tenantID, repoID string) string {
	trimmed := strings.TrimPrefix(e.edgeHTTPURL, "http://")
	return fmt.Sprintf("http://dev:%s@%s/%s/%s.git", e.authToken, trimmed, tenantID, repoID)
}

func (e *stackEnv) sshRepoURL(tenantID, repoID string) string {
	return fmt.Sprintf("ssh://git@%s/%s/%s.git", e.edgeSSHAddr, tenantID, repoID)
}

func (e *stackEnv) checkHealthz(t *testing.T, baseURL string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET %s/healthz: %v", baseURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s/healthz status = %d, want 200", baseURL, resp.StatusCode)
	}
}

func (e *stackEnv) doCtrlRequest(t *testing.T, client *http.Client, method, path string, body []byte) *http.Response {
	t.Helper()
	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, e.ctrlURL+path, bodyReader)
	if err != nil {
		t.Fatalf("NewRequest %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+e.ctrlAdminToken)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func runGit(t *testing.T, dir string, extraEnv []string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=Stravin E2E",
		"GIT_AUTHOR_EMAIL=e2e@stravin.dev",
		"GIT_COMMITTER_NAME=Stravin E2E",
		"GIT_COMMITTER_EMAIL=e2e@stravin.dev",
	)
	cmd.Env = append(cmd.Env, extraEnv...)

	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustRunGit(t *testing.T, dir string, extraEnv []string, args ...string) string {
	t.Helper()
	out, err := runGit(t, dir, extraEnv, args...)
	if err != nil {
		t.Fatalf("git %s failed: %v\noutput:\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(out)
}

func sshEnv(keyPath string) []string {
	sshCmd := fmt.Sprintf(
		"ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o BatchMode=yes",
		keyPath,
	)
	return []string{"GIT_SSH_COMMAND=" + sshCmd}
}

func initLocalRepoWithCommit(t *testing.T, remoteURL, filename, content, message string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	mustRunGit(t, dir, nil, "init", "-b", "main")
	mustRunGit(t, dir, nil, "remote", "add", "origin", remoteURL)
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", filename, err)
	}
	mustRunGit(t, dir, nil, "add", filename)
	mustRunGit(t, dir, nil, "commit", "-m", message)
	sha := mustRunGit(t, dir, nil, "rev-parse", "HEAD")
	return dir, sha
}

func commitFile(t *testing.T, dir, filename, content, message string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", filename, err)
	}
	mustRunGit(t, dir, nil, "add", filename)
	mustRunGit(t, dir, nil, "commit", "-m", message)
	return mustRunGit(t, dir, nil, "rev-parse", "HEAD")
}

func uniqueRepoID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// buildVerdictWasm produces a valid standalone WebAssembly 1.0 module that
// calls stravin.set_verdict(decision, msgPtr, msgLen) and returns 0.
// decision=0 => ALLOW, decision=1 => REJECT.
func buildVerdictWasm(decision int32, msg string) []byte {
	const msgOffset = 1024

	encodeULEB := func(v uint32) []byte {
		var out []byte
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if v != 0 {
				b |= 0x80
			}
			out = append(out, b)
			if v == 0 {
				break
			}
		}
		return out
	}
	encodeSLEB := func(v int64) []byte {
		var out []byte
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if (v == 0 && (b&0x40) == 0) || (v == -1 && (b&0x40) != 0) {
				out = append(out, b)
				break
			}
			out = append(out, b|0x80)
		}
		return out
	}
	encodeName := func(s string) []byte {
		return append(encodeULEB(uint32(len(s))), []byte(s)...)
	}
	encodeSec := func(id byte, payload []byte) []byte {
		var out []byte
		out = append(out, id)
		out = append(out, encodeULEB(uint32(len(payload)))...)
		return append(out, payload...)
	}
	i32Const := func(v int32) []byte {
		return append([]byte{0x41}, encodeSLEB(int64(v))...)
	}

	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00})

	// Type section: type 0 = (i32,i32,i32)->i32, type 1 = ()->i32
	var typeSec bytes.Buffer
	typeSec.Write(encodeULEB(2))
	typeSec.Write([]byte{0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x01, 0x7f})
	typeSec.Write([]byte{0x60, 0x00, 0x01, 0x7f})
	buf.Write(encodeSec(1, typeSec.Bytes()))

	// Import section: stravin.set_verdict (func 0, type 0)
	var impSec bytes.Buffer
	impSec.Write(encodeULEB(1))
	impSec.Write(encodeName("stravin"))
	impSec.Write(encodeName("set_verdict"))
	impSec.WriteByte(0x00)
	impSec.Write(encodeULEB(0))
	buf.Write(encodeSec(2, impSec.Bytes()))

	// Function section: func 1 has type 1
	var fnSec bytes.Buffer
	fnSec.Write(encodeULEB(1))
	fnSec.Write(encodeULEB(1))
	buf.Write(encodeSec(3, fnSec.Bytes()))

	// Memory section: 1 page min
	buf.Write(encodeSec(5, []byte{0x01, 0x00, 0x01}))

	// Export section: "memory" (mem 0) + "run" (func 1)
	var expSec bytes.Buffer
	expSec.Write(encodeULEB(2))
	expSec.Write(encodeName("memory"))
	expSec.Write([]byte{0x02, 0x00})
	expSec.Write(encodeName("run"))
	expSec.Write([]byte{0x00, 0x01})
	buf.Write(encodeSec(7, expSec.Bytes()))

	// Code section: call stravin.set_verdict(decision, msgOffset, len(msg)); drop; i32.const 0; end
	var body bytes.Buffer
	body.WriteByte(0x00) // 0 locals
	body.Write(i32Const(decision))
	body.Write(i32Const(msgOffset))
	body.Write(i32Const(int32(len(msg))))
	body.Write([]byte{0x10, 0x00}) // call 0
	body.WriteByte(0x1a)           // drop
	body.Write(i32Const(0))
	body.WriteByte(0x0b) // end

	var codeSec bytes.Buffer
	codeSec.Write(encodeULEB(1))
	codeSec.Write(encodeULEB(uint32(body.Len())))
	codeSec.Write(body.Bytes())
	buf.Write(encodeSec(10, codeSec.Bytes()))

	// Data section: msg at msgOffset
	var dataSec bytes.Buffer
	dataSec.Write(encodeULEB(1))
	dataSec.WriteByte(0x00)
	dataSec.Write(i32Const(msgOffset))
	dataSec.WriteByte(0x0b)
	dataSec.Write(encodeULEB(uint32(len(msg))))
	dataSec.WriteString(msg)
	buf.Write(encodeSec(11, dataSec.Bytes()))

	return buf.Bytes()
}
