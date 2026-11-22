//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/admin"
	"stravin.dev/core/internal/compaction"
	"stravin.dev/core/internal/ctrl"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/hooks/async"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

// Scenario 1: Smart HTTP + SSH Git lifecycle (initial + incremental thin-pack).
func TestE2E_Scenario1_SmartHTTPAndSSHGitLifecycle(t *testing.T) {
	env := newStackEnv(t)
	if env.sshKeyPath == "" {
		t.Fatal("STRAVIN_E2E_SSH_KEY must be set to an authorized SSH private key path")
	}

	ctx := context.Background()
	repoID := uniqueRepoID("e2e-s1")
	if _, err := env.adminSvc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       "local",
		RepoID:         repoID,
		HomeRegion:     env.region,
		RefShards:      2,
		ManifestShards: 2,
	}); err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}

	httpURL := env.smartHTTPRepoURL("local", repoID)
	sshURL := env.sshRepoURL("local", repoID)

	// 1. Initial commit & push over Smart HTTP (:8080).
	srcDir, c1 := initLocalRepoWithCommit(t, httpURL, "README.md", "stravin core v1.0.0\n", "initial commit")
	mustRunGit(t, srcDir, nil, "push", "origin", "main")

	// 2. Fresh clone over Smart HTTP.
	httpCloneDir := filepath.Join(t.TempDir(), "http-clone")
	mustRunGit(t, "", nil, "clone", httpURL, httpCloneDir)
	if got := mustRunGit(t, httpCloneDir, nil, "rev-parse", "HEAD"); got != c1 {
		t.Fatalf("HTTP clone HEAD = %s, want %s", got, c1)
	}

	// 3. Incremental thin-pack commit & push over Smart HTTP (mutates existing file for delta compression).
	c2 := commitFile(t, srcDir, "README.md", "stravin core v1.0.0\nincremental thin pack update\n", "thin pack commit over HTTP")
	mustRunGit(t, srcDir, nil, "push", "origin", "main")

	// 4. Fetch incremental commit over Smart HTTP.
	mustRunGit(t, httpCloneDir, nil, "fetch", "origin")
	if got := mustRunGit(t, httpCloneDir, nil, "rev-parse", "origin/main"); got != c2 {
		t.Fatalf("HTTP fetch origin/main = %s, want %s", got, c2)
	}

	// 5. Clone over SSH (:2222).
	sshCloneDir := filepath.Join(t.TempDir(), "ssh-clone")
	mustRunGit(t, "", sshEnv(env.sshKeyPath), "clone", sshURL, sshCloneDir)
	if got := mustRunGit(t, sshCloneDir, nil, "rev-parse", "HEAD"); got != c2 {
		t.Fatalf("SSH clone HEAD = %s, want %s", got, c2)
	}

	// 6. Incremental thin-pack commit & push over SSH.
	c3 := commitFile(t, sshCloneDir, "README.md", "stravin core v1.0.0\nincremental thin pack update\nssh thin pack update\n", "thin pack commit over SSH")
	mustRunGit(t, sshCloneDir, sshEnv(env.sshKeyPath), "push", "origin", "main")

	// 7. Fetch over both Smart HTTP and SSH to verify full cross-protocol durability.
	mustRunGit(t, httpCloneDir, nil, "fetch", "origin")
	if got := mustRunGit(t, httpCloneDir, nil, "rev-parse", "origin/main"); got != c3 {
		t.Fatalf("HTTP fetch after SSH push = %s, want %s", got, c3)
	}
	mustRunGit(t, sshCloneDir, sshEnv(env.sshKeyPath), "fetch", "origin")
	if got := mustRunGit(t, sshCloneDir, nil, "rev-parse", "origin/main"); got != c3 {
		t.Fatalf("SSH fetch origin/main = %s, want %s", got, c3)
	}
}

// Scenario 2: Concurrent Ref-CAS conflict -> exactly 1 succeeds, 1 rejected cleanly, zero lost commits.
func TestE2E_Scenario2_ConcurrentRefCASConflict(t *testing.T) {
	env := newStackEnv(t)
	ctx := context.Background()

	repoID := uniqueRepoID("e2e-s2")
	if _, err := env.adminSvc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       "local",
		RepoID:         repoID,
		HomeRegion:     env.region,
		RefShards:      2,
		ManifestShards: 2,
	}); err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}

	httpURL := env.smartHTTPRepoURL("local", repoID)
	seedDir, _ := initLocalRepoWithCommit(t, httpURL, "base.txt", "base\n", "seed commit")
	mustRunGit(t, seedDir, nil, "push", "origin", "main")

	cloneA := filepath.Join(t.TempDir(), "client-a")
	cloneB := filepath.Join(t.TempDir(), "client-b")
	mustRunGit(t, "", nil, "clone", httpURL, cloneA)
	mustRunGit(t, "", nil, "clone", httpURL, cloneB)

	shaA := commitFile(t, cloneA, "a.txt", "from client A\n", "commit A")
	shaB := commitFile(t, cloneB, "b.txt", "from client B\n", "commit B")

	type pushResult struct {
		name string
		sha  string
		out  string
		err  error
	}
	start := make(chan struct{})
	results := make(chan pushResult, 2)

	pushWorker := func(name, dir, sha string) {
		<-start
		out, err := runGit(t, dir, nil, "push", "origin", "main")
		results <- pushResult{name: name, sha: sha, out: out, err: err}
	}

	go pushWorker("A", cloneA, shaA)
	go pushWorker("B", cloneB, shaB)
	close(start)

	r1 := <-results
	r2 := <-results

	var winner, loser *pushResult
	for _, r := range []*pushResult{&r1, &r2} {
		if r.err == nil {
			if winner != nil {
				t.Fatalf("expected exactly 1 concurrent push to succeed, both succeeded: A=%s B=%s", r1.out, r2.out)
			}
			winner = r
		} else {
			loser = r
		}
	}
	if winner == nil || loser == nil {
		t.Fatalf("expected 1 winner and 1 loser; got r1=(%v) r2=(%v)", r1.err, r2.err)
	}

	// Verify fresh clone contains the exact winning commit with zero lost commits.
	verifyDir := filepath.Join(t.TempDir(), "verify")
	mustRunGit(t, "", nil, "clone", httpURL, verifyDir)
	if got := mustRunGit(t, verifyDir, nil, "rev-parse", "HEAD"); got != winner.sha {
		t.Fatalf("remote HEAD = %s, want winner %s", got, winner.sha)
	}

	// Verify Ref History has exactly 2 commits (seed + winner).
	rk := tenant.RepoKey{TenantID: "local", RepoID: tenant.RepoID(repoID)}
	hist, err := env.ddbRepo.ListRefHistory(ctx, rk, "refs/heads/main", 2, "", 10)
	if err != nil {
		t.Fatalf("ListRefHistory: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("RefHistory count = %d, want 2 (seed + single CAS winner)", len(hist))
	}
}

// Scenario 3: Tier 1 Wasm hook ALLOW + REJECT (S3 cancel, zero DDB writes, side-band stderr).
func TestE2E_Scenario3_Tier1WasmHookAllowAndReject(t *testing.T) {
	env := newStackEnv(t)
	ctx := context.Background()

	repoID := uniqueRepoID("e2e-s3")
	rk := tenant.RepoKey{TenantID: "local", RepoID: tenant.RepoID(repoID)}
	if _, err := env.adminSvc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       "local",
		RepoID:         repoID,
		HomeRegion:     env.region,
		RefShards:      1,
		ManifestShards: 1,
	}); err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}

	// 1. Configure ALLOW Wasm hook via admin service (S3 hooks/{sha256}.wasm + DDB HOOKS).
	allowWasm := buildVerdictWasm(0, "allow-ok")
	if _, err := env.adminSvc.ConfigureWasmHook(ctx, admin.ConfigureWasmHookInput{
		TenantID:  "local",
		RepoID:    repoID,
		HookID:    "e2e-allow",
		WasmBytes: allowWasm,
		RefGlob:   "refs/heads/*",
		FailMode:  ddb.FailModeClosed,
	}); err != nil {
		t.Fatalf("ConfigureWasmHook(ALLOW): %v", err)
	}

	httpURL := env.smartHTTPRepoURL("local", repoID)
	workDir, c1 := initLocalRepoWithCommit(t, httpURL, "hello.txt", "allowed\n", "commit allowed by wasm")
	mustRunGit(t, workDir, nil, "push", "origin", "main")

	histBefore, err := env.ddbRepo.ListRefHistory(ctx, rk, "refs/heads/main", 1, "", 10)
	if err != nil || len(histBefore) != 1 {
		t.Fatalf("expected 1 RefHistory after ALLOW push, got len=%d err=%v", len(histBefore), err)
	}
	manifestsBefore, err := env.ddbRepo.QueryManifestShard(ctx, rk, 0, "", true)
	if err != nil || len(manifestsBefore) != 1 {
		t.Fatalf("expected 1 active manifest after ALLOW push, got len=%d err=%v", len(manifestsBefore), err)
	}

	// 2. Replace with REJECT Wasm hook emitting a distinctive diagnostic message.
	const rejectMsg = "E2E-WASM-REJECT: policy violation"
	rejectWasm := buildVerdictWasm(1, rejectMsg)
	if _, err := env.adminSvc.ConfigureWasmHook(ctx, admin.ConfigureWasmHookInput{
		TenantID:  "local",
		RepoID:    repoID,
		HookID:    "e2e-reject",
		WasmBytes: rejectWasm,
		RefGlob:   "refs/heads/*",
		FailMode:  ddb.FailModeClosed,
	}); err != nil {
		t.Fatalf("ConfigureWasmHook(REJECT): %v", err)
	}

	commitFile(t, workDir, "secret.txt", "blocked\n", "commit blocked by wasm")
	pushOut, pushErr := runGit(t, workDir, nil, "push", "origin", "main")
	if pushErr == nil {
		t.Fatalf("expected push to be rejected by Tier 1 Wasm hook, but succeeded:\n%s", pushOut)
	}
	if !strings.Contains(pushOut, rejectMsg) {
		t.Fatalf("expected Git side-band stderr to contain %q, got:\n%s", rejectMsg, pushOut)
	}

	// Verify ZERO DynamoDB writes occurred on REJECT.
	refAfter, err := env.ddbRepo.GetRef(ctx, rk, "refs/heads/main", 1)
	if err != nil || refAfter.OID != c1 {
		t.Fatalf("ref after REJECT = %+v (err=%v), want unchanged OID %s", refAfter, err, c1)
	}
	histAfter, err := env.ddbRepo.ListRefHistory(ctx, rk, "refs/heads/main", 1, "", 10)
	if err != nil || len(histAfter) != 1 {
		t.Fatalf("RefHistory count after REJECT = %d (err=%v), want 1 (zero new writes)", len(histAfter), err)
	}
	manifestsAfter, err := env.ddbRepo.QueryManifestShard(ctx, rk, 0, "", true)
	if err != nil || len(manifestsAfter) != 1 {
		t.Fatalf("active manifests after REJECT = %d (err=%v), want 1 (zero new manifests)", len(manifestsAfter), err)
	}
}

// Scenario 4: Tier 2 stravin-hook async outbox -> HMAC-SHA256 webhook delivery + retry & DLQ.
func TestE2E_Scenario4_Tier2AsyncWebhookAndDLQ(t *testing.T) {
	env := newStackEnv(t)
	env.checkHealthz(t, env.hookURL)

	ctx := context.Background()
	repoID := uniqueRepoID("e2e-s4")
	if _, err := env.adminSvc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       "local",
		RepoID:         repoID,
		HomeRegion:     env.region,
		RefShards:      1,
		ManifestShards: 1,
	}); err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}

	var (
		mu             sync.Mutex
		deliveredBody  []byte
		deliveredSig   string
		deliveredEvent string
		okHits         atomic.Int32
		failHits       atomic.Int32
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		deliveredBody = body
		deliveredSig = r.Header.Get(async.HeaderSignature256)
		deliveredEvent = r.Header.Get(async.HeaderEvent)
		mu.Unlock()
		okHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/fail", func(w http.ResponseWriter, _ *http.Request) {
		failHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	// Bind on 0.0.0.0 so the stravin-hook Docker container can connect via host.docker.internal.
	ln, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen webhook receiver: %v", err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	port := ln.Addr().(*net.TCPAddr).Port
	baseCallback := fmt.Sprintf("http://%s:%d", env.webhookHost, port)
	secret := "e2e-kms-webhook-secret-" + repoID

	if _, err := env.adminSvc.ConfigureWebhook(ctx, admin.ConfigureWebhookInput{
		TenantID:         "local",
		RepoID:           repoID,
		WebhookID:        "wh-ok",
		URL:              baseCallback + "/ok",
		Secret:           secret,
		KMSKeyID:         env.kmsKeyID,
		RefGlob:          "refs/heads/*",
		Enabled:          true,
		MaxAttempts:      2,
		InitialBackoffMS: 25,
		MaxBackoffMS:     100,
	}); err != nil {
		t.Fatalf("ConfigureWebhook(wh-ok): %v", err)
	}
	if _, err := env.adminSvc.ConfigureWebhook(ctx, admin.ConfigureWebhookInput{
		TenantID:         "local",
		RepoID:           repoID,
		WebhookID:        "wh-fail",
		URL:              baseCallback + "/fail",
		Secret:           secret,
		KMSKeyID:         env.kmsKeyID,
		RefGlob:          "refs/heads/*",
		Enabled:          true,
		MaxAttempts:      2,
		InitialBackoffMS: 25,
		MaxBackoffMS:     100,
		Append:           true,
	}); err != nil {
		t.Fatalf("ConfigureWebhook(wh-fail): %v", err)
	}

	// Push via Smart HTTP to trigger DDB Ref History -> Floci Streams -> stravin-hook container.
	httpURL := env.smartHTTPRepoURL("local", repoID)
	workDir, commitSHA := initLocalRepoWithCommit(t, httpURL, "hook.txt", "trigger webhook\n", "trigger tier 2 webhook")
	mustRunGit(t, workDir, nil, "push", "origin", "main")

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if okHits.Load() >= 1 && failHits.Load() >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if okHits.Load() < 1 {
		t.Fatalf("expected signed webhook delivery to %s/ok, got %d hits", baseCallback, okHits.Load())
	}
	if failHits.Load() < 2 {
		t.Fatalf("expected >=2 retry attempts to %s/fail, got %d hits", baseCallback, failHits.Load())
	}

	mu.Lock()
	bodyCopy := append([]byte(nil), deliveredBody...)
	sigCopy := deliveredSig
	eventCopy := deliveredEvent
	mu.Unlock()

	if eventCopy != async.EventPush {
		t.Fatalf("X-Stravin-Event = %q, want %q", eventCopy, async.EventPush)
	}
	if !async.VerifySignature([]byte(secret), bodyCopy, sigCopy) {
		t.Fatalf("HMAC-SHA256 signature verification failed (sig=%q body=%s)", sigCopy, string(bodyCopy))
	}
	var payload async.WebhookPayload
	if err := json.Unmarshal(bodyCopy, &payload); err != nil {
		t.Fatalf("unmarshal webhook payload: %v", err)
	}
	if payload.TenantID != "local" || payload.RepoID != repoID || payload.Ref != "refs/heads/main" || payload.OID != commitSHA {
		t.Fatalf("unexpected webhook payload: %+v (want OID %s)", payload, commitSHA)
	}

	// Verify DLQ entry persisted in Floci DynamoDB for wh-fail.
	var dlqFound bool
	dlqDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(dlqDeadline) {
		qOut, err := env.ddbClient.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(env.ddbTable),
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :sk)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("T#local#REPO#%s#DLQ", repoID)},
				":sk": &ddbtypes.AttributeValueMemberS{Value: "HOOK#wh-fail#"},
			},
		})
		if err == nil && len(qOut.Items) > 0 {
			dlqFound = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !dlqFound {
		t.Fatal("expected DLQ record in Floci DynamoDB for failing webhook wh-fail")
	}
}

// Scenario 5: stravin-pack compaction (.midx + commit-graph + 2-phase swap) & delayed GC.
func TestE2E_Scenario5_StravinPackCompactionAndDelayedGC(t *testing.T) {
	env := newStackEnv(t)
	env.checkHealthz(t, env.packURL)

	ctx := context.Background()
	repoID := uniqueRepoID("e2e-s5")
	rk := tenant.RepoKey{TenantID: "local", RepoID: tenant.RepoID(repoID)}
	if _, err := env.adminSvc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       "local",
		RepoID:         repoID,
		HomeRegion:     env.region,
		RefShards:      1,
		ManifestShards: 1,
	}); err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}

	// Push 3 real commits over Smart HTTP to produce 3 active packfiles in Floci S3 + DDB.
	httpURL := env.smartHTTPRepoURL("local", repoID)
	workDir, _ := initLocalRepoWithCommit(t, httpURL, "f1.txt", "one\n", "commit 1")
	mustRunGit(t, workDir, nil, "push", "origin", "main")
	commitFile(t, workDir, "f2.txt", "two\n", "commit 2")
	mustRunGit(t, workDir, nil, "push", "origin", "main")
	c3 := commitFile(t, workDir, "f3.txt", "three\n", "commit 3")
	mustRunGit(t, workDir, nil, "push", "origin", "main")

	activeBefore, err := env.ddbRepo.QueryManifestShard(ctx, rk, 0, "", true)
	if err != nil || len(activeBefore) != 3 {
		t.Fatalf("expected 3 active manifests before compaction, got %d (err=%v)", len(activeBefore), err)
	}

	// Plant 1 orphaned pack in S3 (no DDB manifest entry, simulating a crashed/rejected upload).
	orphanData := []byte("ORPHAN-PACK-PAYLOAD-" + repoID)
	orphanSum := sha256.Sum256(orphanData)
	orphanSHA := hex.EncodeToString(orphanSum[:])
	if err := env.s3Store.PutPack(ctx, rk, orphanSHA, orphanData, []byte("ORPHAN-IDX")); err != nil {
		t.Fatalf("PutPack(orphan): %v", err)
	}

	gcAdapter, err := compaction.NewDDBManifestGCAdapter(env.ddbClient, env.ddbTable)
	if err != nil {
		t.Fatalf("NewDDBManifestGCAdapter: %v", err)
	}
	leaseMgr, err := compaction.NewLeaseManager(env.ddbRepo, compaction.LeaseManagerConfig{
		OwnerID: "e2e-pack-" + repoID,
		TTL:     15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	tier0, err := compaction.NewTier0Strategy(env.s3Store, pack.DefaultLimits())
	if err != nil {
		t.Fatalf("NewTier0Strategy: %v", err)
	}
	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{DefaultTimeout: 30 * time.Second})
	tier1, err := compaction.NewTier1Strategy(env.s3Store, gitExec, compaction.Tier1Config{
		WorkDir: t.TempDir(),
		Timeout: 30 * time.Second,
		Limits:  pack.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("NewTier1Strategy: %v", err)
	}
	engine, err := compaction.NewEngine(compaction.EngineConfig{
		Leases:        leaseMgr,
		Manifests:     env.ddbRepo,
		ManifestGC:    gcAdapter,
		Refs:          env.ddbRepo,
		GCGraceWindow: 10 * time.Millisecond,
		Strategies: map[compaction.Tier]compaction.CompactionStrategy{
			compaction.Tier0: tier0,
			compaction.Tier1: tier1,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	// Run Tier-0 merge + 2-phase manifest swap.
	res0, err := engine.CompactShard(ctx, compaction.ShardTarget{
		Repo:           rk,
		Shard:          0,
		ManifestShards: 1,
		RefShards:      1,
		OriginRegion:   lease.Region(env.region),
		Tier:           compaction.Tier0,
	})
	if err != nil || res0.Skipped || res0.InputPacks != 3 {
		t.Fatalf("CompactShard(Tier0) = %+v err=%v", res0, err)
	}

	// Run Tier-1 deep repack generating .midx + commit-graph.
	time.Sleep(5 * time.Millisecond)
	res1, err := engine.CompactShard(ctx, compaction.ShardTarget{
		Repo:           rk,
		Shard:          0,
		ManifestShards: 1,
		RefShards:      1,
		OriginRegion:   lease.Region(env.region),
		Tier:           compaction.Tier1,
		Epoch:          "e2e-epoch-1",
	})
	if err != nil || res1.AuxMIDXKey == "" || res1.AuxGraphKey == "" {
		t.Fatalf("CompactShard(Tier1) = %+v err=%v", res1, err)
	}
	midxBytes, err := env.s3Store.GetAuxMIDX(ctx, rk, res1.OutputPackID, "e2e-epoch-1")
	if err != nil || !bytes.HasPrefix(midxBytes, []byte("MIDX")) {
		t.Fatalf("GetAuxMIDX len=%d err=%v", len(midxBytes), err)
	}
	graphBytes, err := env.s3Store.GetAuxGraph(ctx, rk, res1.OutputPackID, "e2e-epoch-1")
	if err != nil || !bytes.HasPrefix(graphBytes, []byte("CGPH")) {
		t.Fatalf("GetAuxGraph len=%d err=%v", len(graphBytes), err)
	}

	// Wait >1s for Floci S3 LastModified 1s resolution, then sweep COMPACTED + orphaned packs.
	time.Sleep(1100 * time.Millisecond)
	sweeper, err := compaction.NewGCSweeper(compaction.GCSweeperConfig{
		Manifests:         env.ddbRepo,
		ManifestGC:        gcAdapter,
		Objects:           env.s3Store,
		GraceWindow:       10 * time.Millisecond,
		OrphanGraceWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}
	gcRes, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo: %v", err)
	}
	if gcRes.CompactedReaped < 3 || gcRes.OrphanedReaped != 1 {
		t.Fatalf("unexpected GCResult: %+v (want CompactedReaped>=3, OrphanedReaped=1)", gcRes)
	}
	if _, err := env.s3Store.GetPack(ctx, rk, orphanSHA); !errors.Is(err, s3.ErrNotFound) {
		t.Fatalf("expected orphaned pack deleted from S3, got err=%v", err)
	}

	// Verify fresh git clone through stravin-edge -> stravin-node still serves all 3 commits!
	cloneDir := filepath.Join(t.TempDir(), "post-compact-clone")
	mustRunGit(t, "", nil, "clone", httpURL, cloneDir)
	if got := mustRunGit(t, cloneDir, nil, "rev-parse", "HEAD"); got != c3 {
		t.Fatalf("post-compaction clone HEAD = %s, want %s", got, c3)
	}
}

// Scenario 6: stravin-ctrl failover_epoch fencing + fault-injected S3 pack deletion scrub & rewind.
func TestE2E_Scenario6_StravinCtrlFencingAndScrubRewind(t *testing.T) {
	env := newStackEnv(t)
	env.checkHealthz(t, env.ctrlURL)

	ctx := context.Background()
	repoID := uniqueRepoID("e2e-s6")
	rk := tenant.RepoKey{TenantID: "local", RepoID: tenant.RepoID(repoID)}
	if _, err := env.adminSvc.RegisterRepo(ctx, admin.RegisterRepoInput{
		TenantID:       "local",
		RepoID:         repoID,
		HomeRegion:     env.region,
		RefShards:      1,
		ManifestShards: 1,
	}); err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}

	// 1. Push commit 1 (P1) and commit 2 (P2) via Smart HTTP.
	httpURL := env.smartHTTPRepoURL("local", repoID)
	workDir, c1 := initLocalRepoWithCommit(t, httpURL, "state.txt", "v1\n", "commit 1")
	mustRunGit(t, workDir, nil, "push", "origin", "main")
	c2 := commitFile(t, workDir, "state.txt", "v2\n", "commit 2")
	mustRunGit(t, workDir, nil, "push", "origin", "main")

	// Find P2's pack ID from Ref History.
	hist, err := env.ddbRepo.ListRefHistory(ctx, rk, "refs/heads/main", 1, "", 10)
	if err != nil || len(hist) != 2 {
		t.Fatalf("expected 2 RefHistory entries, got len=%d err=%v", len(hist), err)
	}
	var pack2ID string
	for _, entry := range hist {
		if entry.OID == c2 {
			pack2ID = entry.PackID
		}
	}
	if pack2ID == "" {
		t.Fatalf("could not locate pack_id for commit %s in RefHistory: %+v", c2, hist)
	}

	// 2. Verify stravin-ctrl holds the active write lease, then advance failover_epoch via HTTP API.
	httpClient := &http.Client{Timeout: 10 * time.Second}
	statusResp, err := httpClient.Get(env.ctrlURL + "/v1/lease/status")
	if err != nil {
		t.Fatalf("GET /v1/lease/status: %v", err)
	}
	var leaseStatus ctrl.LeaseStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&leaseStatus); err != nil {
		_ = statusResp.Body.Close()
		t.Fatalf("decode lease status: %v", err)
	}
	_ = statusResp.Body.Close()
	if !leaseStatus.Valid || !leaseStatus.AllowsWrites || leaseStatus.Epoch < 1 {
		t.Fatalf("expected stravin-ctrl to hold active lease, got %+v", leaseStatus)
	}

	prevEpoch := leaseStatus.Epoch
	advBody, _ := json.Marshal(map[string]any{
		"tenant_id": "local",
		"repo_id":   repoID,
		"mode":      "ACTIVE",
	})
	advResp, err := httpClient.Post(env.ctrlURL+"/v1/epoch/advance", "application/json", bytes.NewReader(advBody))
	if err != nil {
		t.Fatalf("POST /v1/epoch/advance: %v", err)
	}
	var fenceRes ctrl.FenceResult
	if err := json.NewDecoder(advResp.Body).Decode(&fenceRes); err != nil {
		_ = advResp.Body.Close()
		t.Fatalf("decode fence result: %v", err)
	}
	_ = advResp.Body.Close()
	if advResp.StatusCode != http.StatusOK || fenceRes.CurrentEpoch <= prevEpoch {
		t.Fatalf("POST /v1/epoch/advance status=%d res=%+v (prevEpoch=%d)", advResp.StatusCode, fenceRes, prevEpoch)
	}

	// Verify stale epoch writer is fenced both by lease.VerifyEpochFence and by DDB META epoch.
	if err := lease.VerifyEpochFence(fenceRes.CurrentEpoch, prevEpoch); !errors.Is(err, lease.ErrStaleEpoch) {
		t.Fatalf("VerifyEpochFence(current=%d, stale=%d) = %v, want ErrStaleEpoch", fenceRes.CurrentEpoch, prevEpoch, err)
	}
	metaAfterFence, err := env.ddbRepo.GetMeta(ctx, rk)
	if err != nil || metaAfterFence.FailoverEpoch != fenceRes.CurrentEpoch {
		t.Fatalf("META failover_epoch = %d (err=%v), want %d", metaAfterFence.FailoverEpoch, err, fenceRes.CurrentEpoch)
	}

	// 3. Fault injection: delete P2's .pack object from Floci S3!
	pack2Key, err := s3.PackKey(rk, pack2ID)
	if err != nil {
		t.Fatalf("s3.PackKey: %v", err)
	}
	if err := env.s3Store.DeleteObject(ctx, pack2Key); err != nil {
		t.Fatalf("DeleteObject(%s): %v", pack2Key, err)
	}

	// 4. Trigger POST /v1/scrub on stravin-ctrl.
	scrubBody, _ := json.Marshal(map[string]any{
		"tenant_id":        "local",
		"repo_id":          repoID,
		"lookback_seconds": 1200,
	})
	scrubResp, err := httpClient.Post(env.ctrlURL+"/v1/scrub", "application/json", bytes.NewReader(scrubBody))
	if err != nil {
		t.Fatalf("POST /v1/scrub: %v", err)
	}
	var report ctrl.ScrubReport
	if err := json.NewDecoder(scrubResp.Body).Decode(&report); err != nil {
		_ = scrubResp.Body.Close()
		t.Fatalf("decode ScrubReport: %v", err)
	}
	_ = scrubResp.Body.Close()
	if scrubResp.StatusCode != http.StatusOK || report.Status != "rewound" {
		t.Fatalf("POST /v1/scrub status=%d report=%+v, want status=rewound", scrubResp.StatusCode, report)
	}
	if len(report.MissingPackIDs) != 1 || report.MissingPackIDs[0] != pack2ID {
		t.Fatalf("missing_pack_ids = %v, want [%s]", report.MissingPackIDs, pack2ID)
	}

	// 5. Verify deterministic Ref History rewind to c1 in DDB and via fresh Smart HTTP clone.
	mainRef, err := env.ddbRepo.GetRef(ctx, rk, "refs/heads/main", 1)
	if err != nil || mainRef.OID != c1 {
		t.Fatalf("main ref after scrub = %+v (err=%v), want rewound OID %s", mainRef, err, c1)
	}

	rewoundClone := filepath.Join(t.TempDir(), "rewound-clone")
	mustRunGit(t, "", nil, "clone", httpURL, rewoundClone)
	if got := mustRunGit(t, rewoundClone, nil, "rev-parse", "HEAD"); got != c1 {
		t.Fatalf("clone HEAD after scrub rewind = %s, want %s", got, c1)
	}

	// 6. Verify immutable SCRUB_AUDIT entry exists in Floci DynamoDB.
	auditKey, err := ctrl.ScrubAuditKey(report)
	if err != nil {
		t.Fatalf("ScrubAuditKey: %v", err)
	}
	auditOut, err := env.ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(env.ddbTable),
		Key:            auditKey.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil || len(auditOut.Item) == 0 {
		t.Fatalf("expected SCRUB_AUDIT item in Floci DynamoDB, got len=%d err=%v", len(auditOut.Item), err)
	}
}
