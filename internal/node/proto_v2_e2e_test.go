package node_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/edge"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/node"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

func TestAdvertiseV2Capabilities_SHA1AndSHA256(t *testing.T) {
	for _, tc := range []struct {
		format     ddb.ObjectFormat
		wantFormat string
	}{
		{format: "", wantFormat: "object-format=sha1"},
		{format: ddb.ObjectFormatSHA1, wantFormat: "object-format=sha1"},
		{format: ddb.ObjectFormatSHA256, wantFormat: "object-format=sha256"},
	} {
		out := string(node.AdvertiseV2Capabilities(node.AdvertiseOptions{
			IncludeServiceHeader: true,
			ServiceName:          "git-upload-pack",
			ObjectFormat:         tc.format,
		}))
		for _, want := range []string{
			"001e# service=git-upload-pack\n0000",
			"000eversion 2\n",
			"ls-refs=unborn\n",
			"fetch=shallow wait-for-done filter\n",
			tc.wantFormat + "\n",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("format=%q missing %q in %q", tc.format, want, out)
			}
		}
		if !strings.HasSuffix(out, "0000") {
			t.Errorf("format=%q missing trailing flush-pkt in %q", tc.format, out)
		}
	}
}

// inMemoryStack wires a real stravin-edge (HTTP + SSH) -> gRPC -> stravin-node
// (Server + ReceivePackHandler + Materializer + gitexec) stack backed by
// in-memory DynamoDB and S3 stores so real `git` CLI commands exercise the full
// Wire Protocol v2, v0/v1 fallback, partial clone, and SHA-1/SHA-256 paths.
type inMemoryStack struct {
	t         *testing.T
	httpURL   string
	sshAddr   string
	sshKey    string
	authToken string
	ddb       *memDDBStore
	s3        *memS3Store
	cleanup   []func()
}

func newInMemoryStack(t *testing.T) *inMemoryStack {
	return newInMemoryStackWithOptions(t, nil).inMemoryStack
}

func newInMemoryStackWithOptions(t *testing.T, bundleResolver node.BundleURIResolver) *inMemoryStackWithCache {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	region := lease.Region("us-east-1")
	ddbStore := newMemDDBStore(region)
	s3Store := newMemS3Store()

	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := node.NewMaterializer(node.MaterializerConfig{
		Meta:  ddbStore,
		Packs: s3Store,
		Cache: nvmeCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	refresher, err := node.NewLeaseRefresher(node.LeaseRefresherConfig{
		Store:        ddbStore,
		Region:       region,
		SafetyMargin: time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("LeaseRefresher.RefreshOnce: %v", err)
	}

	recvHandler, err := node.NewReceivePackHandler(node.ReceivePackConfig{
		Materializer:   mat,
		Cache:          nvmeCache,
		Packs:          s3Store,
		Committer:      ddbStore,
		LeaseRefresher: refresher,
		Region:         region,
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})

	nodeSrv, err := node.NewServer(node.ServerConfig{
		Materializer:   mat,
		GitExecutor:    gitExec,
		WriteHandler:   recvHandler,
		BundleResolver: bundleResolver,
		LeaseRefresher: refresher,
		Region:         string(region),
	})
	if err != nil {
		t.Fatalf("node.NewServer: %v", err)
	}

	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	grpcSrv := grpc.NewServer(grpc.ForceServerCodec(rpc.WireCodec()))
	rpc.RegisterGitWireServer(grpcSrv, nodeSrv)
	go func() { _ = grpcSrv.Serve(grpcLn) }()

	// Generate client + host SSH keys.
	_, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ssh key: %v", err)
	}
	clientSigner, err := ssh.NewSignerFromKey(clientPriv)
	if err != nil {
		t.Fatalf("ssh signer: %v", err)
	}
	privPEM, err := ssh.MarshalPrivateKey(clientPriv, "")
	if err != nil {
		t.Fatalf("marshal ssh private key: %v", err)
	}
	sshKeyPath := filepath.Join(t.TempDir(), "id_ed25519")
	if err := os.WriteFile(sshKeyPath, pem.EncodeToMemory(privPEM), 0o600); err != nil {
		t.Fatalf("write ssh key: %v", err)
	}

	hostKey, err := edge.LoadOrGenerateHostKey("")
	if err != nil {
		t.Fatalf("LoadOrGenerateHostKey: %v", err)
	}

	authToken := "stravin-e2e-token"
	auth, err := edge.NewStaticAuthenticator(
		[]edge.TokenEntry{{Token: authToken, Principal: edge.Principal{ActorID: "dev-1", TenantID: "acme"}}},
		[]edge.SSHKeyEntry{{Key: clientSigner.PublicKey(), Principal: edge.Principal{ActorID: "dev-1", TenantID: "acme"}}},
	)
	if err != nil {
		t.Fatalf("NewStaticAuthenticator: %v", err)
	}

	leaseVerifier := lease.NewStoreVerifier(ddbStore, lease.RealClock{}, 0)
	resolver, err := edge.NewResolver(region, ddbStore, leaseVerifier, time.Second, lease.RealClock{})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	router := edge.NewRendezvousRouter([]string{grpcLn.Addr().String()})
	dialer := edge.NewGRPCNodeDialer()
	proxy, err := edge.NewWireProxy(dialer)
	if err != nil {
		t.Fatalf("NewWireProxy: %v", err)
	}

	httpHandler, err := edge.NewHTTPHandler(auth, nil, resolver, router, proxy, nil, nil)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	httpSrv := httptest.NewServer(httpHandler)

	sshSrv, err := edge.NewSSHServer(hostKey, auth, nil, resolver, router, proxy, nil, nil)
	if err != nil {
		t.Fatalf("NewSSHServer: %v", err)
	}
	sshLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ssh: %v", err)
	}
	go func() { _ = sshSrv.Serve(sshLn) }()

	s := &inMemoryStack{
		t:         t,
		httpURL:   httpSrv.URL,
		sshAddr:   sshLn.Addr().String(),
		sshKey:    sshKeyPath,
		authToken: authToken,
		ddb:       ddbStore,
		s3:        s3Store,
		cleanup: []func(){
			func() { _ = sshSrv.Close() },
			httpSrv.Close,
			func() { _ = dialer.Close() },
			grpcSrv.Stop,
			func() { _ = grpcLn.Close() },
		},
	}
	t.Cleanup(func() {
		for _, fn := range s.cleanup {
			fn()
		}
	})
	return &inMemoryStackWithCache{
		inMemoryStack: s,
		nvmeCache:     nvmeCache,
	}
}

func (s *inMemoryStack) createRepo(repoID string, format ddb.ObjectFormat) tenant.RepoKey {
	rk, err := tenant.NewRepoKey("acme", repoID)
	if err != nil {
		s.t.Fatalf("NewRepoKey: %v", err)
	}
	s.ddb.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
		ObjectFormat:   format,
	})
	return rk
}

func (s *inMemoryStack) httpRepoURL(repoID string) string {
	return fmt.Sprintf("%s/acme/%s.git", s.httpURL, repoID)
}

func (s *inMemoryStack) sshRepoURL(repoID string) string {
	return fmt.Sprintf("ssh://git@%s/acme/%s.git", s.sshAddr, repoID)
}

func (s *inMemoryStack) runGit(dir string, extraEnv []string, args ...string) string {
	s.t.Helper()
	fullArgs := append([]string{
		"-c", "http.extraHeader=Authorization: Bearer " + s.authToken,
		"-c", "user.name=Stravin Test",
		"-c", "user.email=test@stravin.dev",
	}, args...)
	cmd := exec.Command("git", fullArgs...)
	if dir != "" {
		cmd.Dir = dir
	}
	sshCmd := fmt.Sprintf(
		"ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o IdentitiesOnly=yes -o SendEnv=GIT_PROTOCOL",
		s.sshKey,
	)
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_SSH_COMMAND="+sshCmd,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		s.t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return string(out)
}

func TestWireProtocolV2_HTTP_SSH_Fallback_PartialClone_And_DualHash(t *testing.T) {
	stack := newInMemoryStack(t)
	stack.createRepo("repo-sha1", ddb.ObjectFormatSHA1)
	stack.createRepo("repo-sha256", ddb.ObjectFormatSHA256)

	for _, tc := range []struct {
		name      string
		repoID    string
		objFormat string
		oidHexLen int
	}{
		{name: "SHA1", repoID: "repo-sha1", objFormat: "sha1", oidHexLen: 40},
		{name: "SHA256", repoID: "repo-sha256", objFormat: "sha256", oidHexLen: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpURL := stack.httpRepoURL(tc.repoID)
			sshURL := stack.sshRepoURL(tc.repoID)

			// 1. Seed local repo with matching object-format and push over HTTP with Protocol v2.
			seedDir := filepath.Join(t.TempDir(), "seed")
			stack.runGit("", nil, "init", "--initial-branch=main", "--object-format="+tc.objFormat, seedDir)
			if err := os.WriteFile(filepath.Join(seedDir, "README.md"), []byte("hello "+tc.objFormat+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(seedDir, "large.txt"), []byte(strings.Repeat("stravin-v2-", 200)), 0o600); err != nil {
				t.Fatal(err)
			}
			stack.runGit(seedDir, nil, "add", "-A")
			stack.runGit(seedDir, nil, "commit", "-m", "initial "+tc.objFormat+" commit")
			commit1 := strings.TrimSpace(stack.runGit(seedDir, nil, "rev-parse", "HEAD"))
			if len(commit1) != tc.oidHexLen {
				t.Fatalf("commit1 len = %d (%q), want %d", len(commit1), commit1, tc.oidHexLen)
			}
			stack.runGit(seedDir, nil, "-c", "protocol.version=2", "push", httpURL, "HEAD:refs/heads/main")

			// 2. Wire Protocol v2 over Smart HTTP: ls-remote, clone, and partial clone (--filter=blob:none).
			lsOut := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=2", "ls-remote", httpURL)
			if !strings.Contains(lsOut, "version 2") || !strings.Contains(lsOut, "command=ls-refs") {
				t.Fatalf("HTTP v2 ls-remote did not negotiate v2:\n%s", lsOut)
			}
			if !strings.Contains(lsOut, commit1+"\trefs/heads/main") {
				t.Fatalf("HTTP v2 ls-remote missing commit1 %s:\n%s", commit1, lsOut)
			}

			cloneV2HTTP := filepath.Join(t.TempDir(), "clone-v2-http")
			cloneV2Out := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=2", "clone", httpURL, cloneV2HTTP)
			if !strings.Contains(cloneV2Out, "version 2") || !strings.Contains(cloneV2Out, "command=fetch") {
				t.Fatalf("HTTP v2 clone did not negotiate v2 fetch:\n%s", cloneV2Out)
			}
			stack.runGit(cloneV2HTTP, nil, "fsck", "--full", "--strict")

			partialHTTP := filepath.Join(t.TempDir(), "partial-http")
			partialOut := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=2", "clone", "--filter=blob:none", "--no-checkout", httpURL, partialHTTP)
			if !strings.Contains(partialOut, "filter blob:none") {
				t.Fatalf("HTTP v2 partial clone did not send filter blob:none:\n%s", partialOut)
			}
			// Checkout triggers on-demand promisor fetch of filtered blobs over v2.
			stack.runGit(partialHTTP, nil, "-c", "protocol.version=2", "checkout", "main")
			gotReadme, err := os.ReadFile(filepath.Join(partialHTTP, "README.md"))
			if err != nil || string(gotReadme) != "hello "+tc.objFormat+"\n" {
				t.Fatalf("partial clone checkout README.md = (%q, %v)", string(gotReadme), err)
			}

			// 3. Automatic v0/v1 fallback over Smart HTTP.
			cloneV0HTTP := filepath.Join(t.TempDir(), "clone-v0-http")
			v0Out := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=0", "clone", httpURL, cloneV0HTTP)
			if strings.Contains(v0Out, "git< version 2") {
				t.Fatalf("HTTP v0 clone unexpectedly received v2:\n%s", v0Out)
			}
			stack.runGit(cloneV0HTTP, nil, "fsck", "--full", "--strict")

			// 4. Push a second commit over SSH (Protocol v2), then fetch over HTTP and clone over SSH (v2 + partial + v0).
			if _, err := exec.LookPath("ssh"); err != nil {
				t.Skip("ssh client binary not installed")
			}
			if err := os.WriteFile(filepath.Join(cloneV2HTTP, "second.txt"), []byte("second commit via ssh\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			stack.runGit(cloneV2HTTP, nil, "add", "-A")
			stack.runGit(cloneV2HTTP, nil, "commit", "-m", "second commit")
			commit2 := strings.TrimSpace(stack.runGit(cloneV2HTTP, nil, "rev-parse", "HEAD"))
			stack.runGit(cloneV2HTTP, nil, "-c", "protocol.version=2", "push", sshURL, "HEAD:refs/heads/main")

			// Fetch updated ref over HTTP v2 in v0 clone.
			stack.runGit(cloneV0HTTP, nil, "-c", "protocol.version=2", "fetch", "origin", "main")
			fetchedOID := strings.TrimSpace(stack.runGit(cloneV0HTTP, nil, "rev-parse", "FETCH_HEAD"))
			if fetchedOID != commit2 {
				t.Fatalf("HTTP v2 fetch FETCH_HEAD = %s, want %s", fetchedOID, commit2)
			}

			// Clone over SSH with Protocol v2 + partial clone filter.
			cloneV2SSH := filepath.Join(t.TempDir(), "clone-v2-ssh")
			sshV2Out := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=2", "clone", "--filter=blob:none", sshURL, cloneV2SSH)
			if !strings.Contains(sshV2Out, "version 2") || !strings.Contains(sshV2Out, "filter blob:none") {
				t.Fatalf("SSH v2 partial clone did not negotiate v2 + filter:\n%s", sshV2Out)
			}
			if got := strings.TrimSpace(stack.runGit(cloneV2SSH, nil, "rev-parse", "HEAD")); got != commit2 {
				t.Fatalf("SSH v2 clone HEAD = %s, want %s", got, commit2)
			}

			// Automatic v0 fallback over SSH.
			cloneV0SSH := filepath.Join(t.TempDir(), "clone-v0-ssh")
			sshV0Out := stack.runGit("", []string{"GIT_TRACE_PACKET=1"}, "-c", "protocol.version=0", "clone", sshURL, cloneV0SSH)
			if strings.Contains(sshV0Out, "git< version 2") {
				t.Fatalf("SSH v0 clone unexpectedly received v2:\n%s", sshV0Out)
			}
			stack.runGit(cloneV0SSH, nil, "fsck", "--full", "--strict")
		})
	}
}

// --- In-memory DynamoDB & S3 fakes for end-to-end tests ---

type memDDBStore struct {
	mu        sync.RWMutex
	region    lease.Region
	metas     map[tenant.RepoKey]ddb.RepoMeta
	refs      map[tenant.RepoKey]map[string]string
	manifests map[tenant.RepoKey][]ddb.PackManifest
	seq       int
}

func newMemDDBStore(region lease.Region) *memDDBStore {
	return &memDDBStore{
		region:    region,
		metas:     make(map[tenant.RepoKey]ddb.RepoMeta),
		refs:      make(map[tenant.RepoKey]map[string]string),
		manifests: make(map[tenant.RepoKey][]ddb.PackManifest),
	}
}

func (m *memDDBStore) PutMeta(meta ddb.RepoMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metas[meta.Repo] = meta
}

func (m *memDDBStore) GetMeta(_ context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.metas[repo]
	if !ok {
		return ddb.RepoMeta{}, ddb.ErrNotFound
	}
	return meta, nil
}

func (m *memDDBStore) ListRefs(_ context.Context, repo tenant.RepoKey, _ int) ([]ddb.RefState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	byName := m.refs[repo]
	out := make([]ddb.RefState, 0, len(byName))
	for name, oid := range byName {
		out = append(out, ddb.RefState{Repo: repo, RefName: name, OID: oid})
	}
	return out, nil
}

func (m *memDDBStore) QueryManifestShard(_ context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if shard != 0 {
		return nil, nil
	}
	var out []ddb.PackManifest
	for _, pm := range m.manifests[repo] {
		if afterULID != "" && pm.ULID <= afterULID {
			continue
		}
		if activeOnly && pm.Status != ddb.PackStatusActive {
			continue
		}
		out = append(out, pm)
	}
	return out, nil
}

func (m *memDDBStore) CommitPush(_ context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refs[in.Repo] == nil {
		m.refs[in.Repo] = make(map[string]string)
	}
	for _, u := range in.Updates {
		cur, exists := m.refs[in.Repo][u.RefName]
		if u.IsCreate() {
			if exists {
				return ddb.CommitPushResult{}, ddb.ErrRefCASConflict
			}
		} else if !exists || cur != u.OldOID {
			return ddb.CommitPushResult{}, ddb.ErrRefCASConflict
		}
	}
	for _, u := range in.Updates {
		if u.IsDelete() {
			delete(m.refs[in.Repo], u.RefName)
		} else {
			m.refs[in.Repo][u.RefName] = u.NewOID
		}
	}
	m.seq++
	ulid := fmt.Sprintf("01HSTRAVIN%016d", m.seq)
	if in.Pack != nil {
		m.manifests[in.Repo] = append(m.manifests[in.Repo], ddb.PackManifest{
			Repo:      in.Repo,
			ULID:      ulid,
			PackID:    in.Pack.PackID,
			SizeBytes: in.Pack.SizeBytes,
			Status:    ddb.PackStatusActive,
		})
	}
	return ddb.CommitPushResult{ULID: ulid}, nil
}

func (m *memDDBStore) GetRegionalWriteLease(_ context.Context, region lease.Region) (ddb.RegionalWriteLease, error) {
	return ddb.RegionalWriteLease{
		Region:    region,
		Mode:      lease.ModeActive,
		Epoch:     1,
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

func (m *memDDBStore) GetLease(_ context.Context, repo tenant.RepoKey) (lease.Lease, error) {
	return lease.Lease{
		Repo:       repo,
		HomeRegion: m.region,
		Epoch:      1,
		Mode:       lease.ModeActive,
		ExpiresAt:  time.Now().Add(time.Hour),
		HolderID:   "ctrl-1",
	}, nil
}

type memS3Store struct {
	mu    sync.RWMutex
	packs map[string][]byte
	idxs  map[string][]byte
}

func newMemS3Store() *memS3Store {
	return &memS3Store{
		packs: make(map[string][]byte),
		idxs:  make(map[string][]byte),
	}
}

func (s *memS3Store) key(repo tenant.RepoKey, packID string) string {
	return string(repo.TenantID) + "/" + string(repo.RepoID) + "/" + packID
}

func (s *memS3Store) PutPack(_ context.Context, repo tenant.RepoKey, packSHA256 string, packData, idxData []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(repo, packSHA256)
	s.packs[k] = append([]byte(nil), packData...)
	s.idxs[k] = append([]byte(nil), idxData...)
	return nil
}

func (s *memS3Store) GetPack(_ context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.packs[s.key(repo, packSHA256)]
	if !ok {
		return nil, fmt.Errorf("pack %s not found", packSHA256)
	}
	return append([]byte(nil), b...), nil
}

func (s *memS3Store) GetIndex(_ context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.idxs[s.key(repo, packSHA256)]
	if !ok {
		return nil, fmt.Errorf("idx %s not found", packSHA256)
	}
	return append([]byte(nil), b...), nil
}
