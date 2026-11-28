package edge

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/tenant"
)

func TestParseSSHExecCommand_ValidationAndInjectionRejection(t *testing.T) {
	validCases := []struct {
		cmd    string
		wantOp rpc.Operation
		want   tenant.RepoKey
	}{
		{
			cmd:    "git-upload-pack '/acme/core.git'",
			wantOp: rpc.OpUploadPack,
			want:   tenant.RepoKey{TenantID: "acme", RepoID: "core"},
		},
		{
			cmd:    "git-receive-pack '/acme/my-repo_1.git'",
			wantOp: rpc.OpReceivePack,
			want:   tenant.RepoKey{TenantID: "acme", RepoID: "my-repo_1"},
		},
		{
			cmd:    `git-upload-pack "acme/core.git"`,
			wantOp: rpc.OpUploadPack,
			want:   tenant.RepoKey{TenantID: "acme", RepoID: "core"},
		},
		{
			cmd:    "git-upload-pack /acme/core",
			wantOp: rpc.OpUploadPack,
			want:   tenant.RepoKey{TenantID: "acme", RepoID: "core"},
		},
	}

	for _, tc := range validCases {
		op, repo, err := ParseSSHExecCommand(tc.cmd)
		if err != nil {
			t.Fatalf("ParseSSHExecCommand(%q) unexpected error: %v", tc.cmd, err)
		}
		if op != tc.wantOp || repo != tc.want {
			t.Fatalf("ParseSSHExecCommand(%q) = (%q, %+v), want (%q, %+v)", tc.cmd, op, repo, tc.wantOp, tc.want)
		}
	}

	invalidCases := []string{
		"",
		"sh -c id",
		"git-upload-pack '/acme/core.git; rm -rf /'",
		"git-upload-pack '/acme/core.git' | cat /etc/passwd",
		"git-upload-pack '/acme/$(whoami).git'",
		"git-upload-pack '/acme/`id`.git'",
		"git-upload-pack '/../beta/core.git'",
		"git-upload-pack '/acme/../beta/core.git'",
		"git-upload-pack '/acme/sub/core.git'",
		"git-upload-pack '/acme/core.git' extra-arg",
		"git-upload-pack '/acme/core.git\nwhoami'",
	}

	for _, cmd := range invalidCases {
		if _, _, err := ParseSSHExecCommand(cmd); err == nil {
			t.Fatalf("expected error for malicious/invalid SSH command %q", cmd)
		}
	}
}

func TestSSHServer_EndToEndAuthStreamingIsolationAndLease(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ssh: %v", err)
	}
	go func() { _ = env.sshServer.Serve(ln) }()
	defer func() { _ = env.sshServer.Close() }()

	// 1. Unknown key rejected at SSH handshake.
	unknownSigner, _ := generateTestSSHKey(t)
	if _, err := ssh.Dial("tcp", ln.Addr().String(), &ssh.ClientConfig{
		User:            "git",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(unknownSigner)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         3 * time.Second,
	}); err == nil {
		t.Fatalf("expected unknown SSH key to be rejected")
	}

	// 2. Authorized client connects.
	client, err := ssh.Dial("tcp", ln.Addr().String(), &ssh.ClientConfig{
		User:            "git",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(env.clientKey.(ssh.Signer))},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         3 * time.Second,
	})
	if err != nil {
		t.Fatalf("ssh.Dial authorized: %v", err)
	}
	defer func() { _ = client.Close() }()

	// 3. Valid git-upload-pack streams stdin/stdout and propagates traceparent.
	sess, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession upload-pack: %v", err)
	}
	var stdout, stderr bytes.Buffer
	sess.Stdin = strings.NewReader("ssh-upload-req")
	sess.Stdout = &stdout
	sess.Stderr = &stderr
	if err := sess.Run("git-upload-pack '/acme/core.git'"); err != nil {
		t.Fatalf("ssh git-upload-pack failed: %v (stderr=%q)", err, stderr.String())
	}
	_ = sess.Close()

	if !strings.Contains(stdout.String(), "refs/heads/main") {
		t.Fatalf("unexpected ssh stdout: %q", stdout.String())
	}
	meta, payload := env.node.snapshot()
	if meta.TenantID != "acme" || meta.RepoID != "core" || meta.Operation != rpc.OpUploadPack {
		t.Fatalf("unexpected rpc meta from ssh: %+v", meta)
	}
	if string(payload) != "ssh-upload-req" {
		t.Fatalf("expected ssh payload %q, got %q", "ssh-upload-req", string(payload))
	}
	if meta.TraceParent == "" {
		t.Fatalf("expected W3C traceparent from ssh session")
	}

	// 4. Cross-tenant SSH request fails closed with permission denied.
	sessCross, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession cross-tenant: %v", err)
	}
	var crossStderr bytes.Buffer
	sessCross.Stderr = &crossStderr
	if err := sessCross.Run("git-upload-pack '/beta/secret.git'"); err == nil {
		t.Fatalf("expected cross-tenant ssh command to fail")
	}
	_ = sessCross.Close()
	if !strings.Contains(crossStderr.String(), "permission denied") {
		t.Fatalf("expected permission denied on stderr, got %q", crossStderr.String())
	}

	// 5. Expired write lease rejects SSH git-receive-pack.
	env.clk.Advance(2 * time.Minute)
	sessPush, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession expired push: %v", err)
	}
	var pushStderr bytes.Buffer
	sessPush.Stderr = &pushStderr
	if err := sessPush.Run("git-receive-pack '/acme/core.git'"); err == nil {
		t.Fatalf("expected expired write lease ssh push to fail")
	}
	_ = sessPush.Close()
	if !strings.Contains(pushStderr.String(), "temporarily unavailable") {
		t.Fatalf("expected unavailable message on stderr, got %q", pushStderr.String())
	}
}

func TestSSHServer_PreAuthStallDoesNotHangShutdown(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ssh: %v", err)
	}
	go func() { _ = env.sshServer.Serve(ln) }()

	// Open raw TCP connection and stall without sending an SSH banner.
	rawConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial raw tcp: %v", err)
	}
	defer func() { _ = rawConn.Close() }()

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		done <- env.sshServer.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		if err != nil && err != context.DeadlineExceeded {
			t.Fatalf("unexpected Shutdown error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SSHServer.Shutdown hung on stalled unauthenticated TCP connection")
	}
}

func TestSSHServer_SessionChannelCap(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()
	env.sshServer.maxSessionsPerConn = 2

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ssh: %v", err)
	}
	go func() { _ = env.sshServer.Serve(ln) }()
	defer func() { _ = env.sshServer.Close() }()

	client, err := ssh.Dial("tcp", ln.Addr().String(), &ssh.ClientConfig{
		User:            "git",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(env.clientKey.(ssh.Signer))},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         3 * time.Second,
	})
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	s1, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession 1: %v", err)
	}
	defer func() { _ = s1.Close() }()

	s2, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession 2: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if s3, err := client.NewSession(); err == nil {
		_ = s3.Close()
		t.Fatal("expected 3rd concurrent SSH session channel to be rejected")
	}
}

func TestParseSSHExecCommandPath_MultiSegmentAndFailClosed(t *testing.T) {
	valid := []struct {
		cmd      string
		wantOp   rpc.Operation
		wantFull string
	}{
		{"git-upload-pack '/acme/core.git'", rpc.OpUploadPack, "acme/core"},
		{"git-upload-pack '/acme/eng/core.git'", rpc.OpUploadPack, "acme/eng/core"},
		{"git-receive-pack '/acme/eng/payments/core.git'", rpc.OpReceivePack, "acme/eng/payments/core"},
		{"git-upload-pack '/acme/eng/payments/sub/core.git'", rpc.OpUploadPack, "acme/eng/payments/sub/core"},
		{`git-upload-pack "acme/eng/payments/sub/core"`, rpc.OpUploadPack, "acme/eng/payments/sub/core"},
	}
	for _, tc := range valid {
		op, parsed, err := ParseSSHExecCommandPath(tc.cmd, DefaultMaxOrgDepth)
		if err != nil {
			t.Fatalf("ParseSSHExecCommandPath(%q) unexpected error: %v", tc.cmd, err)
		}
		if op != tc.wantOp || parsed.FullPath != tc.wantFull {
			t.Fatalf("ParseSSHExecCommandPath(%q) = (%q, %q), want (%q, %q)", tc.cmd, op, parsed.FullPath, tc.wantOp, tc.wantFull)
		}
	}

	invalid := []string{
		"git-upload-pack '/a/b/c/d/e/core.git'",
		"git-upload-pack '/acme/./core.git'",
		"git-upload-pack '/acme/../core.git'",
		"git-upload-pack '/acme//core.git'",
		"git-upload-pack '//acme/core.git'",
		"git-upload-pack '/acme/-/core.git'",
		"git-upload-pack '/acme/eng\x07/core.git'",
		"git-upload-pack '/acme/%2e%2e/core.git'",
	}
	for _, cmd := range invalid {
		if _, _, err := ParseSSHExecCommandPath(cmd, DefaultMaxOrgDepth); err == nil {
			t.Fatalf("expected error for invalid SSH command %q", cmd)
		}
	}
}

func TestSSHServer_NestedOrgRouteMapAndFailClosed(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	env.sshServer.SetRouteResolver(&stubRouteResolver{
		routes: map[string]RouteEntry{
			"acme/eng/core":               {RootTenantID: "acme", RepoID: "core"},
			"acme/eng/payments/core":      {RootTenantID: "acme", RepoID: "core"},
			"acme/eng/payments/sub/core":  {RootTenantID: "acme", RepoID: "core"},
			"acme/alias-flat":             {RootTenantID: "acme", RepoID: "core"},
			"beta/eng/payments/forbidden": {RootTenantID: "beta", RepoID: "secret"},
		},
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ssh: %v", err)
	}
	go func() { _ = env.sshServer.Serve(ln) }()
	defer func() { _ = env.sshServer.Close() }()

	client, err := ssh.Dial("tcp", ln.Addr().String(), &ssh.ClientConfig{
		User:            "git",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(env.clientKey.(ssh.Signer))},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         3 * time.Second,
	})
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	// 1. 2-, 3-, 4-, 5-segment mapped paths & 2-segment fallback all resolve to immutable (acme, core).
	for _, cmd := range []string{
		"git-upload-pack '/acme/core.git'",
		"git-upload-pack '/acme/alias-flat.git'",
		"git-upload-pack '/acme/eng/core.git'",
		"git-upload-pack '/acme/eng/payments/core.git'",
		"git-upload-pack '/acme/eng/payments/sub/core.git'",
	} {
		sess, err := client.NewSession()
		if err != nil {
			t.Fatalf("NewSession(%q): %v", cmd, err)
		}
		var stdout, stderr bytes.Buffer
		sess.Stdout = &stdout
		sess.Stderr = &stderr
		if err := sess.Run(cmd); err != nil {
			t.Fatalf("Run(%q) failed: %v (stderr=%q)", cmd, err, stderr.String())
		}
		_ = sess.Close()
		meta, _ := env.node.snapshot()
		if meta.TenantID != "acme" || meta.RepoID != "core" {
			t.Fatalf("Run(%q) routed to %+v, want acme/core", cmd, meta)
		}
	}

	// 2. Unmapped >2-segment path fails closed with repository not found.
	sessUnmapped, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession unmapped: %v", err)
	}
	var unmappedStderr bytes.Buffer
	sessUnmapped.Stderr = &unmappedStderr
	if err := sessUnmapped.Run("git-upload-pack '/acme/eng/unmapped.git'"); err == nil {
		t.Fatal("expected unmapped >2-segment SSH path to fail closed")
	}
	_ = sessUnmapped.Close()
	if !strings.Contains(unmappedStderr.String(), "repository not found") {
		t.Fatalf("expected repository not found stderr, got %q", unmappedStderr.String())
	}

	// 3. Depth > cap (6 segments) fails closed as invalid path.
	sessDeep, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession deep: %v", err)
	}
	var deepStderr bytes.Buffer
	sessDeep.Stderr = &deepStderr
	if err := sessDeep.Run("git-upload-pack '/acme/a/b/c/d/core.git'"); err == nil {
		t.Fatal("expected 6-segment SSH path to be rejected")
	}
	_ = sessDeep.Close()
	if !strings.Contains(deepStderr.String(), "invalid git command") {
		t.Fatalf("expected invalid git command stderr, got %q", deepStderr.String())
	}
}
