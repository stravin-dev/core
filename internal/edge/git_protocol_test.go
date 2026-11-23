package edge

import (
	"bytes"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/rpc"
)

func TestHTTPHandler_GitProtocolForwardingAndSanitization(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	t.Run("ForwardsValidGitProtocolOnInfoRefsAndUploadPack", func(t *testing.T) {
		for _, tc := range []struct {
			method string
			path   string
			hdr    string
			want   string
		}{
			{
				method: http.MethodGet,
				path:   "/acme/core.git/info/refs?service=git-upload-pack",
				hdr:    "version=2",
				want:   "version=2",
			},
			{
				method: http.MethodPost,
				path:   "/acme/core.git/git-upload-pack",
				hdr:    "version=2:object-format=sha256",
				want:   "version=2:object-format=sha256",
			},
		} {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("0000"))
			req.Header.Set("Authorization", "Bearer tok-acme")
			req.Header.Set("Git-Protocol", tc.hdr)
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("%s %s status = %d, want 200 (body=%q)", tc.method, tc.path, rec.Code, rec.Body.String())
			}
			meta, _ := env.node.snapshot()
			if meta.GitProtocol != tc.want {
				t.Fatalf("%s %s forwarded GitProtocol = %q, want %q", tc.method, tc.path, meta.GitProtocol, tc.want)
			}
		}
	})

	t.Run("StripsMalformedAndInjectedGitProtocolHeaders", func(t *testing.T) {
		malformed := []string{
			"version=3",
			"version=2\r\nX-Injected: evil",
			"version=2\x00evil",
			"version=2; rm -rf /",
			"LD_PRELOAD=/tmp/evil.so",
			"version=2:version=1",
			"agent=" + strings.Repeat("a", rpc.MaxGitProtocolLen),
		}
		for _, bad := range malformed {
			req := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
			req.Header.Set("Authorization", "Bearer tok-acme")
			req.Header["Git-Protocol"] = []string{bad}
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("malformed header %q caused status %d, want 200 fallback", bad, rec.Code)
			}
			meta, _ := env.node.snapshot()
			if meta.GitProtocol != "" {
				t.Fatalf("malformed header %q leaked GitProtocol = %q, want empty", bad, meta.GitProtocol)
			}
		}
	})

	t.Run("StripsDuplicateGitProtocolHeaders", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
		req.Header.Set("Authorization", "Bearer tok-acme")
		req.Header["Git-Protocol"] = []string{"version=2", "version=1"}
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("duplicate headers status = %d, want 200 fallback", rec.Code)
		}
		meta, _ := env.node.snapshot()
		if meta.GitProtocol != "" {
			t.Fatalf("duplicate headers leaked GitProtocol = %q, want empty", meta.GitProtocol)
		}
	})
}

func TestSSHServer_GitProtocolEnvForwardingAndSanitization(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

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

	t.Run("ValidGitProtocolEnvForwarded", func(t *testing.T) {
		sess, err := client.NewSession()
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer func() { _ = sess.Close() }()

		if err := sess.Setenv("GIT_PROTOCOL", "version=2"); err != nil {
			t.Fatalf("Setenv GIT_PROTOCOL=version=2: %v", err)
		}
		var stdout, stderr bytes.Buffer
		sess.Stdin = strings.NewReader("0000")
		sess.Stdout = &stdout
		sess.Stderr = &stderr
		if err := sess.Run("git-upload-pack '/acme/core.git'"); err != nil {
			t.Fatalf("Run: %v (stderr=%q)", err, stderr.String())
		}
		meta, _ := env.node.snapshot()
		if meta.GitProtocol != "version=2" {
			t.Fatalf("SSH forwarded GitProtocol = %q, want %q", meta.GitProtocol, "version=2")
		}
	})

	t.Run("NonGitProtocolEnvRejected", func(t *testing.T) {
		sess, err := client.NewSession()
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer func() { _ = sess.Close() }()

		if err := sess.Setenv("LD_PRELOAD", "/tmp/evil.so"); err == nil {
			t.Fatal("expected LD_PRELOAD env request to be rejected by SSH server")
		}
	})

	t.Run("MalformedGitProtocolEnvRejectedAndNotForwarded", func(t *testing.T) {
		sess, err := client.NewSession()
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer func() { _ = sess.Close() }()

		if err := sess.Setenv("GIT_PROTOCOL", "version=2; id"); err == nil {
			t.Fatal("expected malformed GIT_PROTOCOL env request to be rejected")
		}
		var stdout, stderr bytes.Buffer
		sess.Stdin = strings.NewReader("0000")
		sess.Stdout = &stdout
		sess.Stderr = &stderr
		if err := sess.Run("git-upload-pack '/acme/core.git'"); err != nil {
			t.Fatalf("Run fallback: %v (stderr=%q)", err, stderr.String())
		}
		meta, _ := env.node.snapshot()
		if meta.GitProtocol != "" {
			t.Fatalf("malformed SSH GIT_PROTOCOL leaked %q, want empty", meta.GitProtocol)
		}
	})
}

func TestDecodeSSHEnvPayload(t *testing.T) {
	var valid bytes.Buffer
	writeSSHString := func(buf *bytes.Buffer, s string) {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
		buf.Write(lenBuf[:])
		buf.WriteString(s)
	}
	writeSSHString(&valid, "GIT_PROTOCOL")
	writeSSHString(&valid, "version=2")

	name, val, err := decodeSSHEnvPayload(valid.Bytes())
	if err != nil || name != "GIT_PROTOCOL" || val != "version=2" {
		t.Fatalf("decodeSSHEnvPayload valid = (%q, %q, %v)", name, val, err)
	}

	// Trailing garbage must be rejected.
	withTail := append(append([]byte(nil), valid.Bytes()...), 'x')
	if _, _, err := decodeSSHEnvPayload(withTail); err == nil {
		t.Fatal("expected error for trailing bytes in SSH env payload")
	}

	// Truncated payload must be rejected.
	if _, _, err := decodeSSHEnvPayload([]byte{0, 0, 0, 10, 'a'}); err == nil {
		t.Fatal("expected error for truncated SSH env payload")
	}
}
