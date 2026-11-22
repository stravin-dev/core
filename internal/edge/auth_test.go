package edge

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/tenant"
)

func generateTestSSHKey(t *testing.T) (ssh.Signer, ssh.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("NewSignerFromKey: %v", err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatalf("NewPublicKey: %v", err)
	}
	return signer, sshPub
}

func TestStaticAuthenticator_TokenAndSSH(t *testing.T) {
	_, pubA := generateTestSSHKey(t)
	_, pubOther := generateTestSSHKey(t)

	auth, err := NewStaticAuthenticator(
		[]TokenEntry{
			{Token: "secret-token-a", Principal: Principal{ActorID: "alice", TenantID: "acme"}},
			{Token: "secret-token-b", Principal: Principal{ActorID: "bob", TenantID: "beta"}},
		},
		[]SSHKeyEntry{
			{Key: pubA, Principal: Principal{ActorID: "alice", TenantID: "acme"}},
		},
	)
	if err != nil {
		t.Fatalf("NewStaticAuthenticator: %v", err)
	}

	p, err := auth.AuthenticateToken(context.Background(), "secret-token-a")
	if err != nil {
		t.Fatalf("AuthenticateToken valid: %v", err)
	}
	if p.TenantID != "acme" || p.ActorID != "alice" {
		t.Fatalf("unexpected principal: %+v", p)
	}

	if _, err := auth.AuthenticateToken(context.Background(), "wrong-token"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for wrong token, got %v", err)
	}
	if _, err := auth.AuthenticateToken(context.Background(), ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for empty token, got %v", err)
	}

	p, err = auth.AuthenticateSSHKey(context.Background(), pubA)
	if err != nil {
		t.Fatalf("AuthenticateSSHKey valid: %v", err)
	}
	if p.TenantID != "acme" || p.ActorID != "alice" {
		t.Fatalf("unexpected ssh principal: %+v", p)
	}

	if _, err := auth.AuthenticateSSHKey(context.Background(), pubOther); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for unknown key, got %v", err)
	}
}

func TestAuthorizeTenant_Isolation(t *testing.T) {
	p := Principal{ActorID: "alice", TenantID: "acme"}
	if err := AuthorizeTenant(p, tenant.TenantID("acme")); err != nil {
		t.Fatalf("expected matching tenant to succeed, got %v", err)
	}
	if err := AuthorizeTenant(p, tenant.TenantID("other")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden for cross-tenant access, got %v", err)
	}
}

func TestParseTokensAndAuthorizedKeys(t *testing.T) {
	tokens, err := ParseTokens("# comment\nacme:alice:tok1, beta:bob:tok2\n")
	if err != nil {
		t.Fatalf("ParseTokens: %v", err)
	}
	if len(tokens) != 2 || tokens[0].Principal.TenantID != "acme" || tokens[1].Token != "tok2" {
		t.Fatalf("unexpected parsed tokens: %+v", tokens)
	}

	_, pub := generateTestSSHKey(t)
	line := fmt.Sprintf("# header\n%s acme:alice\n", string(ssh.MarshalAuthorizedKey(pub))[:len(ssh.MarshalAuthorizedKey(pub))-1])
	keys, err := ParseAuthorizedKeys([]byte(line))
	if err != nil {
		t.Fatalf("ParseAuthorizedKeys: %v", err)
	}
	if len(keys) != 1 || keys[0].Principal.TenantID != "acme" || keys[0].Principal.ActorID != "alice" {
		t.Fatalf("unexpected parsed keys: %+v", keys)
	}
}
