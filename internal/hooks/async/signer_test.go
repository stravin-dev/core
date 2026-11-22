package async

import (
	"strings"
	"testing"
)

func TestSignAndVerifyPayload(t *testing.T) {
	secret := []byte("super-secret-signing-key")
	payload := []byte(`{"tenant_id":"acme","repo_id":"core","ref":"refs/heads/main"}`)

	sig := SignPayload(secret, payload)
	if !strings.HasPrefix(sig, "sha256=") {
		t.Fatalf("signature %q missing sha256= prefix", sig)
	}
	if len(sig) != len("sha256=")+64 {
		t.Fatalf("signature length = %d, want %d", len(sig), len("sha256=")+64)
	}

	if !VerifySignature(secret, payload, sig) {
		t.Fatal("VerifySignature returned false for valid signature")
	}
	if VerifySignature([]byte("wrong-secret"), payload, sig) {
		t.Fatal("VerifySignature returned true for wrong secret")
	}
	if VerifySignature(secret, []byte(`{"tampered":true}`), sig) {
		t.Fatal("VerifySignature returned true for tampered payload")
	}
	if VerifySignature(secret, payload, "md5=deadbeef") {
		t.Fatal("VerifySignature returned true for invalid prefix")
	}
	if VerifySignature(secret, payload, "sha256=not-hex") {
		t.Fatal("VerifySignature returned true for invalid hex")
	}
}
