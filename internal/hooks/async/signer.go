package async

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// HeaderSignature256 is the HTTP header carrying the HMAC-SHA256 payload digest.
	HeaderSignature256 = "X-Stravin-Signature-256"

	// HeaderEvent identifies the webhook event type.
	HeaderEvent = "X-Stravin-Event"

	// EventPush is the event header value for Git push Ref History notifications.
	EventPush = "push"

	signaturePrefix = "sha256="
)

// SignPayload computes the HMAC-SHA256 signature header value ("sha256=<hex>").
func SignPayload(secret, payload []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature performs a constant-time HMAC-SHA256 verification of signatureHeader.
func VerifySignature(secret, payload []byte, signatureHeader string) bool {
	if !strings.HasPrefix(signatureHeader, signaturePrefix) {
		return false
	}
	providedHex := strings.TrimPrefix(signatureHeader, signaturePrefix)
	provided, err := hex.DecodeString(providedHex)
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	expected := mac.Sum(nil)
	return hmac.Equal(provided, expected)
}
