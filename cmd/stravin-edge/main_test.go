package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"testing"

	"stravin.dev/core/internal/edge"
)

func TestLoadAuthenticatorFromEnv_StaticDefaultAndJWKSMode(t *testing.T) {
	t.Run("defaults to StaticAuthenticator when JWKS not configured", func(t *testing.T) {
		t.Setenv("STRAVIN_AUTH_MODE", "")
		t.Setenv("STRAVIN_AUTH_JWKS_URL", "")
		t.Setenv("STRAVIN_AUTH_JWKS_STATIC", "")
		t.Setenv("STRAVIN_AUTH_TOKENS", "acme:alice:local-dev-tok")

		auth, cleanup, err := loadAuthenticatorFromEnv(context.Background(), slog.Default())
		if err != nil {
			t.Fatalf("loadAuthenticatorFromEnv: %v", err)
		}
		defer cleanup()

		if _, ok := auth.(*edge.StaticAuthenticator); !ok {
			t.Fatalf("expected *edge.StaticAuthenticator, got %T", auth)
		}
		p, err := auth.AuthenticateToken(context.Background(), "local-dev-tok")
		if err != nil || p.TenantID != "acme" || p.ActorID != "alice" {
			t.Fatalf("unexpected static auth result: principal=%+v err=%v", p, err)
		}
	})

	t.Run("selects JWKSAuthenticator when static JWKS is configured", func(t *testing.T) {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("ed25519.GenerateKey: %v", err)
		}
		jwksBytes, err := json.Marshal(map[string]any{
			"keys": []map[string]any{
				{
					"kty": "OKP",
					"crv": "Ed25519",
					"kid": "kid-1",
					"use": "sig",
					"alg": "EdDSA",
					"x":   base64.RawURLEncoding.EncodeToString(pub),
				},
			},
		})
		if err != nil {
			t.Fatalf("marshal jwks: %v", err)
		}

		t.Setenv("STRAVIN_AUTH_MODE", "jwks")
		t.Setenv("STRAVIN_AUTH_JWKS_STATIC", string(jwksBytes))
		t.Setenv("STRAVIN_AUTH_JWKS_URL", "")
		t.Setenv("STRAVIN_AUTH_VALKEY_ADDR", "")

		auth, cleanup, err := loadAuthenticatorFromEnv(context.Background(), slog.Default())
		if err != nil {
			t.Fatalf("loadAuthenticatorFromEnv(jwks): %v", err)
		}
		defer cleanup()

		if _, ok := auth.(*edge.JWKSAuthenticator); !ok {
			t.Fatalf("expected *edge.JWKSAuthenticator, got %T", auth)
		}
	})

	t.Run("rejects unknown STRAVIN_AUTH_MODE", func(t *testing.T) {
		t.Setenv("STRAVIN_AUTH_MODE", "magic")
		if _, _, err := loadAuthenticatorFromEnv(context.Background(), slog.Default()); err == nil {
			t.Fatal("expected error for unsupported STRAVIN_AUTH_MODE")
		}
	})
}
