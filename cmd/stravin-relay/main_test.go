package main

import (
	"testing"
	"time"
)

func TestRelayEnvFallbacks(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		t.Setenv("STRAVIN_RELAY_CONSUMER_ID", "")
		t.Setenv("STRAVIN_HOOK_CONSUMER_ID", "")
		t.Setenv("STRAVIN_RELAY_POLL_INTERVAL", "")
		t.Setenv("STRAVIN_HOOK_POLL_INTERVAL", "")
		t.Setenv("STRAVIN_NATS_URL", "")
		t.Setenv("NATS_URL", "")

		if got := consumerIDFromEnv(); got != "stravin-relay" {
			t.Fatalf("consumerIDFromEnv() = %q, want stravin-relay", got)
		}
		if got := pollIntervalFromEnv(); got != 500*time.Millisecond {
			t.Fatalf("pollIntervalFromEnv() = %v, want 500ms", got)
		}
		if got := natsURLFromEnv(); got != "" {
			t.Fatalf("natsURLFromEnv() = %q, want empty", got)
		}
	})

	t.Run("legacy hook env fallback", func(t *testing.T) {
		t.Setenv("STRAVIN_RELAY_CONSUMER_ID", "")
		t.Setenv("STRAVIN_HOOK_CONSUMER_ID", "legacy-consumer")
		t.Setenv("STRAVIN_RELAY_POLL_INTERVAL", "")
		t.Setenv("STRAVIN_HOOK_POLL_INTERVAL", "250ms")
		t.Setenv("STRAVIN_NATS_URL", "")
		t.Setenv("NATS_URL", "nats://nats:4222")

		if got := consumerIDFromEnv(); got != "legacy-consumer" {
			t.Fatalf("consumerIDFromEnv() = %q, want legacy-consumer", got)
		}
		if got := pollIntervalFromEnv(); got != 250*time.Millisecond {
			t.Fatalf("pollIntervalFromEnv() = %v, want 250ms", got)
		}
		if got := natsURLFromEnv(); got != "nats://nats:4222" {
			t.Fatalf("natsURLFromEnv() = %q, want nats://nats:4222", got)
		}
	})

	t.Run("relay env overrides legacy", func(t *testing.T) {
		t.Setenv("STRAVIN_RELAY_CONSUMER_ID", "relay-1")
		t.Setenv("STRAVIN_HOOK_CONSUMER_ID", "legacy-consumer")
		t.Setenv("STRAVIN_RELAY_POLL_INTERVAL", "100ms")
		t.Setenv("STRAVIN_HOOK_POLL_INTERVAL", "250ms")
		t.Setenv("STRAVIN_NATS_URL", "nats://custom:4222")
		t.Setenv("NATS_URL", "nats://nats:4222")

		if got := consumerIDFromEnv(); got != "relay-1" {
			t.Fatalf("consumerIDFromEnv() = %q, want relay-1", got)
		}
		if got := pollIntervalFromEnv(); got != 100*time.Millisecond {
			t.Fatalf("pollIntervalFromEnv() = %v, want 100ms", got)
		}
		if got := natsURLFromEnv(); got != "nats://custom:4222" {
			t.Fatalf("natsURLFromEnv() = %q, want nats://custom:4222", got)
		}
	})
}
