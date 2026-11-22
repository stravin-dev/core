package main

import "testing"

func TestAllowPrivateNetworksFromEnv(t *testing.T) {
	const key = "STRAVIN_WEBHOOK_ALLOW_PRIVATE_NETWORKS"

	t.Run("unset fails closed", func(t *testing.T) {
		t.Setenv(key, "")
		if allowPrivateNetworksFromEnv() {
			t.Fatalf("got true, want false")
		}
	})

	t.Run("parses true and false", func(t *testing.T) {
		t.Setenv(key, "true")
		if !allowPrivateNetworksFromEnv() {
			t.Fatalf("got false, want true")
		}
		t.Setenv(key, "false")
		if allowPrivateNetworksFromEnv() {
			t.Fatalf("got true, want false")
		}
	})

	t.Run("invalid value fails closed", func(t *testing.T) {
		t.Setenv(key, "not-a-bool")
		if allowPrivateNetworksFromEnv() {
			t.Fatalf("got true, want false")
		}
	})
}
