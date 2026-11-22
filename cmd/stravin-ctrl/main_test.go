package main

import (
	"context"
	"strings"
	"testing"
)

func TestLoadConfig_DefaultsAndOverrides(t *testing.T) {
	t.Setenv("STRAVIN_CTRL_ADDR", "")
	t.Setenv("STRAVIN_CTRL_LISTEN_ADDR", "")
	t.Setenv("STRAVIN_HEALTH_ADDR", "")
	t.Setenv("STRAVIN_CTRL_ADMIN_TOKEN", "")

	if defaultListenAddr != "127.0.0.1:8083" {
		t.Fatalf("defaultListenAddr = %q, want %q", defaultListenAddr, "127.0.0.1:8083")
	}

	cfg := loadConfig()
	if cfg.ListenAddr != "127.0.0.1:8083" {
		t.Fatalf("default ListenAddr = %q, want %q", cfg.ListenAddr, "127.0.0.1:8083")
	}
	if cfg.AdminToken != "" {
		t.Fatalf("default AdminToken = %q, want empty", cfg.AdminToken)
	}

	t.Setenv("STRAVIN_CTRL_LISTEN_ADDR", ":8083")
	t.Setenv("STRAVIN_CTRL_ADMIN_TOKEN", "  secret-token  ")
	cfg = loadConfig()
	if cfg.ListenAddr != ":8083" {
		t.Fatalf("STRAVIN_CTRL_LISTEN_ADDR ListenAddr = %q, want %q", cfg.ListenAddr, ":8083")
	}
	if cfg.AdminToken != "secret-token" {
		t.Fatalf("AdminToken = %q, want %q", cfg.AdminToken, "secret-token")
	}

	t.Setenv("STRAVIN_CTRL_ADDR", "127.0.0.1:9083")
	cfg = loadConfig()
	if cfg.ListenAddr != "127.0.0.1:9083" {
		t.Fatalf("STRAVIN_CTRL_ADDR ListenAddr = %q, want %q", cfg.ListenAddr, "127.0.0.1:9083")
	}
}

func TestRun_RequiresAdminToken(t *testing.T) {
	err := run(context.Background(), ctrlConfig{
		ListenAddr: "127.0.0.1:0",
		Region:     "us-east-1",
	})
	if err == nil {
		t.Fatal("expected run() to fail when AdminToken is empty, got nil")
	}
	if !strings.Contains(err.Error(), "STRAVIN_CTRL_ADMIN_TOKEN") {
		t.Fatalf("unexpected error = %q, want mention of STRAVIN_CTRL_ADMIN_TOKEN", err.Error())
	}
}
