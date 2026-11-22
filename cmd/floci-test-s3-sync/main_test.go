package main

import (
	"testing"
	"time"
)

func setRequiredSyncEnv(t *testing.T) {
	t.Helper()
	t.Setenv("FLOCI_SYNC_SOURCE_ENDPOINT", "http://floci-region-a:4566")
	t.Setenv("FLOCI_SYNC_SOURCE_BUCKET", "git-objects-us-east-1")
	t.Setenv("FLOCI_SYNC_SOURCE_QUEUE_URL", "http://floci-region-a:4566/000000000000/floci-test-s3-sync-queue")
	t.Setenv("FLOCI_SYNC_TARGET_ENDPOINT", "http://floci-region-b:4566")
	t.Setenv("FLOCI_SYNC_TARGET_BUCKET", "git-objects-us-west-2")
}

func TestLoadConfig_ValidAndDefaults(t *testing.T) {
	setRequiredSyncEnv(t)
	t.Setenv("FLOCI_SYNC_SOURCE_REGION", "")
	t.Setenv("FLOCI_SYNC_TARGET_REGION", "")
	t.Setenv("FLOCI_SYNC_LAG_DELAY", "")
	t.Setenv("FLOCI_SYNC_REPLICATE_DELETES", "")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.SourceRegion != "us-east-1" {
		t.Errorf("SourceRegion = %q, want us-east-1", cfg.SourceRegion)
	}
	if cfg.TargetRegion != "us-west-2" {
		t.Errorf("TargetRegion = %q, want us-west-2", cfg.TargetRegion)
	}
	if cfg.LagDelay != 0 {
		t.Errorf("LagDelay = %v, want 0s", cfg.LagDelay)
	}
	if cfg.ReplicateDeletes {
		t.Errorf("ReplicateDeletes = true, want false")
	}
}

func TestLoadConfig_CustomLagAndDeletes(t *testing.T) {
	setRequiredSyncEnv(t)
	t.Setenv("FLOCI_SYNC_SOURCE_REGION", "eu-west-1")
	t.Setenv("FLOCI_SYNC_TARGET_REGION", "ap-northeast-1")
	t.Setenv("FLOCI_SYNC_LAG_DELAY", "250ms")
	t.Setenv("FLOCI_SYNC_REPLICATE_DELETES", "true")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.SourceRegion != "eu-west-1" || cfg.TargetRegion != "ap-northeast-1" {
		t.Errorf("regions = (%q, %q), want (eu-west-1, ap-northeast-1)", cfg.SourceRegion, cfg.TargetRegion)
	}
	if cfg.LagDelay != 250*time.Millisecond {
		t.Errorf("LagDelay = %v, want 250ms", cfg.LagDelay)
	}
	if !cfg.ReplicateDeletes {
		t.Errorf("ReplicateDeletes = false, want true")
	}
}

func TestLoadConfig_Errors(t *testing.T) {
	t.Run("missing required env", func(t *testing.T) {
		t.Setenv("FLOCI_SYNC_SOURCE_ENDPOINT", "")
		t.Setenv("FLOCI_SYNC_SOURCE_BUCKET", "")
		t.Setenv("FLOCI_SYNC_SOURCE_QUEUE_URL", "")
		t.Setenv("FLOCI_SYNC_TARGET_ENDPOINT", "")
		t.Setenv("FLOCI_SYNC_TARGET_BUCKET", "")

		if _, err := loadConfig(); err == nil {
			t.Fatal("expected error when required env vars are missing")
		}
	})

	t.Run("invalid lag delay", func(t *testing.T) {
		setRequiredSyncEnv(t)
		t.Setenv("FLOCI_SYNC_LAG_DELAY", "not-a-duration")
		if _, err := loadConfig(); err == nil {
			t.Fatal("expected error for invalid FLOCI_SYNC_LAG_DELAY")
		}
	})

	t.Run("negative lag delay", func(t *testing.T) {
		setRequiredSyncEnv(t)
		t.Setenv("FLOCI_SYNC_LAG_DELAY", "-1s")
		if _, err := loadConfig(); err == nil {
			t.Fatal("expected error for negative FLOCI_SYNC_LAG_DELAY")
		}
	})

	t.Run("invalid replicate deletes bool", func(t *testing.T) {
		setRequiredSyncEnv(t)
		t.Setenv("FLOCI_SYNC_REPLICATE_DELETES", "maybe")
		if _, err := loadConfig(); err == nil {
			t.Fatal("expected error for invalid FLOCI_SYNC_REPLICATE_DELETES")
		}
	})
}
