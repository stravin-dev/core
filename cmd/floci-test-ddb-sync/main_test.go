package main

import (
	"testing"
	"time"

	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func TestLoadEnvConfig_ValidAndDefaults(t *testing.T) {
	t.Setenv("FLOCI_SYNC_SOURCE_ENDPOINT", "http://floci-a:4566")
	t.Setenv("FLOCI_SYNC_SOURCE_REGION", "us-east-1")
	t.Setenv("FLOCI_SYNC_SOURCE_TABLE", "stravin-core")
	t.Setenv("FLOCI_SYNC_TARGET_ENDPOINT", "http://floci-b:4566")
	t.Setenv("FLOCI_SYNC_TARGET_REGION", "us-west-2")
	t.Setenv("FLOCI_SYNC_TARGET_TABLE", "stravin-core")

	cfg, err := loadEnvConfig()
	if err != nil {
		t.Fatalf("loadEnvConfig: %v", err)
	}
	if cfg.LagDelay != 0 {
		t.Errorf("LagDelay = %v, want 0", cfg.LagDelay)
	}
	if cfg.PollInterval != 200*time.Millisecond {
		t.Errorf("PollInterval = %v, want 200ms", cfg.PollInterval)
	}
	if cfg.IteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Errorf("IteratorType = %v, want TRIM_HORIZON", cfg.IteratorType)
	}
}

func TestLoadEnvConfig_OverridesAndValidation(t *testing.T) {
	t.Setenv("FLOCI_SYNC_SOURCE_ENDPOINT", "http://floci-a:4566")
	t.Setenv("FLOCI_SYNC_SOURCE_REGION", "us-east-1")
	t.Setenv("FLOCI_SYNC_SOURCE_TABLE", "stravin-core")
	t.Setenv("FLOCI_SYNC_TARGET_ENDPOINT", "http://floci-b:4566")
	t.Setenv("FLOCI_SYNC_TARGET_REGION", "us-west-2")
	t.Setenv("FLOCI_SYNC_TARGET_TABLE", "stravin-core")
	t.Setenv("FLOCI_SYNC_LAG_DELAY", "150ms")
	t.Setenv("FLOCI_SYNC_POLL_INTERVAL", "50ms")
	t.Setenv("FLOCI_SYNC_ITERATOR_TYPE", "LATEST")

	cfg, err := loadEnvConfig()
	if err != nil {
		t.Fatalf("loadEnvConfig: %v", err)
	}
	if cfg.LagDelay != 150*time.Millisecond {
		t.Errorf("LagDelay = %v, want 150ms", cfg.LagDelay)
	}
	if cfg.PollInterval != 50*time.Millisecond {
		t.Errorf("PollInterval = %v, want 50ms", cfg.PollInterval)
	}
	if cfg.IteratorType != streamtypes.ShardIteratorTypeLatest {
		t.Errorf("IteratorType = %v, want LATEST", cfg.IteratorType)
	}

	t.Setenv("FLOCI_SYNC_SOURCE_ENDPOINT", "")
	if _, err := loadEnvConfig(); err == nil {
		t.Fatal("expected error when FLOCI_SYNC_SOURCE_ENDPOINT is empty")
	}
}
