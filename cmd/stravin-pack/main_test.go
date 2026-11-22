package main

import (
	"testing"
	"time"

	"stravin.dev/core/internal/compaction"
)

func TestLoadConfig_CRRReplicationGateDefaultsAndOverrides(t *testing.T) {
	t.Run("disabled_by_default_for_single_region", func(t *testing.T) {
		t.Setenv("STRAVIN_PACK_REQUIRE_CRR_REPLICATION", "")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_THRESHOLD", "")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_COOLDOWN", "")

		cfg := loadConfig()
		if cfg.RequireCRRReplication {
			t.Fatal("RequireCRRReplication must default to false")
		}
		if cfg.CRRBreakerThreshold != compaction.DefaultCRRBreakerThreshold {
			t.Fatalf("CRRBreakerThreshold = %d, want %d", cfg.CRRBreakerThreshold, compaction.DefaultCRRBreakerThreshold)
		}
		if cfg.CRRBreakerCooldown != compaction.DefaultCRRBreakerCooldown {
			t.Fatalf("CRRBreakerCooldown = %v, want %v", cfg.CRRBreakerCooldown, compaction.DefaultCRRBreakerCooldown)
		}
	})

	t.Run("enabled_with_custom_breaker_settings", func(t *testing.T) {
		t.Setenv("STRAVIN_PACK_REQUIRE_CRR_REPLICATION", "true")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_THRESHOLD", "8")
		t.Setenv("STRAVIN_PACK_CRR_BREAKER_COOLDOWN", "20m")

		cfg := loadConfig()
		if !cfg.RequireCRRReplication {
			t.Fatal("expected RequireCRRReplication = true")
		}
		if cfg.CRRBreakerThreshold != 8 {
			t.Fatalf("CRRBreakerThreshold = %d, want 8", cfg.CRRBreakerThreshold)
		}
		if cfg.CRRBreakerCooldown != 20*time.Minute {
			t.Fatalf("CRRBreakerCooldown = %v, want 20m", cfg.CRRBreakerCooldown)
		}
	})
}
