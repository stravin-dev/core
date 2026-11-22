package telemetry_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGrafanaGoldenSignalsDashboard_ValidJSONAndPanels(t *testing.T) {
	dashPath := filepath.Join("..", "..", "deploy", "local", "grafana", "provisioning", "dashboards", "stravin-golden-signals.json")
	raw, err := os.ReadFile(dashPath)
	if err != nil {
		t.Fatalf("read dashboard JSON: %v", err)
	}

	var dash struct {
		UID           string `json:"uid"`
		Title         string `json:"title"`
		SchemaVersion int    `json:"schemaVersion"`
		Panels        []struct {
			Title   string `json:"title"`
			Type    string `json:"type"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("invalid dashboard JSON: %v", err)
	}

	if dash.UID == "" || dash.Title == "" {
		t.Fatalf("expected non-empty uid and title, got uid=%q title=%q", dash.UID, dash.Title)
	}
	if dash.SchemaVersion == 0 {
		t.Error("expected non-zero schemaVersion")
	}

	var allExprs []string
	for _, p := range dash.Panels {
		for _, tgt := range p.Targets {
			allExprs = append(allExprs, tgt.Expr)
		}
	}
	joined := strings.Join(allExprs, "\n")

	requiredMetrics := []string{
		"stravin_edge_requests_total",
		"stravin_edge_request_duration_seconds",
		"stravin_ddb_transact_total",
		"stravin_ddb_cas_retries_total",
		"stravin_s3_ops_total",
		"stravin_s3_origin_fallback_total",
		"stravin_hook_wasm_duration_seconds",
		"stravin_hook_wasm_verdicts_total",
		"stravin_node_cache_hits_total",
		"stravin_node_cache_misses_total",
		"stravin_node_cache_bytes_used",
		"stravin_pack_uncompacted_packs",
		"stravin_pack_compaction_lag_seconds",
		"stravin_pack_gc_reaped_total",
		"otelcol_exporter_enqueue_failed_spans",
		"stravin_otel_dropped_items_total",
	}
	for _, m := range requiredMetrics {
		if !strings.Contains(joined, m) {
			t.Errorf("dashboard missing required metric %q", m)
		}
	}

	provPath := filepath.Join("..", "..", "deploy", "local", "grafana", "provisioning", "dashboards", "dashboards.yaml")
	provRaw, err := os.ReadFile(provPath)
	if err != nil {
		t.Fatalf("read dashboards.yaml: %v", err)
	}
	if !strings.Contains(string(provRaw), "/etc/grafana/provisioning/dashboards") {
		t.Errorf("dashboards.yaml missing expected provisioning path")
	}
}
