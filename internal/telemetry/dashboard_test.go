package telemetry_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type grafanaDatasourceRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type grafanaDataLink struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

type grafanaTarget struct {
	Datasource grafanaDatasourceRef `json:"datasource"`
	Expr       string               `json:"expr"`
	Exemplar   bool                 `json:"exemplar"`
	RefID      string               `json:"refId"`
}

type grafanaPanel struct {
	ID          int                  `json:"id"`
	Title       string               `json:"title"`
	Type        string               `json:"type"`
	Datasource  grafanaDatasourceRef `json:"datasource"`
	Targets     []grafanaTarget      `json:"targets"`
	FieldConfig struct {
		Defaults struct {
			Links []grafanaDataLink `json:"links"`
		} `json:"defaults"`
	} `json:"fieldConfig"`
}

type grafanaDashboard struct {
	UID           string            `json:"uid"`
	Title         string            `json:"title"`
	SchemaVersion int               `json:"schemaVersion"`
	Links         []grafanaDataLink `json:"links"`
	Panels        []grafanaPanel    `json:"panels"`
}

var allowedDatasourceUIDs = map[string]bool{
	"prometheus":    true,
	"loki":          true,
	"tempo":         true,
	"-- Grafana --": true,
}

func deployLocalPath(parts ...string) string {
	all := append([]string{"..", "..", "deploy", "local"}, parts...)
	return filepath.Join(all...)
}

func loadDashboard(t *testing.T, filename string) grafanaDashboard {
	t.Helper()
	path := deployLocalPath("grafana", "provisioning", "dashboards", filename)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dashboard %s: %v", filename, err)
	}
	var dash grafanaDashboard
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("unmarshal dashboard %s: %v", filename, err)
	}
	return dash
}

func assertDashboardBasics(t *testing.T, dash grafanaDashboard, wantUID string, requiredMetrics []string, wantLogExpr string) {
	t.Helper()

	if dash.UID != wantUID {
		t.Errorf("dashboard uid = %q, want %q", dash.UID, wantUID)
	}
	if strings.TrimSpace(dash.Title) == "" {
		t.Error("expected non-empty dashboard title")
	}
	if dash.SchemaVersion == 0 {
		t.Error("expected non-zero schemaVersion")
	}
	if len(dash.Panels) == 0 {
		t.Fatal("expected at least one panel")
	}

	seenIDs := make(map[int]bool, len(dash.Panels))
	var allExprs []string
	var allLinks []string
	for _, link := range dash.Links {
		allLinks = append(allLinks, link.URL)
	}

	hasExemplar := false
	hasLogsPanel := false

	for _, p := range dash.Panels {
		if p.ID <= 0 {
			t.Errorf("panel %q has invalid id %d", p.Title, p.ID)
		}
		if seenIDs[p.ID] {
			t.Errorf("duplicate panel id %d (%q)", p.ID, p.Title)
		}
		seenIDs[p.ID] = true

		if strings.TrimSpace(p.Title) == "" || strings.TrimSpace(p.Type) == "" {
			t.Errorf("panel id=%d must have non-empty title and type", p.ID)
		}
		if p.Datasource.UID != "" && !allowedDatasourceUIDs[p.Datasource.UID] {
			t.Errorf("panel %q references unknown datasource uid %q", p.Title, p.Datasource.UID)
		}
		for _, link := range p.FieldConfig.Defaults.Links {
			allLinks = append(allLinks, link.URL)
		}
		for _, tgt := range p.Targets {
			if !allowedDatasourceUIDs[tgt.Datasource.UID] {
				t.Errorf("panel %q target %q references unknown datasource uid %q", p.Title, tgt.RefID, tgt.Datasource.UID)
			}
			if tgt.Exemplar {
				hasExemplar = true
			}
			allExprs = append(allExprs, tgt.Expr)
			if p.Type == "logs" && tgt.Datasource.UID == "loki" && strings.Contains(tgt.Expr, wantLogExpr) {
				hasLogsPanel = true
			}
		}
	}

	if !hasExemplar {
		t.Errorf("dashboard %q must enable exemplar: true on at least one latency target", dash.UID)
	}
	if !hasLogsPanel {
		t.Errorf("dashboard %q missing embedded Loki logs panel matching %q", dash.UID, wantLogExpr)
	}

	joinedLinks := strings.Join(allLinks, "\n")
	if !strings.Contains(joinedLinks, "tempo") {
		t.Errorf("dashboard %q missing Explore data link to Tempo", dash.UID)
	}
	if !strings.Contains(joinedLinks, "loki") {
		t.Errorf("dashboard %q missing Explore data link to Loki", dash.UID)
	}

	joinedExprs := strings.Join(allExprs, "\n")
	for _, m := range requiredMetrics {
		if !strings.Contains(joinedExprs, m) {
			t.Errorf("dashboard %q missing required metric %q", dash.UID, m)
		}
	}
}

func TestGrafanaDashboards_StravinCoreAndLoadTester(t *testing.T) {
	legacyPath := deployLocalPath("grafana", "provisioning", "dashboards", "stravin-golden-signals.json")
	if _, err := os.Stat(legacyPath); err == nil {
		t.Errorf("legacy dashboard %s should be replaced by stravin-core.json", legacyPath)
	}

	coreDash := loadDashboard(t, "stravin-core.json")
	assertDashboardBasics(t, coreDash, "stravin-core", []string{
		"stravin_edge_requests_total",
		"stravin_edge_request_duration_seconds",
		"stravin_node_requests_total",
		"stravin_node_request_duration_seconds",
		"stravin_node_cache_hits_total",
		"stravin_node_cache_misses_total",
		"stravin_node_cache_bytes_used",
		"stravin_node_s3_pack_fetches_total",
		"stravin_ddb_transact_total",
		"stravin_ddb_cas_retries_total",
		"stravin_s3_ops_total",
		"stravin_s3_origin_fallback_total",
		"stravin_hook_wasm_duration_seconds",
		"stravin_hook_wasm_verdicts_total",
		"stravin_pack_uncompacted_packs",
		"stravin_pack_compaction_lag_seconds",
		"stravin_pack_compaction_cycles_total",
		"stravin_pack_gc_reaped_total",
		"stravin_ctrl_lease_renewals_total",
		"stravin_ctrl_scrub_runs_total",
		"stravin_ctrl_recovery_runs_total",
		"otelcol_exporter_enqueue_failed_spans",
		"stravin_otel_dropped_items_total",
	}, `{service_name=~"stravin-.*"}`)

	loadgenDash := loadDashboard(t, "stravin-loadgen.json")
	assertDashboardBasics(t, loadgenDash, "stravin-loadgen", []string{
		"stravin_loadgen_active_threads",
		"stravin_loadgen_ops_total",
		"stravin_loadgen_op_duration_seconds_bucket",
		"stravin_loadgen_retries_total",
		"stravin_loadgen_unexpected_errors_total",
		"stravin_loadgen_fsck_failures_total",
	}, `{service_name=~"stravin-loadgen|stravin-chaos"}`)
}

func TestGrafanaProvisioning_DatasourcesAndDashboards(t *testing.T) {
	provPath := deployLocalPath("grafana", "provisioning", "dashboards", "dashboards.yaml")
	provRaw, err := os.ReadFile(provPath)
	if err != nil {
		t.Fatalf("read dashboards.yaml: %v", err)
	}
	if !strings.Contains(string(provRaw), "/etc/grafana/provisioning/dashboards") {
		t.Error("dashboards.yaml missing expected provisioning path")
	}

	dsPath := deployLocalPath("grafana", "provisioning", "datasources", "datasources.yaml")
	dsRaw, err := os.ReadFile(dsPath)
	if err != nil {
		t.Fatalf("read datasources.yaml: %v", err)
	}
	ds := string(dsRaw)

	requiredTokens := []string{
		"uid: prometheus",
		"uid: tempo",
		"uid: loki",
		"exemplarTraceIdDestinations:",
		"datasourceUid: tempo",
		"tracesToLogsV2:",
		"datasourceUid: loki",
		"filterByTraceID: true",
		"service_name",
		"tracesToMetrics:",
		"datasourceUid: prometheus",
		"serviceMap:",
		"derivedFields:",
		"trace_id",
		"traceID",
	}
	for _, tok := range requiredTokens {
		if !strings.Contains(ds, tok) {
			t.Errorf("datasources.yaml missing required cross-link setting %q", tok)
		}
	}
}

func TestOTelAndLGTMConfigs_TracesMetricsLogsPipelines(t *testing.T) {
	otelRaw, err := os.ReadFile(deployLocalPath("otel-collector.yaml"))
	if err != nil {
		t.Fatalf("read otel-collector.yaml: %v", err)
	}
	otelCfg := string(otelRaw)
	for _, tok := range []string{
		"0.0.0.0:4317",
		"0.0.0.0:4318",
		"tempo:4317",
		"http://prometheus:9090/api/v1/otlp",
		"http://loki:3100/otlp",
		"traces:",
		"metrics:",
		"logs:",
	} {
		if !strings.Contains(otelCfg, tok) {
			t.Errorf("otel-collector.yaml missing required setting %q", tok)
		}
	}

	lokiRaw, err := os.ReadFile(deployLocalPath("loki.yaml"))
	if err != nil {
		t.Fatalf("read loki.yaml: %v", err)
	}
	if !strings.Contains(string(lokiRaw), "allow_structured_metadata: true") {
		t.Error("loki.yaml must enable allow_structured_metadata: true for OTLP ingestion")
	}

	promRaw, err := os.ReadFile(deployLocalPath("prometheus.yml"))
	if err != nil {
		t.Fatalf("read prometheus.yml: %v", err)
	}
	if !strings.Contains(string(promRaw), "service.name") {
		t.Error("prometheus.yml must promote service.name OTLP resource attribute")
	}
}
