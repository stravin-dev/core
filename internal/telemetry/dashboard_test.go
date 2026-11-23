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

type grafanaGridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type grafanaPanel struct {
	ID          int                  `json:"id"`
	Title       string               `json:"title"`
	Type        string               `json:"type"`
	Collapsed   bool                 `json:"collapsed"`
	GridPos     grafanaGridPos       `json:"gridPos"`
	Datasource  grafanaDatasourceRef `json:"datasource"`
	Targets     []grafanaTarget      `json:"targets"`
	Panels      []grafanaPanel       `json:"panels"`
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

func flattenPanels(panels []grafanaPanel) []grafanaPanel {
	var out []grafanaPanel
	for _, p := range panels {
		out = append(out, p)
		if len(p.Panels) > 0 {
			out = append(out, flattenPanels(p.Panels)...)
		}
	}
	return out
}

func gridsOverlap(a, b grafanaGridPos) bool {
	return a.X < b.X+b.W && b.X < a.X+a.W && a.Y < b.Y+b.H && b.Y < a.Y+a.H
}

func assertNoGridOverlaps(t *testing.T, panels []grafanaPanel, scope string) {
	t.Helper()
	for i := 0; i < len(panels); i++ {
		gp := panels[i].GridPos
		if gp.W <= 0 || gp.H <= 0 || gp.X < 0 || gp.Y < 0 || gp.X+gp.W > 24 {
			t.Errorf("%s panel %q has invalid gridPos %+v", scope, panels[i].Title, gp)
		}
		for j := i + 1; j < len(panels); j++ {
			if gridsOverlap(gp, panels[j].GridPos) {
				t.Errorf("%s panels %q (%+v) and %q (%+v) overlap",
					scope, panels[i].Title, gp, panels[j].Title, panels[j].GridPos)
			}
		}
	}
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

	allPanels := flattenPanels(dash.Panels)
	seenIDs := make(map[int]bool, len(allPanels))
	var allExprs []string
	var allLinks []string
	for _, link := range dash.Links {
		allLinks = append(allLinks, link.URL)
	}

	hasExemplar := false
	hasLogsPanel := false

	for _, p := range allPanels {
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
		if p.Type == "row" {
			continue
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

type dashboardSection struct {
	row    grafanaPanel
	panels []grafanaPanel
}

func extractSections(panels []grafanaPanel) []dashboardSection {
	var sections []dashboardSection
	for _, p := range panels {
		if p.Type == "row" {
			sec := dashboardSection{row: p}
			if len(p.Panels) > 0 {
				sec.panels = append(sec.panels, p.Panels...)
			}
			sections = append(sections, sec)
			continue
		}
		if len(sections) > 0 {
			sections[len(sections)-1].panels = append(sections[len(sections)-1].panels, p)
		}
	}
	return sections
}

func assertCoreDashboardRowsAndGrid(t *testing.T, dash grafanaDashboard) {
	t.Helper()

	assertNoGridOverlaps(t, dash.Panels, "stravin-core top-level")

	hasLoadgenLink := false
	for _, link := range dash.Links {
		if strings.Contains(link.URL, "stravin-loadgen") {
			hasLoadgenLink = true
			break
		}
	}
	if !hasLoadgenLink {
		t.Error("stravin-core dashboard missing cross-link to stravin-loadgen")
	}

	sections := extractSections(dash.Panels)
	wantMetricRows := []string{
		"Edge Layer",
		"Node & Cache Layer",
		"Compaction & GC Layer",
		"Control Plane & Recovery Layer",
	}
	wantAllRows := append(append([]string{}, wantMetricRows...), "Logs & Trace Correlation")

	if len(sections) < len(wantAllRows) {
		t.Fatalf("stravin-core has %d collapsible row sections, want at least %d", len(sections), len(wantAllRows))
	}

	foundRows := make(map[string]dashboardSection, len(sections))
	for _, sec := range sections {
		if sec.row.GridPos.W != 24 || sec.row.GridPos.X != 0 || sec.row.GridPos.H != 1 {
			t.Errorf("row %q gridPos = %+v, want w=24 x=0 h=1", sec.row.Title, sec.row.GridPos)
		}
		if len(sec.row.Panels) > 0 {
			assertNoGridOverlaps(t, sec.row.Panels, "row "+sec.row.Title)
		}
		for _, wantPrefix := range wantAllRows {
			if strings.Contains(sec.row.Title, wantPrefix) {
				foundRows[wantPrefix] = sec
			}
		}
	}

	for _, wantRow := range wantAllRows {
		if _, ok := foundRows[wantRow]; !ok {
			t.Errorf("stravin-core missing collapsible row section matching %q", wantRow)
		}
	}

	validFourUpX := map[int]bool{0: true, 6: true, 12: true, 18: true}
	for _, metricRow := range wantMetricRows {
		sec, ok := foundRows[metricRow]
		if !ok {
			continue
		}
		if len(sec.panels) == 0 || len(sec.panels)%4 != 0 {
			t.Errorf("row %q has %d metric panels, want a non-zero multiple of 4", sec.row.Title, len(sec.panels))
		}
		byY := make(map[int]map[int]bool)
		for _, p := range sec.panels {
			if p.GridPos.W != 6 {
				t.Errorf("row %q panel %q gridPos.w = %d, want 6 (4 panels per line)", sec.row.Title, p.Title, p.GridPos.W)
			}
			if !validFourUpX[p.GridPos.X] {
				t.Errorf("row %q panel %q gridPos.x = %d, want one of {0, 6, 12, 18}", sec.row.Title, p.Title, p.GridPos.X)
			}
			if p.GridPos.H != 8 {
				t.Errorf("row %q panel %q gridPos.h = %d, want uniform height 8", sec.row.Title, p.Title, p.GridPos.H)
			}
			if byY[p.GridPos.Y] == nil {
				byY[p.GridPos.Y] = make(map[int]bool)
			}
			byY[p.GridPos.Y][p.GridPos.X] = true
		}
		for y, xs := range byY {
			if len(xs) != 4 {
				t.Errorf("row %q line y=%d has %d panels across, want 4 (x in {0, 6, 12, 18})", sec.row.Title, y, len(xs))
			}
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
		// Edge Layer (stravin-edge)
		"stravin_edge_requests_total",
		"stravin_edge_request_duration_seconds_bucket",
		// Node & Cache Layer (stravin-node)
		"stravin_node_requests_total",
		"stravin_node_request_duration_seconds_bucket",
		"stravin_node_cache_hits_total",
		"stravin_node_cache_misses_total",
		"stravin_node_cache_bytes_used",
		"stravin_node_s3_pack_fetches_total",
		// Compaction & GC Layer (stravin-pack)
		"stravin_pack_compaction_cycles_total",
		"stravin_pack_compaction_duration_seconds_bucket",
		"stravin_pack_uncompacted_packs",
		"stravin_pack_bytes_reclaimed_total",
		"stravin_pack_gc_reaped_total",
		"stravin_pack_lease_contention_total",
		"stravin_pack_gc_crr_blocked_total",
		"stravin_pack_gc_crr_breaker_open",
		// Control Plane & Recovery Layer (stravin-ctrl)
		"stravin_ctrl_lease_renewals_total",
		"stravin_ctrl_epoch_transitions_total",
		"stravin_ctrl_promotion_guard_total",
		"stravin_ctrl_scrub_runs_total",
		"stravin_ctrl_scrub_head_checks_total",
		"stravin_ctrl_scrub_rewound_refs_total",
		"stravin_ctrl_scrub_duration_seconds_bucket",
		"stravin_ctrl_recovery_runs_total",
		"stravin_ctrl_recovery_refs_published_total",
	}, `{service_name=~"stravin-.*"}`)
	assertCoreDashboardRowsAndGrid(t, coreDash)

	loadgenDash := loadDashboard(t, "stravin-loadgen.json")
	assertDashboardBasics(t, loadgenDash, "stravin-loadgen", []string{
		"stravin_loadgen_active_threads",
		"stravin_loadgen_ops_total",
		"stravin_loadgen_op_duration_seconds_bucket",
		"stravin_loadgen_retries_total",
		"stravin_loadgen_unexpected_errors_total",
		"stravin_loadgen_fsck_failures_total",
	}, `{service_name=~"stravin-loadgen|stravin-chaos"}`)
	assertLoadgenDashboardRowsAndPanels(t, loadgenDash)
}

func assertLoadgenDashboardRowsAndPanels(t *testing.T, dash grafanaDashboard) {
	t.Helper()

	assertNoGridOverlaps(t, dash.Panels, "stravin-loadgen top-level")

	hasCoreLink := false
	for _, link := range dash.Links {
		if strings.Contains(link.URL, "stravin-core") {
			hasCoreLink = true
			break
		}
	}
	if !hasCoreLink {
		t.Error("stravin-loadgen dashboard missing cross-link to stravin-core")
	}

	sections := extractSections(dash.Panels)
	wantRows := []string{
		"Overview",
		"Scenario Distribution",
		"Developer Experience",
		"AI Agent Experience",
		"Traces & Logs",
	}
	if len(sections) < len(wantRows) {
		t.Fatalf("stravin-loadgen has %d collapsible row sections, want at least %d", len(sections), len(wantRows))
	}

	foundRows := make(map[string]dashboardSection, len(sections))
	for _, sec := range sections {
		if sec.row.GridPos.W != 24 || sec.row.GridPos.X != 0 || sec.row.GridPos.H != 1 {
			t.Errorf("row %q gridPos = %+v, want w=24 x=0 h=1", sec.row.Title, sec.row.GridPos)
		}
		if len(sec.row.Panels) > 0 {
			assertNoGridOverlaps(t, sec.row.Panels, "row "+sec.row.Title)
		}
		for _, wantPrefix := range wantRows {
			if strings.Contains(sec.row.Title, wantPrefix) {
				foundRows[wantPrefix] = sec
			}
		}
	}
	for _, wantRow := range wantRows {
		if _, ok := foundRows[wantRow]; !ok {
			t.Errorf("stravin-loadgen missing collapsible row section matching %q", wantRow)
		}
	}

	// Verify separate Dev & AI Agent scenario distribution pie charts.
	if distSec, ok := foundRows["Scenario Distribution"]; ok {
		var hasDevPie, hasAgentPie bool
		for _, p := range distSec.panels {
			if p.Type != "piechart" {
				continue
			}
			for _, tgt := range p.Targets {
				if strings.Contains(tgt.Expr, `persona="dev"`) && strings.Contains(tgt.Expr, "scenario") {
					hasDevPie = true
				}
				if strings.Contains(tgt.Expr, `persona="agent"`) && strings.Contains(tgt.Expr, "scenario") {
					hasAgentPie = true
				}
			}
		}
		if !hasDevPie {
			t.Error("Scenario Distribution row missing Developer (persona=\"dev\") piechart panel")
		}
		if !hasAgentPie {
			t.Error("Scenario Distribution row missing AI Agent (persona=\"agent\") piechart panel")
		}
	}

	// Verify Developer and AI Agent sections contain persona-filtered ops, latency, retries, errors, and fsck.
	personaChecks := []struct {
		rowKey        string
		personaFilter string
		fsckFilter    string
	}{
		{"Developer Experience", `persona="dev"`, `dev_.*`},
		{"AI Agent Experience", `persona="agent"`, `agent_.*`},
	}
	for _, tc := range personaChecks {
		sec, ok := foundRows[tc.rowKey]
		if !ok {
			continue
		}
		var exprs []string
		hasExemplar := false
		for _, p := range sec.panels {
			for _, tgt := range p.Targets {
				exprs = append(exprs, tgt.Expr)
				if tgt.Exemplar {
					hasExemplar = true
				}
			}
		}
		joined := strings.Join(exprs, "\n")
		for _, tok := range []string{
			tc.personaFilter,
			"stravin_loadgen_ops_total",
			"stravin_loadgen_op_duration_seconds_bucket",
			"histogram_quantile(0.50",
			"histogram_quantile(0.95",
			"histogram_quantile(0.99",
			"stravin_loadgen_retries_total",
			"stravin_loadgen_unexpected_errors_total",
			"stravin_loadgen_fsck_failures_total",
			tc.fsckFilter,
		} {
			if !strings.Contains(joined, tok) {
				t.Errorf("row %q missing expected PromQL token %q", tc.rowKey, tok)
			}
		}
		if !hasExemplar {
			t.Errorf("row %q missing exemplar: true on latency targets", tc.rowKey)
		}
	}
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
