package telemetry_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
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

type grafanaThresholdStep struct {
	Color string   `json:"color"`
	Value *float64 `json:"value"`
}

type grafanaThresholds struct {
	Mode  string                 `json:"mode"`
	Steps []grafanaThresholdStep `json:"steps"`
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
			Links      []grafanaDataLink `json:"links"`
			Thresholds grafanaThresholds `json:"thresholds"`
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
	hasValkeyLink := false
	for _, link := range dash.Links {
		if strings.Contains(link.URL, "stravin-loadgen") {
			hasLoadgenLink = true
		}
		if strings.Contains(link.URL, "stravin-valkey") {
			hasValkeyLink = true
		}
	}
	if !hasLoadgenLink {
		t.Error("stravin-core dashboard missing cross-link to stravin-loadgen")
	}
	if !hasValkeyLink {
		t.Error("stravin-core dashboard missing cross-link to stravin-valkey")
	}

	sections := extractSections(dash.Panels)
	wantMetricRows := []string{
		"Edge Layer",
		"Node & Cache Layer",
		"Compaction & GC Layer",
		"Control Plane & Recovery Layer",
		"Event Relay Layer",
	}
	wantAllRows := append([]string{"Overview"}, append(append([]string{}, wantMetricRows...), "Logs & Trace Correlation")...)

	if len(sections) < len(wantAllRows) {
		t.Fatalf("stravin-core has %d collapsible row sections, want at least %d", len(sections), len(wantAllRows))
	}

	if !strings.Contains(sections[0].row.Title, "Overview") {
		t.Errorf("stravin-core first row = %q, want Overview row at top", sections[0].row.Title)
	}
	if sections[0].row.Collapsed {
		t.Errorf("stravin-core Overview row must be expanded (collapsed=false)")
	}
	if sections[0].row.GridPos.Y != 0 {
		t.Errorf("stravin-core Overview row gridPos.y = %d, want 0", sections[0].row.GridPos.Y)
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

	assertCoreOverviewSection(t, foundRows["Overview"])

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

	if nodeSec, ok := foundRows["Node & Cache Layer"]; ok {
		var nodeExprs []string
		for _, p := range nodeSec.panels {
			for _, tgt := range p.Targets {
				nodeExprs = append(nodeExprs, tgt.Expr)
			}
		}
		joined := strings.Join(nodeExprs, "\n")
		for _, tok := range []string{
			"stravin_node_aux_index_refresh_failures_total",
			"stravin_node_upload_pack_queue_wait_seconds_bucket",
			"stravin_node_upload_pack_queue_timeouts_total",
			"stravin_cache_active_packs",
			"stravin_cache_pending_prune_packs",
			"stravin_cache_repo_active_packs_max",
			"stravin_cache_repo_pending_prune_packs_max",
			"stravin_cache_packs_pruned_total",
		} {
			if !strings.Contains(joined, tok) {
				t.Errorf("Node & Cache Layer row missing expected metric %q", tok)
			}
		}
	}
}

var counterSelectorRe = regexp.MustCompile(`(?:stravin|redis)_[a-z0-9_]+_total(?:\{[^}]*\})?`)

func assertNoUnboundedCounters(t *testing.T, dashboard, panelTitle, expr string) {
	t.Helper()
	for _, loc := range counterSelectorRe.FindAllStringIndex(expr, -1) {
		rest := expr[loc[1]:]
		if !strings.HasPrefix(rest, "[") {
			t.Errorf("%s Overview panel %q queries unbounded counter %q in expr %q",
				dashboard, panelTitle, expr[loc[0]:loc[1]], expr)
		}
	}
}

func assertRangeScopedCounterExpr(t *testing.T, dashboard, panelTitle, expr string) {
	t.Helper()
	if !strings.Contains(expr, "increase(") || !strings.Contains(expr, "[$__range]") {
		t.Errorf("%s Overview error panel %q must use increase(...[$__range]), got %q",
			dashboard, panelTitle, expr)
	}
	if !strings.Contains(expr, "or vector(0)") {
		t.Errorf("%s Overview error panel %q must include `or vector(0)` for empty ranges, got %q",
			dashboard, panelTitle, expr)
	}
	matches := counterSelectorRe.FindAllStringIndex(expr, -1)
	if len(matches) == 0 {
		t.Errorf("%s Overview error panel %q expected at least one counter metric in expr %q",
			dashboard, panelTitle, expr)
		return
	}
	for _, loc := range matches {
		rest := expr[loc[1]:]
		if !strings.HasPrefix(rest, "[$__range]") {
			t.Errorf("%s Overview error panel %q counter %q must be scoped with [$__range] in expr %q",
				dashboard, panelTitle, expr[loc[0]:loc[1]], expr)
		}
	}
}

func assertCoreOverviewSection(t *testing.T, sec dashboardSection) {
	t.Helper()
	if len(sec.panels) < 6 {
		t.Fatalf("stravin-core Overview row has %d panels, want at least 6 KPI stat panels", len(sec.panels))
	}

	requiredRangeScopedPanels := map[string]bool{
		"System Health (Server Error Rate)":          false,
		"Total Server Errors":                        false,
		"Client Canceled (499) & Rate Limited (429)": false,
		"Compaction & Control Errors":                false,
	}

	var exprs []string
	statCount := 0
	thresholdedHealthPanels := 0
	for _, p := range sec.panels {
		if p.Type == "stat" {
			statCount++
		}
		hasGreen, hasRed := false, false
		for _, step := range p.FieldConfig.Defaults.Thresholds.Steps {
			if step.Color == "green" {
				hasGreen = true
			}
			if step.Color == "red" {
				hasRed = true
			}
		}
		if hasGreen && hasRed {
			thresholdedHealthPanels++
		}
		_, mustScopeRange := requiredRangeScopedPanels[p.Title]
		if mustScopeRange {
			requiredRangeScopedPanels[p.Title] = true
		}
		for _, tgt := range p.Targets {
			exprs = append(exprs, tgt.Expr)
			if p.Type == "stat" {
				assertNoUnboundedCounters(t, "stravin-core", p.Title, tgt.Expr)
			}
			if mustScopeRange {
				assertRangeScopedCounterExpr(t, "stravin-core", p.Title, tgt.Expr)
			}
		}
	}
	for title, found := range requiredRangeScopedPanels {
		if !found {
			t.Errorf("stravin-core Overview row missing expected error stat panel %q", title)
		}
	}
	if statCount < 6 {
		t.Errorf("stravin-core Overview row has %d stat panels, want at least 6", statCount)
	}
	if thresholdedHealthPanels < 4 {
		t.Errorf("stravin-core Overview row has %d panels with green/red health thresholds, want at least 4", thresholdedHealthPanels)
	}

	joined := strings.Join(exprs, "\n")
	for _, tok := range []string{
		"stravin_edge_requests_total",
		"stravin_edge_request_duration_seconds_bucket",
		"histogram_quantile(0.99",
		"client_canceled",
		"rate_limited",
		"stravin_node_requests_total",
		"stravin_node_s3_pack_fetches_total",
		"stravin_node_aux_index_refresh_failures_total",
		"stravin_node_cache_hits_total",
		"stravin_node_cache_misses_total",
		"stravin_cache_pending_prune_packs",
		"stravin_pack_compaction_cycles_total",
		"stravin_pack_uncompacted_packs",
		"stravin_ctrl_lease_renewals_total",
		"stravin_ctrl_scrub_runs_total",
		"stravin_ctrl_recovery_runs_total",
	} {
		if !strings.Contains(joined, tok) {
			t.Errorf("stravin-core Overview row missing expected PromQL token %q", tok)
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
		"stravin_edge_pack_cache_lookups_total",
		"stravin_edge_pack_cache_writes_total",
		"stravin_edge_pack_cache_op_duration_seconds_bucket",
		"stravin_edge_pack_cache_bytes_total",
		"stravin_edge_valkey_pool_dials_total",
		"stravin_edge_meta_cache_lookups_total",
		"stravin_edge_active_clones",
		"stravin_edge_info_refs_retries_total",
		// Node & Cache Layer (stravin-node)
		"stravin_node_requests_total",
		"stravin_node_request_duration_seconds_bucket",
		"stravin_node_cache_hits_total",
		"stravin_node_cache_misses_total",
		"stravin_node_cache_bytes_used",
		"stravin_node_s3_pack_fetches_total",
		"stravin_node_aux_index_refresh_failures_total",
		"stravin_node_upload_pack_queue_wait_seconds_bucket",
		"stravin_node_upload_pack_queue_timeouts_total",
		"stravin_cache_active_packs",
		"stravin_cache_pending_prune_packs",
		"stravin_cache_repo_active_packs_max",
		"stravin_cache_repo_pending_prune_packs_max",
		"stravin_cache_packs_pruned_total",
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
		// Event Relay Layer (stravin-relay)
		"stravin_relay_publishes_total",
		"stravin_relay_publish_duration_seconds_bucket",
		"stravin_relay_stream_records_total",
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

func assertSingleGreenBaseThreshold(t *testing.T, dashboard string, p grafanaPanel) {
	t.Helper()
	th := p.FieldConfig.Defaults.Thresholds
	if th.Mode != "absolute" {
		t.Errorf("%s stat panel %q (id=%d) thresholds.mode = %q, want %q", dashboard, p.Title, p.ID, th.Mode, "absolute")
	}
	if len(th.Steps) != 1 {
		t.Errorf("%s stat panel %q (id=%d) has %d threshold steps, want 1 base step (got %+v)", dashboard, p.Title, p.ID, len(th.Steps), th.Steps)
		return
	}
	if th.Steps[0].Color != "green" || th.Steps[0].Value != nil {
		t.Errorf("%s stat panel %q (id=%d) threshold step = %+v, want {Color:\"green\", Value:nil}", dashboard, p.Title, p.ID, th.Steps[0])
	}
}

func TestGrafanaDashboards_StatPanelThresholds(t *testing.T) {
	cases := []struct {
		filename             string
		singleGreenStatPanel map[int]string
	}{
		{
			filename: "stravin-core.json",
			singleGreenStatPanel: map[int]string{
				103: "Client Canceled (499) & Rate Limited (429)",
				105: "Total Throughput (Edge req/s)",
			},
		},
		{
			filename: "stravin-loadgen.json",
			singleGreenStatPanel: map[int]string{
				2: "Total Ops/sec (Dev vs Agent)",
				3: "Active Worker Threads",
			},
		},
		{
			filename: "stravin-valkey.json",
			singleGreenStatPanel: map[int]string{
				104: "Commands/sec",
				106: "Connected Clients",
			},
		},
	}

	for _, tc := range cases {
		dash := loadDashboard(t, tc.filename)
		found := make(map[int]bool, len(tc.singleGreenStatPanel))

		for _, p := range flattenPanels(dash.Panels) {
			if p.Type != "stat" {
				continue
			}
			th := p.FieldConfig.Defaults.Thresholds
			if th.Mode == "" || len(th.Steps) == 0 || th.Steps[0].Value != nil {
				t.Errorf("%s stat panel %q (id=%d) must declare explicit thresholds with a null base step to avoid Grafana's default 80-red threshold",
					tc.filename, p.Title, p.ID)
			}
			if wantTitle, ok := tc.singleGreenStatPanel[p.ID]; ok {
				found[p.ID] = true
				if p.Title != wantTitle {
					t.Errorf("%s panel id=%d title = %q, want %q", tc.filename, p.ID, p.Title, wantTitle)
				}
				assertSingleGreenBaseThreshold(t, tc.filename, p)
			}
		}

		for id, title := range tc.singleGreenStatPanel {
			if !found[id] {
				t.Errorf("%s missing expected stat panel id=%d (%q)", tc.filename, id, title)
			}
		}
	}
}

func assertLoadgenOverviewSection(t *testing.T, sec dashboardSection) {
	t.Helper()
	requiredRangeScopedPanels := map[string]bool{
		"Unexpected Errors (Total)": false,
		"git fsck Failures (Total)": false,
	}
	for _, p := range sec.panels {
		if p.Type != "stat" {
			continue
		}
		_, mustScopeRange := requiredRangeScopedPanels[p.Title]
		if mustScopeRange {
			requiredRangeScopedPanels[p.Title] = true
		}
		for _, tgt := range p.Targets {
			assertNoUnboundedCounters(t, "stravin-loadgen", p.Title, tgt.Expr)
			if mustScopeRange {
				assertRangeScopedCounterExpr(t, "stravin-loadgen", p.Title, tgt.Expr)
			}
		}
	}
	for title, found := range requiredRangeScopedPanels {
		if !found {
			t.Errorf("stravin-loadgen Overview row missing expected error stat panel %q", title)
		}
	}
}

func assertLoadgenDashboardRowsAndPanels(t *testing.T, dash grafanaDashboard) {
	t.Helper()

	assertNoGridOverlaps(t, dash.Panels, "stravin-loadgen top-level")

	hasCoreLink := false
	hasValkeyLink := false
	for _, link := range dash.Links {
		if strings.Contains(link.URL, "stravin-core") {
			hasCoreLink = true
		}
		if strings.Contains(link.URL, "stravin-valkey") {
			hasValkeyLink = true
		}
	}
	if !hasCoreLink {
		t.Error("stravin-loadgen dashboard missing cross-link to stravin-core")
	}
	if !hasValkeyLink {
		t.Error("stravin-loadgen dashboard missing cross-link to stravin-valkey")
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

	if overviewSec, ok := foundRows["Overview"]; ok {
		assertLoadgenOverviewSection(t, overviewSec)
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
	promCfg := string(promRaw)
	for _, tok := range []string{
		"service.name",
		"job_name: valkey",
		"valkey-exporter:9121",
		"job_name: nats",
		"nats-exporter:7777",
	} {
		if !strings.Contains(promCfg, tok) {
			t.Errorf("prometheus.yml missing required setting %q", tok)
		}
	}
}

func TestGrafanaDashboards_StravinValkey(t *testing.T) {
	dash := loadDashboard(t, "stravin-valkey.json")

	if dash.UID != "stravin-valkey" {
		t.Errorf("dashboard uid = %q, want %q", dash.UID, "stravin-valkey")
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

	assertNoGridOverlaps(t, dash.Panels, "stravin-valkey top-level")

	hasCoreLink, hasLoadgenLink := false, false
	for _, link := range dash.Links {
		if strings.Contains(link.URL, "stravin-core") {
			hasCoreLink = true
		}
		if strings.Contains(link.URL, "stravin-loadgen") {
			hasLoadgenLink = true
		}
	}
	if !hasCoreLink {
		t.Error("stravin-valkey dashboard missing cross-link to stravin-core")
	}
	if !hasLoadgenLink {
		t.Error("stravin-valkey dashboard missing cross-link to stravin-loadgen")
	}

	allPanels := flattenPanels(dash.Panels)
	seenIDs := make(map[int]bool, len(allPanels))
	var allExprs []string
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
		for _, tgt := range p.Targets {
			if !allowedDatasourceUIDs[tgt.Datasource.UID] {
				t.Errorf("panel %q target %q references unknown datasource uid %q", p.Title, tgt.RefID, tgt.Datasource.UID)
			}
			allExprs = append(allExprs, tgt.Expr)
		}
	}

	joinedExprs := strings.Join(allExprs, "\n")
	for _, m := range []string{
		"redis_up",
		"redis_memory_used_bytes",
		"redis_memory_max_bytes",
		"redis_memory_used_rss_bytes",
		"redis_mem_fragmentation_ratio",
		"redis_evicted_keys_total",
		"redis_expired_keys_total",
		"redis_db_keys",
		"redis_db_keys_expiring",
		"redis_keyspace_hits_total",
		"redis_keyspace_misses_total",
		"redis_commands_processed_total",
		"redis_commands_total",
		"redis_commands_duration_seconds_total",
		"redis_net_input_bytes_total",
		"redis_net_output_bytes_total",
		"redis_connected_clients",
		"redis_blocked_clients",
		"redis_rejected_connections_total",
		"redis_cpu_sys_seconds_total",
		"redis_cpu_user_seconds_total",
		"redis_uptime_in_seconds",
	} {
		if !strings.Contains(joinedExprs, m) {
			t.Errorf("stravin-valkey dashboard missing required metric %q", m)
		}
	}

	sections := extractSections(dash.Panels)
	wantDetailRows := []string{
		"Memory & Eviction",
		"Throughput, Hit Rate & Commands",
		"Connections & Health",
	}
	wantAllRows := append([]string{"Overview"}, wantDetailRows...)
	if len(sections) < len(wantAllRows) {
		t.Fatalf("stravin-valkey has %d row sections, want at least %d", len(sections), len(wantAllRows))
	}

	if !strings.Contains(sections[0].row.Title, "Overview") {
		t.Errorf("stravin-valkey first row = %q, want Overview row at top", sections[0].row.Title)
	}
	if sections[0].row.Collapsed {
		t.Error("stravin-valkey Overview row must be expanded (collapsed=false)")
	}
	if sections[0].row.GridPos.Y != 0 {
		t.Errorf("stravin-valkey Overview row gridPos.y = %d, want 0", sections[0].row.GridPos.Y)
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
			t.Errorf("stravin-valkey missing row section matching %q", wantRow)
		}
	}

	if overviewSec, ok := foundRows["Overview"]; ok {
		if len(overviewSec.panels) != 6 {
			t.Errorf("stravin-valkey Overview row has %d panels, want 6 KPI stat panels", len(overviewSec.panels))
		}
		requiredRangeScoped := map[string]bool{
			"Keyspace Hit Ratio %": false,
			"Evicted Keys":         false,
		}
		thresholdedHealth := 0
		for _, p := range overviewSec.panels {
			if p.Type != "stat" {
				t.Errorf("stravin-valkey Overview panel %q type = %q, want stat", p.Title, p.Type)
			}
			hasGreen, hasRed := false, false
			for _, step := range p.FieldConfig.Defaults.Thresholds.Steps {
				if step.Color == "green" {
					hasGreen = true
				}
				if step.Color == "red" {
					hasRed = true
				}
			}
			if hasGreen && hasRed {
				thresholdedHealth++
			}
			_, mustScope := requiredRangeScoped[p.Title]
			if mustScope {
				requiredRangeScoped[p.Title] = true
			}
			for _, tgt := range p.Targets {
				assertNoUnboundedCounters(t, "stravin-valkey", p.Title, tgt.Expr)
				if mustScope {
					assertRangeScopedCounterExpr(t, "stravin-valkey", p.Title, tgt.Expr)
				}
			}
		}
		for title, found := range requiredRangeScoped {
			if !found {
				t.Errorf("stravin-valkey Overview row missing expected range-scoped stat panel %q", title)
			}
		}
		if thresholdedHealth < 4 {
			t.Errorf("stravin-valkey Overview row has %d panels with green/red health thresholds, want at least 4", thresholdedHealth)
		}
	}

	validFourUpX := map[int]bool{0: true, 6: true, 12: true, 18: true}
	for _, detailRow := range wantDetailRows {
		sec, ok := foundRows[detailRow]
		if !ok {
			continue
		}
		if len(sec.panels) == 0 || len(sec.panels)%4 != 0 {
			t.Errorf("row %q has %d panels, want a non-zero multiple of 4", sec.row.Title, len(sec.panels))
		}
		byY := make(map[int]map[int]bool)
		for _, p := range sec.panels {
			if p.GridPos.W != 6 {
				t.Errorf("row %q panel %q gridPos.w = %d, want 6", sec.row.Title, p.Title, p.GridPos.W)
			}
			if !validFourUpX[p.GridPos.X] {
				t.Errorf("row %q panel %q gridPos.x = %d, want one of {0, 6, 12, 18}", sec.row.Title, p.Title, p.GridPos.X)
			}
			if p.GridPos.H != 8 {
				t.Errorf("row %q panel %q gridPos.h = %d, want 8", sec.row.Title, p.Title, p.GridPos.H)
			}
			if byY[p.GridPos.Y] == nil {
				byY[p.GridPos.Y] = make(map[int]bool)
			}
			byY[p.GridPos.Y][p.GridPos.X] = true
		}
		for y, xs := range byY {
			if len(xs) != 4 {
				t.Errorf("row %q line y=%d has %d panels across, want 4", sec.row.Title, y, len(xs))
			}
		}
	}
}
