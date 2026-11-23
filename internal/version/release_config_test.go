package version_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type releaseConfig struct {
	Branches  []string `json:"branches"`
	TagFormat string   `json:"tagFormat"`
	Plugins   []any    `json:"plugins"`
}

func TestReleaseRCConfig(t *testing.T) {
	root := findRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".releaserc.json"))
	if err != nil {
		t.Fatalf("read .releaserc.json: %v", err)
	}

	var cfg releaseConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse .releaserc.json: %v", err)
	}

	if len(cfg.Branches) != 1 || cfg.Branches[0] != "main" {
		t.Fatalf("branches = %v, want [main]", cfg.Branches)
	}
	if cfg.TagFormat != "v${version}" {
		t.Fatalf("tagFormat = %q, want %q", cfg.TagFormat, "v${version}")
	}

	plugins := make(map[string]map[string]any)
	for _, entry := range cfg.Plugins {
		switch v := entry.(type) {
		case string:
			plugins[v] = nil
		case []any:
			if len(v) == 0 {
				t.Fatal("empty plugin tuple in .releaserc.json")
			}
			name, ok := v[0].(string)
			if !ok {
				t.Fatalf("plugin tuple name is %T, want string", v[0])
			}
			var opts map[string]any
			if len(v) > 1 {
				opts, _ = v[1].(map[string]any)
			}
			plugins[name] = opts
		default:
			t.Fatalf("unexpected plugin entry type %T", entry)
		}
	}

	requiredPlugins := []string{
		"@semantic-release/commit-analyzer",
		"@semantic-release/release-notes-generator",
		"@semantic-release/changelog",
		"@semantic-release/exec",
		"@semantic-release/git",
		"@semantic-release/github",
	}
	for _, name := range requiredPlugins {
		if _, ok := plugins[name]; !ok {
			t.Errorf("missing required plugin %q in .releaserc.json", name)
		}
	}

	execOpts := plugins["@semantic-release/exec"]
	if prepareCmd, _ := execOpts["prepareCmd"].(string); !strings.Contains(prepareCmd, "./scripts/set-version.sh") {
		t.Errorf("@semantic-release/exec prepareCmd = %q, want ./scripts/set-version.sh", prepareCmd)
	}

	gitOpts := plugins["@semantic-release/git"]
	assetsRaw, _ := gitOpts["assets"].([]any)
	assets := make(map[string]bool)
	for _, a := range assetsRaw {
		if s, ok := a.(string); ok {
			assets[s] = true
		}
	}
	for _, wantAsset := range []string{"internal/version/version.go", "CHANGELOG.md"} {
		if !assets[wantAsset] {
			t.Errorf("@semantic-release/git assets missing %q", wantAsset)
		}
	}
}

func TestCIAndReleaseWorkflows(t *testing.T) {
	root := findRepoRoot(t)
	ciRaw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	ciText := string(ciRaw)

	releaseRaw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release-artifacts.yml"))
	if err != nil {
		t.Fatalf("read release-artifacts.yml: %v", err)
	}
	releaseText := string(releaseRaw)

	// Verify only Node 24 action versions are referenced (no v3/v4/v5 legacy actions).
	legacyAction := regexp.MustCompile(`uses:\s*actions/(checkout|setup-go|setup-node|setup-python|cache)@v[1-5]\b`)
	for name, content := range map[string]string{"ci.yml": ciText, "release-artifacts.yml": releaseText} {
		if m := legacyAction.FindString(content); m != "" {
			t.Errorf("%s uses legacy Node 20 action %q", name, m)
		}
	}

	for _, want := range []string{
		"semver-dry-run:",
		"release:",
		"release-artifacts:",
		"actions/setup-node@v7",
		"github.event_name == 'push' && github.ref == 'refs/heads/main'",
		"needs: [semver-dry-run, pre-commit, unit-tests, integration-tests, e2e-tests, multi-region-e2e]",
		"uses: ./.github/workflows/release-artifacts.yml",
	} {
		if !strings.Contains(ciText, want) {
			t.Errorf("ci.yml missing expected snippet %q", want)
		}
	}

	for _, want := range []string{
		"release:",
		"types: [published]",
		"workflow_call:",
		"goos: linux",
		"goarch: amd64",
		"target: linux-amd64",
		"goos: darwin",
		"goarch: arm64",
		"target: darwin-arm64",
		"stravin.dev/core/internal/version.Version=",
		"gh release upload",
		"stravin-edge",
		"stravin-node",
		"stravin-pack",
		"stravin-ctrl",
		"stravin-hook",
		"stravin-admin",
		"stravin-loadgen",
		"stravin-chaos",
	} {
		if !strings.Contains(releaseText, want) {
			t.Errorf("release-artifacts.yml missing expected snippet %q", want)
		}
	}
}
