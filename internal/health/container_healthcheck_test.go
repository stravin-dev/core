package health_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func TestDockerfiles_HealthchecksConfigured(t *testing.T) {
	root := repoRoot(t)

	// 1. Main Dockerfile: all 5 daemon targets probe /readyz on their respective ports; admin has HEALTHCHECK NONE.
	dockerfileBytes, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	dockerfile := string(dockerfileBytes)
	expectedDaemonChecks := map[string]string{
		"edge": "http://127.0.0.1:8080/readyz",
		"node": "http://127.0.0.1:8081/readyz",
		"pack": "http://127.0.0.1:8082/readyz",
		"ctrl": "http://127.0.0.1:8083/readyz",
		"hook": "http://127.0.0.1:8084/readyz",
	}
	for stage, probeURL := range expectedDaemonChecks {
		if !strings.Contains(dockerfile, "AS "+stage) {
			t.Fatalf("Dockerfile missing stage %q", stage)
		}
		if !strings.Contains(dockerfile, probeURL) {
			t.Errorf("Dockerfile stage %q missing HEALTHCHECK probe %q", stage, probeURL)
		}
	}
	if !strings.Contains(dockerfile, "HEALTHCHECK NONE") {
		t.Error("Dockerfile missing explicit HEALTHCHECK NONE for CLI-only admin stage")
	}

	// 2. Dockerfile.chaos: CLI-only runner with explicit HEALTHCHECK NONE.
	chaosBytes, err := os.ReadFile(filepath.Join(root, "Dockerfile.chaos"))
	if err != nil {
		t.Fatalf("read Dockerfile.chaos: %v", err)
	}
	if !strings.Contains(string(chaosBytes), "HEALTHCHECK NONE") {
		t.Error("Dockerfile.chaos missing explicit HEALTHCHECK NONE")
	}

	// 3. Dockerfile.floci-test: process HEALTHCHECK on both sync daemons.
	flociBytes, err := os.ReadFile(filepath.Join(root, "Dockerfile.floci-test"))
	if err != nil {
		t.Fatalf("read Dockerfile.floci-test: %v", err)
	}
	floci := string(flociBytes)
	for _, bin := range []string{"floci-test-s3-sync", "floci-test-ddb-sync"} {
		if !strings.Contains(floci, "pidof "+bin) {
			t.Errorf("Dockerfile.floci-test missing HEALTHCHECK for %s", bin)
		}
	}
}

func TestComposeFiles_ProbeReadyz(t *testing.T) {
	root := repoRoot(t)
	composeFiles := []struct {
		name      string
		wantCount int
	}{
		{"docker-compose.yml", 5},
		{"docker-compose.multi-region.yml", 6},
		{"docker-compose.chaos.yml", 5},
	}

	for _, tc := range composeFiles {
		raw, err := os.ReadFile(filepath.Join(root, tc.name))
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		content := string(raw)
		if strings.Contains(content, "/healthz") {
			t.Errorf("%s still references /healthz; expected /readyz", tc.name)
		}
		got := strings.Count(content, "/readyz")
		if got != tc.wantCount {
			t.Errorf("%s has %d /readyz healthcheck(s), want %d", tc.name, got, tc.wantCount)
		}
	}
}
