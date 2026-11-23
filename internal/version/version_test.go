package version_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"stravin.dev/core/internal/version"
)

var semverPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)

func TestVersionDefault(t *testing.T) {
	if !semverPattern.MatchString(version.Version) {
		t.Fatalf("version.Version = %q, want valid Go semver (vX.Y.Z)", version.Version)
	}
	if got := version.String(); got != version.Version {
		t.Fatalf("version.String() = %q, want %q", got, version.Version)
	}
}

func TestVersionOverride(t *testing.T) {
	orig := version.Version
	t.Cleanup(func() {
		version.Version = orig
	})

	version.Version = "v2.3.4"
	if got := version.String(); got != "v2.3.4" {
		t.Fatalf("version.String() = %q, want %q", got, "v2.3.4")
	}
}

func TestSetVersionScript(t *testing.T) {
	repoRoot := findRepoRoot(t)
	scriptPath := filepath.Join(repoRoot, "scripts", "set-version.sh")

	cases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "bare semver", input: "1.2.3", want: "v1.2.3"},
		{name: "v-prefixed semver", input: "v2.4.0", want: "v2.4.0"},
		{name: "prerelease semver", input: "1.3.0-rc.1", want: "v1.3.0-rc.1"},
		{name: "build metadata semver", input: "v1.3.0+build.42", want: "v1.3.0+build.42"},
		{name: "empty input", input: "", wantErr: true},
		{name: "missing patch", input: "1.2", wantErr: true},
		{name: "non-numeric", input: "v1.2.x", wantErr: true},
		{name: "double v prefix", input: "vv1.2.3", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "version.go")
			cmd := exec.Command(scriptPath, tc.input, target)
			out, err := cmd.CombinedOutput()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got success: %s", tc.input, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("set-version.sh %q failed: %v\n%s", tc.input, err, out)
			}

			raw, err := os.ReadFile(target)
			if err != nil {
				t.Fatalf("read generated version.go: %v", err)
			}
			wantLine := `var Version = "` + tc.want + `"`
			if !strings.Contains(string(raw), wantLine) {
				t.Fatalf("generated file missing %q:\n%s", wantLine, raw)
			}

			if gofmtPath, lookErr := exec.LookPath("gofmt"); lookErr == nil {
				fmtOut, fmtErr := exec.Command(gofmtPath, "-l", target).CombinedOutput()
				if fmtErr != nil {
					t.Fatalf("gofmt failed: %v\n%s", fmtErr, fmtOut)
				}
				if strings.TrimSpace(string(fmtOut)) != "" {
					t.Fatalf("generated version.go is not gofmt-clean: %s", fmtOut)
				}
			}
		})
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repository root (go.mod)")
		}
		dir = parent
	}
}
