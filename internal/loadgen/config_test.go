package loadgen

import (
	"bytes"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	var stderr bytes.Buffer
	cfg, err := ParseFlags(nil, &stderr)
	if err != nil {
		t.Fatalf("ParseFlags(nil) unexpected error: %v", err)
	}

	if cfg.Devs != 10 {
		t.Errorf("Devs = %d, want 10", cfg.Devs)
	}
	if cfg.Agents != 20 {
		t.Errorf("Agents = %d, want 20", cfg.Agents)
	}
	if cfg.HTTPURL != "http://127.0.0.1:8080" {
		t.Errorf("HTTPURL = %q, want http://127.0.0.1:8080", cfg.HTTPURL)
	}
	if cfg.SSHAddr != "127.0.0.1:2222" {
		t.Errorf("SSHAddr = %q, want 127.0.0.1:2222", cfg.SSHAddr)
	}
	if cfg.Tenant != "loadgen" {
		t.Errorf("Tenant = %q, want loadgen", cfg.Tenant)
	}
	wantRepos := []string{"repo-1", "repo-2", "repo-3"}
	if len(cfg.Repos) != len(wantRepos) {
		t.Fatalf("Repos = %v, want %v", cfg.Repos, wantRepos)
	}
	for i, r := range wantRepos {
		if cfg.Repos[i] != r {
			t.Errorf("Repos[%d] = %q, want %q", i, cfg.Repos[i], r)
		}
	}
	if cfg.Seed != 0 {
		t.Errorf("Seed = %d, want 0", cfg.Seed)
	}
	if cfg.Duration != 0 {
		t.Errorf("Duration = %v, want 0", cfg.Duration)
	}
	if cfg.ThinkMin != 50*time.Millisecond {
		t.Errorf("ThinkMin = %v, want 50ms", cfg.ThinkMin)
	}
	if cfg.ThinkMax != 500*time.Millisecond {
		t.Errorf("ThinkMax = %v, want 500ms", cfg.ThinkMax)
	}
	if cfg.OtelEndpoint != "http://127.0.0.1:4317" {
		t.Errorf("OtelEndpoint = %q, want http://127.0.0.1:4317", cfg.OtelEndpoint)
	}
	if cfg.FlushTimeout != 10*time.Second {
		t.Errorf("FlushTimeout = %v, want 10s", cfg.FlushTimeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.FailFast {
		t.Errorf("FailFast = true, want false")
	}
	if len(cfg.Scenarios) != 0 {
		t.Errorf("Scenarios = %v, want empty", cfg.Scenarios)
	}
}

func TestParseFlagsCustomValues(t *testing.T) {
	args := []string{
		"--devs=4",
		"--agents=8",
		"--http-url=http://edge:8080",
		"--ssh-addr=edge:2222",
		"--ssh-key=/tmp/id_ed25519",
		"--auth-token=secret-token",
		"--admin-url=http://ctrl:8090",
		"--admin-token=admin-secret",
		"--tenant=acme",
		"--repos=alpha, beta ,gamma",
		"--seed=42",
		"--duration=2m",
		"--think-min=10ms",
		"--think-max=100ms",
		"--otel-endpoint=",
		"--flush-timeout=5s",
		"--scenarios=dev-push, agent-clone",
		"--max-retries=5",
		"--fail-fast",
	}

	var stderr bytes.Buffer
	cfg, err := ParseFlags(args, &stderr)
	if err != nil {
		t.Fatalf("ParseFlags unexpected error: %v", err)
	}

	if cfg.Devs != 4 || cfg.Agents != 8 {
		t.Errorf("Devs/Agents = %d/%d, want 4/8", cfg.Devs, cfg.Agents)
	}
	if cfg.HTTPURL != "http://edge:8080" || cfg.SSHAddr != "edge:2222" {
		t.Errorf("endpoints = %q / %q", cfg.HTTPURL, cfg.SSHAddr)
	}
	if cfg.SSHKey != "/tmp/id_ed25519" || cfg.AuthToken != "secret-token" {
		t.Errorf("auth = %q / %q", cfg.SSHKey, cfg.AuthToken)
	}
	if cfg.AdminURL != "http://ctrl:8090" || cfg.AdminToken != "admin-secret" {
		t.Errorf("admin = %q / %q", cfg.AdminURL, cfg.AdminToken)
	}
	if cfg.Tenant != "acme" || cfg.Seed != 42 || cfg.Duration != 2*time.Minute {
		t.Errorf("tenant/seed/duration = %q/%d/%v", cfg.Tenant, cfg.Seed, cfg.Duration)
	}
	if cfg.OtelEndpoint != "" || cfg.FlushTimeout != 5*time.Second {
		t.Errorf("otel/flush = %q/%v", cfg.OtelEndpoint, cfg.FlushTimeout)
	}
	if cfg.MaxRetries != 5 || !cfg.FailFast {
		t.Errorf("retries/failfast = %d/%v", cfg.MaxRetries, cfg.FailFast)
	}
	if len(cfg.Repos) != 3 || cfg.Repos[1] != "beta" {
		t.Errorf("Repos = %v, want [alpha beta gamma]", cfg.Repos)
	}
	if len(cfg.Scenarios) != 2 || cfg.Scenarios[0] != "dev-push" || cfg.Scenarios[1] != "agent-clone" {
		t.Errorf("Scenarios = %v, want [dev-push agent-clone]", cfg.Scenarios)
	}
}

func TestConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "zero workers", mutate: func(c *Config) { c.Devs = 0; c.Agents = 0 }},
		{name: "negative devs", mutate: func(c *Config) { c.Devs = -1 }},
		{name: "negative agents", mutate: func(c *Config) { c.Agents = -1 }},
		{name: "think max < think min", mutate: func(c *Config) { c.ThinkMin = 200 * time.Millisecond; c.ThinkMax = 100 * time.Millisecond }},
		{name: "negative think min", mutate: func(c *Config) { c.ThinkMin = -1 * time.Millisecond }},
		{name: "negative duration", mutate: func(c *Config) { c.Duration = -1 * time.Second }},
		{name: "zero flush timeout", mutate: func(c *Config) { c.FlushTimeout = 0 }},
		{name: "negative max retries", mutate: func(c *Config) { c.MaxRetries = -1 }},
		{name: "empty repos", mutate: func(c *Config) { c.Repos = nil }},
		{name: "empty tenant", mutate: func(c *Config) { c.Tenant = "" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestConfigRepoURLs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTPURL = "http://127.0.0.1:8080/"
	cfg.SSHAddr = "127.0.0.1:2222"
	cfg.Tenant = "loadgen"

	if got := cfg.RepoHTTPURL("repo-1"); got != "http://127.0.0.1:8080/loadgen/repo-1.git" {
		t.Errorf("RepoHTTPURL = %q, want http://127.0.0.1:8080/loadgen/repo-1.git", got)
	}
	if got := cfg.RepoSSHURL("repo-1"); got != "ssh://git@127.0.0.1:2222/loadgen/repo-1.git" {
		t.Errorf("RepoSSHURL = %q, want ssh://git@127.0.0.1:2222/loadgen/repo-1.git", got)
	}
}
