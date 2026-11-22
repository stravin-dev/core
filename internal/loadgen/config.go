// Package loadgen implements the standalone Stravin load generator engine,
// pluggable persona/scenario registry, client-side integrity oracle, and
// OpenTelemetry traces/metrics/logs instrumentation.
package loadgen

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

// Config holds CLI and runtime configuration for stravin-loadgen.
type Config struct {
	Devs         int
	Agents       int
	HTTPURL      string
	SSHAddr      string
	SSHKey       string
	AuthToken    string
	AdminURL     string
	AdminToken   string
	Tenant       string
	Repos        []string
	Seed         int64
	Duration     time.Duration
	ThinkMin     time.Duration
	ThinkMax     time.Duration
	OtelEndpoint string
	FlushTimeout time.Duration
	Scenarios    []string
	MaxRetries   int
	FailFast     bool
}

// DefaultConfig returns the standard stravin-loadgen defaults.
func DefaultConfig() Config {
	return Config{
		Devs:         10,
		Agents:       20,
		HTTPURL:      "http://127.0.0.1:8080",
		SSHAddr:      "127.0.0.1:2222",
		Tenant:       "loadgen",
		Repos:        []string{"repo-1", "repo-2", "repo-3"},
		ThinkMin:     50 * time.Millisecond,
		ThinkMax:     500 * time.Millisecond,
		OtelEndpoint: "http://127.0.0.1:4317",
		FlushTimeout: 10 * time.Second,
		MaxRetries:   3,
	}
}

// ParseFlags parses CLI arguments into a validated Config.
func ParseFlags(args []string, stderr io.Writer) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("stravin-loadgen", flag.ContinueOnError)
	if stderr != nil {
		fs.SetOutput(stderr)
	}

	var reposCSV string
	var scenariosCSV string

	fs.IntVar(&cfg.Devs, "devs", cfg.Devs, "number of concurrent developer personas")
	fs.IntVar(&cfg.Agents, "agents", cfg.Agents, "number of concurrent AI agent personas")
	fs.StringVar(&cfg.HTTPURL, "http-url", cfg.HTTPURL, "Stravin Edge Git HTTP base URL")
	fs.StringVar(&cfg.SSHAddr, "ssh-addr", cfg.SSHAddr, "Stravin Edge Git SSH host:port")
	fs.StringVar(&cfg.SSHKey, "ssh-key", "", "optional SSH private key path")
	fs.StringVar(&cfg.AuthToken, "auth-token", "", "Bearer or Basic (user:pass) token for Git HTTP auth")
	fs.StringVar(&cfg.AdminURL, "admin-url", "", "optional Stravin Control Plane admin API URL")
	fs.StringVar(&cfg.AdminToken, "admin-token", "", "optional bearer token for Control Plane admin API")
	fs.StringVar(&cfg.Tenant, "tenant", cfg.Tenant, "Stravin tenant identifier")
	fs.StringVar(&reposCSV, "repos", strings.Join(cfg.Repos, ","), "comma-separated repository names")
	fs.Int64Var(&cfg.Seed, "seed", 0, "PRNG seed (0 uses current time)")
	fs.DurationVar(&cfg.Duration, "duration", 0, "run duration (0 runs continuously until Ctrl+C)")
	fs.DurationVar(&cfg.ThinkMin, "think-min", cfg.ThinkMin, "minimum randomized think time between scenarios")
	fs.DurationVar(&cfg.ThinkMax, "think-max", cfg.ThinkMax, "maximum randomized think time between scenarios")
	fs.StringVar(&cfg.OtelEndpoint, "otel-endpoint", cfg.OtelEndpoint, "OTLP gRPC endpoint (empty disables OTLP export)")
	fs.DurationVar(&cfg.FlushTimeout, "flush-timeout", cfg.FlushTimeout, "timeout for flushing OTel providers on shutdown")
	fs.StringVar(&scenariosCSV, "scenarios", "", "comma-separated scenario filter (empty runs all registered)")
	fs.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "maximum retries for retryable conflict/transient errors")
	fs.BoolVar(&cfg.FailFast, "fail-fast", false, "stop immediately on first unexpected error or fsck failure")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg.Repos = splitCSV(reposCSV)
	cfg.Scenarios = splitCSV(scenariosCSV)
	cfg.HTTPURL = strings.TrimSpace(cfg.HTTPURL)
	cfg.SSHAddr = strings.TrimSpace(cfg.SSHAddr)
	cfg.Tenant = strings.TrimSpace(cfg.Tenant)
	cfg.OtelEndpoint = strings.TrimSpace(cfg.OtelEndpoint)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks configuration invariants.
func (c Config) Validate() error {
	if c.Devs < 0 || c.Agents < 0 {
		return errors.New("loadgen: --devs and --agents must be non-negative")
	}
	if c.Devs+c.Agents == 0 {
		return errors.New("loadgen: at least one worker is required (--devs + --agents > 0)")
	}
	if c.Tenant == "" {
		return errors.New("loadgen: --tenant must be non-empty")
	}
	if len(c.Repos) == 0 {
		return errors.New("loadgen: --repos must include at least one repository")
	}
	if c.Duration < 0 {
		return errors.New("loadgen: --duration must be non-negative")
	}
	if c.ThinkMin < 0 || c.ThinkMax < 0 {
		return errors.New("loadgen: --think-min and --think-max must be non-negative")
	}
	if c.ThinkMax < c.ThinkMin {
		return fmt.Errorf("loadgen: --think-max (%s) must be >= --think-min (%s)", c.ThinkMax, c.ThinkMin)
	}
	if c.FlushTimeout <= 0 {
		return errors.New("loadgen: --flush-timeout must be positive")
	}
	if c.MaxRetries < 0 {
		return errors.New("loadgen: --max-retries must be non-negative")
	}
	return nil
}

// EffectiveSeed returns Seed if non-zero, otherwise a time-based seed.
func (c Config) EffectiveSeed() int64 {
	if c.Seed != 0 {
		return c.Seed
	}
	return time.Now().UnixNano()
}

// RepoHTTPURL constructs the Stravin Smart HTTP repository URL for repo.
func (c Config) RepoHTTPURL(repo string) string {
	base := strings.TrimRight(c.HTTPURL, "/")
	return fmt.Sprintf("%s/%s/%s.git", base, c.Tenant, strings.TrimSuffix(repo, ".git"))
}

// RepoSSHURL constructs the Stravin Git SSH repository URL for repo.
func (c Config) RepoSSHURL(repo string) string {
	addr := strings.TrimPrefix(c.SSHAddr, "ssh://")
	addr = strings.TrimRight(addr, "/")
	return fmt.Sprintf("ssh://git@%s/%s/%s.git", addr, c.Tenant, strings.TrimSuffix(repo, ".git"))
}

func splitCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
