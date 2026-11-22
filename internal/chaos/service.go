package chaos

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// ServiceController restarts a target service in the chaos stack.
type ServiceController interface {
	Restart(ctx context.Context, service string) error
}

// ServiceRestarter is an alias for ServiceController.
type ServiceRestarter = ServiceController

var protectedServices = map[string]struct{}{
	"floci":      {},
	"floci-init": {},
	"toxiproxy":  {},
}

// IsProtectedService reports whether service must never be restarted by chaos automation.
func IsProtectedService(service string) bool {
	_, ok := protectedServices[strings.ToLower(strings.TrimSpace(service))]
	return ok
}

// GuardedServiceController (GoF Proxy) enforces an explicit allowlist and protected-service
// denylist before delegating restarts to the underlying ServiceController.
type GuardedServiceController struct {
	inner   ServiceController
	allowed map[string]struct{}
}

// NewGuardedServiceController wraps inner so only allowedTargets (and never protected services)
// can be restarted.
func NewGuardedServiceController(inner ServiceController, allowedTargets []string) *GuardedServiceController {
	allowed := make(map[string]struct{}, len(allowedTargets))
	for _, target := range allowedTargets {
		trimmed := strings.TrimSpace(target)
		if trimmed == "" || IsProtectedService(trimmed) || !isSafeServiceName(trimmed) {
			continue
		}
		allowed[trimmed] = struct{}{}
	}
	return &GuardedServiceController{
		inner:   inner,
		allowed: allowed,
	}
}

// Restart validates service against protected and allowed sets before delegating.
func (g *GuardedServiceController) Restart(ctx context.Context, service string) error {
	trimmed := strings.TrimSpace(service)
	if trimmed == "" {
		return errors.New("chaos: service name is required")
	}
	if !isSafeServiceName(trimmed) {
		return fmt.Errorf("chaos: invalid service name %q", trimmed)
	}
	if IsProtectedService(trimmed) {
		return fmt.Errorf("chaos: refusing to restart protected service %q", trimmed)
	}
	if _, ok := g.allowed[trimmed]; !ok {
		return fmt.Errorf("chaos: service %q is not in allowed restart targets", trimmed)
	}
	if g.inner == nil {
		return errors.New("chaos: nil underlying service controller")
	}
	return g.inner.Restart(ctx, trimmed)
}

// ComposeServiceController restarts Docker Compose services via `docker compose`.
type ComposeServiceController struct {
	composeFile string
}

// NewComposeServiceController returns a ComposeServiceController targeting composeFile.
func NewComposeServiceController(composeFile string) *ComposeServiceController {
	return &ComposeServiceController{composeFile: composeFile}
}

// Restart runs `docker compose -f <composeFile> restart <service>`.
func (c *ComposeServiceController) Restart(ctx context.Context, service string) error {
	if !isSafeServiceName(service) || IsProtectedService(service) {
		return fmt.Errorf("chaos: refusing unsafe restart target %q", service)
	}
	if strings.TrimSpace(c.composeFile) == "" {
		return errors.New("chaos: compose file is required")
	}
	// #nosec G204 -- service name is validated by isSafeServiceName and composeFile is operator-configured.
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", c.composeFile, "restart", service)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("chaos: restart %q: %w: %s", service, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func isSafeServiceName(name string) bool {
	if name == "" || strings.HasPrefix(name, "-") {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		valid := (ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '_' || ch == '.'
		if !valid {
			return false
		}
	}
	return true
}
