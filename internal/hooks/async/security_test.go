package async

import (
	"errors"
	"net"
	"testing"
)

func TestValidateTargetURL(t *testing.T) {
	valid := []string{
		"https://hooks.example.com/stravin",
		"http://127.0.0.1:8080/webhook",
		"http://localhost:9000/cb",
		"HTTPS://CI.EXAMPLE.ORG/events",
	}
	for _, raw := range valid {
		if _, err := ValidateTargetURL(raw); err != nil {
			t.Errorf("ValidateTargetURL(%q) unexpected error: %v", raw, err)
		}
	}

	invalid := []struct {
		url     string
		wantErr error
	}{
		{"", ErrInvalidTargetURL},
		{"ftp://example.com/hook", ErrInvalidTargetURL},
		{"file:///etc/passwd", ErrInvalidTargetURL},
		{"javascript:alert(1)", ErrInvalidTargetURL},
		{"https://", ErrInvalidTargetURL},
		{"http://169.254.169.254/latest/meta-data/", ErrBlockedTargetHost},
		{"https://169.254.169.254/latest/api/token", ErrBlockedTargetHost},
		{"http://[fd00:ec2::254]/latest/meta-data/", ErrBlockedTargetHost},
		{"http://169.254.170.2/v2/credentials", ErrBlockedTargetHost},
		{"http://metadata.google.internal/computeMetadata/v1/", ErrBlockedTargetHost},
		{"http://metadata.goog/computeMetadata/v1/", ErrBlockedTargetHost},
	}
	for _, tc := range invalid {
		_, err := ValidateTargetURL(tc.url)
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("ValidateTargetURL(%q) err = %v, want %v", tc.url, err, tc.wantErr)
		}
	}
}

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"169.254.169.254",
		"169.254.170.2",
		"fd00:ec2::254",
		"::ffff:169.254.169.254",
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("failed to parse IP %q", s)
		}
		if !isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%q) = false, want true", s)
		}
	}

	allowed := []string{
		"127.0.0.1",
		"::1",
		"93.184.216.34",
		"2606:2800:220:1:248:1893:25c8:1946",
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%q) = true, want false", s)
		}
	}
}
