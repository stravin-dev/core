package async

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestValidateTargetURL(t *testing.T) {
	valid := []string{
		"https://hooks.example.com/stravin",
		"HTTPS://CI.EXAMPLE.ORG/events",
		"https://93.184.216.34/webhook",
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
		// Loopback
		{"http://127.0.0.1:8080/webhook", ErrBlockedTargetHost},
		{"http://localhost:9000/cb", ErrBlockedTargetHost},
		{"http://sub.localhost:9000/cb", ErrBlockedTargetHost},
		{"http://[::1]:8080/webhook", ErrBlockedTargetHost},
		{"http://[::ffff:127.0.0.1]:8080/webhook", ErrBlockedTargetHost},
		// RFC1918 & IPv6 ULA
		{"http://10.0.0.5/hook", ErrBlockedTargetHost},
		{"http://172.16.0.1/hook", ErrBlockedTargetHost},
		{"http://192.168.1.1/hook", ErrBlockedTargetHost},
		{"http://[::ffff:10.0.0.5]/hook", ErrBlockedTargetHost},
		{"http://[fc00::1]/hook", ErrBlockedTargetHost},
		{"http://[fd12:3456::1]/hook", ErrBlockedTargetHost},
		// Link-local, CGNAT, multicast, unspecified
		{"http://169.254.1.1/hook", ErrBlockedTargetHost},
		{"http://[fe80::1]/hook", ErrBlockedTargetHost},
		{"http://100.64.0.1/hook", ErrBlockedTargetHost},
		{"http://224.0.0.1/hook", ErrBlockedTargetHost},
		{"http://0.0.0.0/hook", ErrBlockedTargetHost},
		{"http://[::]/hook", ErrBlockedTargetHost},
		// Cloud metadata
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

func TestValidateTargetURLWithOptions_AllowPrivateNetworks(t *testing.T) {
	opts := SafeClientOptions{AllowPrivateNetworks: true}

	allowed := []string{
		"http://127.0.0.1:8080/webhook",
		"http://localhost:9000/cb",
		"http://[::1]:8080/webhook",
		"http://10.0.0.5/hook",
		"http://172.16.0.1/hook",
		"http://192.168.1.1/hook",
		"http://[fd12:3456::1]/hook",
		"https://hooks.example.com/stravin",
	}
	for _, raw := range allowed {
		if _, err := ValidateTargetURLWithOptions(raw, opts); err != nil {
			t.Errorf("ValidateTargetURLWithOptions(%q, allowPrivate) unexpected error: %v", raw, err)
		}
	}

	// Cloud metadata, link-local, CGNAT, multicast, and unspecified remain blocked even in opt-in mode.
	stillBlocked := []string{
		"http://169.254.169.254/latest/meta-data/",
		"http://169.254.170.2/v2/credentials",
		"http://[fd00:ec2::254]/latest/meta-data/",
		"http://metadata.google.internal/computeMetadata/v1/",
		"http://100.64.0.1/hook",
		"http://169.254.1.1/hook",
		"http://0.0.0.0/hook",
	}
	for _, raw := range stillBlocked {
		_, err := ValidateTargetURLWithOptions(raw, opts)
		if !errors.Is(err, ErrBlockedTargetHost) {
			t.Errorf("ValidateTargetURLWithOptions(%q, allowPrivate) err = %v, want %v", raw, err, ErrBlockedTargetHost)
		}
	}
}

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		// Loopback (IPv4, IPv6, IPv4-mapped IPv6)
		"127.0.0.1",
		"127.255.255.254",
		"::1",
		"::ffff:127.0.0.1",
		// RFC1918 private (IPv4 and IPv4-mapped IPv6)
		"10.0.0.1",
		"10.255.255.254",
		"172.16.0.1",
		"172.31.255.254",
		"192.168.0.1",
		"192.168.255.254",
		"::ffff:10.0.0.1",
		"::ffff:172.16.0.1",
		"::ffff:192.168.1.1",
		// IPv6 ULA
		"fc00::1",
		"fd00::1",
		"fd12:3456:789a::1",
		// Link-local unicast and multicast
		"169.254.0.1",
		"fe80::1",
		"224.0.0.1",
		"ff02::1",
		// CGNAT 100.64.0.0/10
		"100.64.0.1",
		"100.127.255.254",
		"::ffff:100.64.0.1",
		// Multicast
		"224.0.0.251",
		"239.255.255.250",
		"ff05::1",
		// Unspecified
		"0.0.0.0",
		"::",
		"::ffff:0.0.0.0",
		// Cloud metadata
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

	if !isBlockedIP(nil) {
		t.Error("isBlockedIP(nil) = false, want true")
	}

	allowed := []string{
		"93.184.216.34",
		"1.1.1.1",
		"100.63.255.255",
		"100.128.0.0",
		"2606:2800:220:1:248:1893:25c8:1946",
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("failed to parse IP %q", s)
		}
		if isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%q) = true, want false", s)
		}
	}
}

func TestNewSafeHTTPClient_DisablesProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9999")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9999")

	for _, tc := range []struct {
		name   string
		client *http.Client
	}{
		{"default", NewSafeHTTPClient(5 * time.Second)},
		{"allow-private", NewSafeHTTPClientWithOptions(5*time.Second, SafeClientOptions{AllowPrivateNetworks: true})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr, ok := tc.client.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("expected *http.Transport, got %T", tc.client.Transport)
			}
			if tr.Proxy != nil {
				t.Fatalf("expected http.Transport.Proxy to be nil so HTTP_PROXY cannot bypass DialContext")
			}
		})
	}
}

func TestNewSafeHTTPClient_LoopbackAndRedirectEnforcement(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Default fail-closed client blocks loopback at dial time.
	strictClient := NewSafeHTTPClient(5 * time.Second)
	resp, err := strictClient.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected strict client to block loopback %s", srv.URL)
	}
	if !errors.Is(err, ErrBlockedTargetHost) {
		t.Fatalf("strict client err = %v, want %v", err, ErrBlockedTargetHost)
	}

	// CheckRedirect on strict client blocks 30x redirects to loopback/private/metadata targets.
	for _, redirectTarget := range []string{
		"http://127.0.0.1:8080/internal",
		"http://10.0.0.1/internal",
		"http://169.254.169.254/latest/meta-data/",
	} {
		u, _ := url.Parse(redirectTarget)
		err := strictClient.CheckRedirect(&http.Request{URL: u}, []*http.Request{{}})
		if !errors.Is(err, ErrBlockedTargetHost) {
			t.Errorf("CheckRedirect(%q) err = %v, want %v", redirectTarget, err, ErrBlockedTargetHost)
		}
	}

	// Opt-in client permits loopback when explicitly enabled.
	allowPrivateClient := NewSafeHTTPClientWithOptions(5*time.Second, SafeClientOptions{
		AllowPrivateNetworks: true,
	})
	resp, err = allowPrivateClient.Get(srv.URL)
	if err != nil {
		t.Fatalf("allowPrivate client unexpected error against loopback: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Redirect to cloud metadata is still blocked even when AllowPrivateNetworks is true.
	redirSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer redirSrv.Close()

	resp, err = allowPrivateClient.Get(redirSrv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected redirect to metadata IP to be blocked")
	}
	if !errors.Is(err, ErrBlockedTargetHost) {
		t.Fatalf("redirect to metadata err = %v, want %v", err, ErrBlockedTargetHost)
	}
}
