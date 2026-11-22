package async

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// MaxResponseBodyBytes caps how many bytes are read from a webhook HTTP response.
	MaxResponseBodyBytes = 64 * 1024

	defaultDialTimeout = 5 * time.Second
)

var (
	ErrInvalidTargetURL  = errors.New("async: invalid webhook target URL")
	ErrBlockedTargetHost = errors.New("async: blocked webhook target host")
)

var blockedMetadataHosts = map[string]struct{}{
	"metadata.google.internal": {},
	"metadata.goog":            {},
	"metadata":                 {},
}

var blockedMetadataIPs = []net.IP{
	net.ParseIP("169.254.169.254"),
	net.ParseIP("169.254.170.2"),
	net.ParseIP("fd00:ec2::254"),
}

var cgnatCIDR = mustParseCIDR("100.64.0.0/10")

func mustParseCIDR(cidr string) *net.IPNet {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(fmt.Sprintf("async: invalid CIDR %q: %v", cidr, err))
	}
	return network
}

// SafeClientOptions configures SSRF validation for webhook target URLs and HTTP clients.
// Zero value is fail-closed (blocks loopback, RFC1918, ULA, link-local, CGNAT, multicast,
// unspecified, and cloud metadata addresses).
type SafeClientOptions struct {
	// AllowPrivateNetworks permits loopback and private (RFC1918 / IPv6 ULA) targets.
	// Cloud metadata endpoints, link-local, CGNAT, multicast, and unspecified addresses
	// remain blocked even when enabled. Intended only for local development and E2E tests.
	AllowPrivateNetworks bool
}

// ValidateTargetURL checks that rawURL uses http or https, has a non-empty host,
// and does not target blocked private, loopback, link-local, CGNAT, or cloud metadata endpoints.
func ValidateTargetURL(rawURL string) (*url.URL, error) {
	return ValidateTargetURLWithOptions(rawURL, SafeClientOptions{})
}

// ValidateTargetURLWithOptions validates rawURL under the provided SSRF policy.
func ValidateTargetURLWithOptions(rawURL string, opts SafeClientOptions) (*url.URL, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty url", ErrInvalidTargetURL)
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTargetURL, err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("%w: unsupported scheme %q", ErrInvalidTargetURL, parsed.Scheme)
	}

	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "" {
		return nil, fmt.Errorf("%w: empty host", ErrInvalidTargetURL)
	}

	if _, blocked := blockedMetadataHosts[host]; blocked {
		return nil, fmt.Errorf("%w: %s", ErrBlockedTargetHost, host)
	}

	if !opts.AllowPrivateNetworks && isLocalhostName(host) {
		return nil, fmt.Errorf("%w: %s", ErrBlockedTargetHost, host)
	}

	if ip := net.ParseIP(host); ip != nil && isBlockedIPWithPolicy(ip, opts.AllowPrivateNetworks) {
		return nil, fmt.Errorf("%w: %s", ErrBlockedTargetHost, ip.String())
	}

	return parsed, nil
}

func isLocalhostName(host string) bool {
	return host == "localhost" || strings.HasSuffix(host, ".localhost")
}

func isBlockedIP(ip net.IP) bool {
	return isBlockedIPWithPolicy(ip, false)
}

func isBlockedIPWithPolicy(ip net.IP, allowPrivate bool) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	for _, blocked := range blockedMetadataIPs {
		if blocked != nil && ip.Equal(blocked) {
			return true
		}
	}
	if ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified() ||
		cgnatCIDR.Contains(ip) {
		return true
	}
	if !allowPrivate && (ip.IsLoopback() || ip.IsPrivate()) {
		return true
	}
	return false
}

// NewSafeHTTPClient builds a fail-closed HTTP client that blocks loopback, private,
// link-local, CGNAT, multicast, unspecified, and cloud metadata IPs at both
// URL-validation and dial time (mitigating DNS rebinding), disables proxy env vars,
// validates redirects, and enforces per-request timeouts.
func NewSafeHTTPClient(timeout time.Duration) *http.Client {
	return NewSafeHTTPClientWithOptions(timeout, SafeClientOptions{})
}

// NewSafeHTTPClientWithOptions builds an SSRF-hardened HTTP client configured by opts.
func NewSafeHTTPClientWithOptions(timeout time.Duration, opts SafeClientOptions) *http.Client {
	dialer := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		Proxy:                 nil, // Never allow HTTP_PROXY/HTTPS_PROXY to bypass DialContext IP checks.
		MaxIdleConns:          64,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrInvalidTargetURL, err)
			}
			normalizedHost := strings.ToLower(strings.TrimSuffix(host, "."))
			if _, blocked := blockedMetadataHosts[normalizedHost]; blocked {
				return nil, fmt.Errorf("%w: %s", ErrBlockedTargetHost, normalizedHost)
			}
			if !opts.AllowPrivateNetworks && isLocalhostName(normalizedHost) {
				return nil, fmt.Errorf("%w: %s", ErrBlockedTargetHost, normalizedHost)
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, ipAddr := range ips {
				if isBlockedIPWithPolicy(ipAddr.IP, opts.AllowPrivateNetworks) {
					return nil, fmt.Errorf("%w: resolved to %s", ErrBlockedTargetHost, ipAddr.IP.String())
				}
			}
			var lastErr error
			for _, ipAddr := range ips {
				conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ipAddr.IP.String(), port))
				if dialErr == nil {
					return conn, nil
				}
				lastErr = dialErr
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("async: no addresses resolved for %s", host)
			}
			return nil, lastErr
		},
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("async: stopped after 5 redirects")
			}
			if _, err := ValidateTargetURLWithOptions(req.URL.String(), opts); err != nil {
				return err
			}
			return nil
		},
	}
}
