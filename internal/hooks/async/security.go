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

// ValidateTargetURL checks that rawURL uses http or https, has a non-empty host,
// and does not target known cloud metadata endpoints.
func ValidateTargetURL(rawURL string) (*url.URL, error) {
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

	if ip := net.ParseIP(host); ip != nil && isBlockedIP(ip) {
		return nil, fmt.Errorf("%w: %s", ErrBlockedTargetHost, ip.String())
	}

	return parsed, nil
}

func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	for _, blocked := range blockedMetadataIPs {
		if blocked != nil && ip.Equal(blocked) {
			return true
		}
	}
	return false
}

// NewSafeHTTPClient builds an HTTP client that blocks cloud metadata IPs at both
// URL-validation and dial time (mitigating DNS rebinding), validates redirects,
// and enforces per-request timeouts.
func NewSafeHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          64,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrInvalidTargetURL, err)
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, ipAddr := range ips {
				if isBlockedIP(ipAddr.IP) {
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
			if _, err := ValidateTargetURL(req.URL.String()); err != nil {
				return err
			}
			return nil
		},
	}
}
