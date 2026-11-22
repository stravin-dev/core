package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	defaultHTTPTimeout = 10 * time.Second
	maxErrorBodyBytes  = 4096
)

// Proxy represents a Toxiproxy proxy entry returned by the admin API.
type Proxy struct {
	Name     string  `json:"name"`
	Listen   string  `json:"listen"`
	Upstream string  `json:"upstream"`
	Enabled  bool    `json:"enabled"`
	Toxics   []Toxic `json:"toxics,omitempty"`
}

// Toxic represents a single Toxiproxy fault rule attached to a proxy.
type Toxic struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Stream     string         `json:"stream"`
	Toxicity   float64        `json:"toxicity"`
	Attributes map[string]any `json:"attributes"`
}

// ProxyController abstracts Toxiproxy admin operations for dependency inversion and testing.
type ProxyController interface {
	ListProxies(ctx context.Context) ([]Proxy, error)
	AddToxic(ctx context.Context, proxyName string, toxic Toxic) (Toxic, error)
	RemoveToxic(ctx context.Context, proxyName, toxicName string) error
	ResetAll(ctx context.Context) error
}

// ToxiproxyClient talks to the Toxiproxy HTTP admin API using Go stdlib net/http.
type ToxiproxyClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewToxiproxyClient creates a ToxiproxyClient pointing at baseURL (e.g. http://127.0.0.1:8474).
func NewToxiproxyClient(baseURL string, httpClient *http.Client) *ToxiproxyClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &ToxiproxyClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
	}
}

// ListProxies retrieves all configured proxies sorted by name for deterministic selection.
func (c *ToxiproxyClient) ListProxies(ctx context.Context) ([]Proxy, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/proxies", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("chaos: build list proxies request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chaos: list proxies: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, unexpectedStatusError("list proxies", resp)
	}

	var byName map[string]Proxy
	if err := json.NewDecoder(resp.Body).Decode(&byName); err != nil {
		return nil, fmt.Errorf("chaos: decode proxies response: %w", err)
	}

	proxies := make([]Proxy, 0, len(byName))
	for _, p := range byName {
		proxies = append(proxies, p)
	}
	sort.Slice(proxies, func(i, j int) bool {
		return proxies[i].Name < proxies[j].Name
	})
	return proxies, nil
}

// AddToxic attaches toxic to proxyName and returns the created Toxic.
func (c *ToxiproxyClient) AddToxic(ctx context.Context, proxyName string, toxic Toxic) (Toxic, error) {
	if proxyName == "" {
		return Toxic{}, errors.New("chaos: proxy name is required")
	}

	payload, err := json.Marshal(toxic)
	if err != nil {
		return Toxic{}, fmt.Errorf("chaos: marshal toxic: %w", err)
	}

	endpoint := fmt.Sprintf("%s/proxies/%s/toxics", c.baseURL, url.PathEscape(proxyName))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return Toxic{}, fmt.Errorf("chaos: build add toxic request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Toxic{}, fmt.Errorf("chaos: add toxic %q to %q: %w", toxic.Name, proxyName, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return Toxic{}, unexpectedStatusError("add toxic", resp)
	}

	var created Toxic
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return Toxic{}, fmt.Errorf("chaos: decode add toxic response: %w", err)
	}
	return created, nil
}

// RemoveToxic removes toxicName from proxyName.
func (c *ToxiproxyClient) RemoveToxic(ctx context.Context, proxyName, toxicName string) error {
	if proxyName == "" || toxicName == "" {
		return errors.New("chaos: proxy name and toxic name are required")
	}

	endpoint := fmt.Sprintf(
		"%s/proxies/%s/toxics/%s",
		c.baseURL,
		url.PathEscape(proxyName),
		url.PathEscape(toxicName),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, http.NoBody)
	if err != nil {
		return fmt.Errorf("chaos: build remove toxic request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("chaos: remove toxic %q from %q: %w", toxicName, proxyName, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return unexpectedStatusError("remove toxic", resp)
	}
	return nil
}

// ResetAll removes every active toxic across all proxies on the Toxiproxy server.
func (c *ToxiproxyClient) ResetAll(ctx context.Context) error {
	proxies, err := c.ListProxies(ctx)
	if err != nil {
		return fmt.Errorf("chaos: reset all list proxies: %w", err)
	}

	var errs []error
	for _, proxy := range proxies {
		for _, toxic := range proxy.Toxics {
			if err := c.RemoveToxic(ctx, proxy.Name, toxic.Name); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func unexpectedStatusError(op string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	return fmt.Errorf("chaos: %s: status %d: %s", op, resp.StatusCode, strings.TrimSpace(string(body)))
}
