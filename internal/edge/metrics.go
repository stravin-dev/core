package edge

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/telemetry"
)

type metricKey struct {
	protocol   string
	operation  string
	statusCode string
	errorClass string
}

// Metrics instruments stravin-edge with bounded-cardinality OTel counters/histograms
// and exposes a lightweight Prometheus text snapshot for /metrics.
type Metrics struct {
	tracer   trace.Tracer
	reqTotal metric.Int64Counter
	reqDur   metric.Float64Histogram

	mu     sync.RWMutex
	counts map[metricKey]int64
}

// NewMetrics creates edge tracing and bounded-cardinality metrics backed by provider.
// If provider is nil, a muted no-op provider is used.
func NewMetrics(provider *telemetry.Provider) (*Metrics, error) {
	if provider == nil {
		provider = telemetry.NewNoopProvider()
	}
	meter := provider.Meter("stravin.dev/core/internal/edge")
	reqTotal, err := meter.Int64Counter(
		"stravin_edge_requests_total",
		metric.WithDescription("Total Git ingress requests handled by stravin-edge."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_requests_total: %w", err)
	}
	reqDur, err := meter.Float64Histogram(
		"stravin_edge_request_duration_seconds",
		metric.WithDescription("Duration of Git ingress requests in seconds."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_request_duration_seconds: %w", err)
	}
	return &Metrics{
		tracer:   provider.Tracer("stravin.dev/core/internal/edge"),
		reqTotal: reqTotal,
		reqDur:   reqDur,
		counts:   make(map[metricKey]int64),
	}, nil
}

// StartIngressSpan opens the root edge.ingress span with standardized Stravin attributes.
func (m *Metrics) StartIngressSpan(ctx context.Context, protocol, operation, tenantID, repoID, homeRegion string) (context.Context, trace.Span) {
	attrs := telemetry.NewSpanAttrs().
		GitProtocol(protocol).
		GitOperation(operation).
		TenantID(tenantID).
		RepoID(repoID).
		HomeRegion(homeRegion).
		Build()
	return m.tracer.Start(ctx, "edge.ingress", trace.WithAttributes(attrs...))
}

// RecordRequest records bounded-cardinality request count and duration metrics.
func (m *Metrics) RecordRequest(ctx context.Context, protocol, operation, statusCode, errorClass string, d time.Duration) {
	countAttrs := []attribute.KeyValue{
		telemetry.AttrGitProtocol.String(protocol),
		telemetry.AttrGitOperation.String(operation),
		telemetry.AttrStatusCode.String(statusCode),
		telemetry.AttrErrorClass.String(errorClass),
	}
	durAttrs := []attribute.KeyValue{
		telemetry.AttrGitProtocol.String(protocol),
		telemetry.AttrGitOperation.String(operation),
		telemetry.AttrStatusCode.String(statusCode),
	}
	m.reqTotal.Add(ctx, 1, metric.WithAttributes(countAttrs...))
	m.reqDur.Record(ctx, d.Seconds(), metric.WithAttributes(durAttrs...))

	key := metricKey{
		protocol:   protocol,
		operation:  operation,
		statusCode: statusCode,
		errorClass: errorClass,
	}
	m.mu.Lock()
	m.counts[key]++
	m.mu.Unlock()
}

// ServeHTTP renders bounded-cardinality edge metrics in Prometheus text exposition format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	keys := make([]metricKey, 0, len(m.counts))
	for k := range m.counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].protocol != keys[j].protocol {
			return keys[i].protocol < keys[j].protocol
		}
		if keys[i].operation != keys[j].operation {
			return keys[i].operation < keys[j].operation
		}
		if keys[i].statusCode != keys[j].statusCode {
			return keys[i].statusCode < keys[j].statusCode
		}
		return keys[i].errorClass < keys[j].errorClass
	})

	var b strings.Builder
	b.WriteString("# HELP stravin_edge_requests_total Total Git ingress requests handled by stravin-edge.\n")
	b.WriteString("# TYPE stravin_edge_requests_total counter\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "stravin_edge_requests_total{git_protocol=%q,git_operation=%q,status_code=%q,error_class=%q} %d\n",
			k.protocol, k.operation, k.statusCode, k.errorClass, m.counts[k])
	}
	m.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}
