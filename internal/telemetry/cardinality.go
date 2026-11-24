package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
)

// ErrHighCardinalityLabel is returned when an unbounded label is attached to a Prometheus metric.
var ErrHighCardinalityLabel = errors.New("telemetry: high-cardinality label forbidden on metrics")

// forbiddenMetricLabels lists unbounded identifiers that belong only on traces
// and logs, never on Prometheus metric dimensions.
var forbiddenMetricLabels = map[string]struct{}{
	string(AttrRepoID):        {},
	string(AttrRefName):       {},
	string(AttrPackID):        {},
	string(AttrOID):           {},
	string(AttrActorID):       {},
	string(AttrTenantID):      {},
	string(AttrHookID):        {},
	string(AttrFailoverEpoch): {},
}

// IsForbiddenMetricLabel reports whether key is an unbounded label disallowed on metrics.
func IsForbiddenMetricLabel(key string) bool {
	_, forbidden := forbiddenMetricLabels[key]
	return forbidden
}

// ValidateMetricAttributes returns an error wrapping ErrHighCardinalityLabel if any
// attribute key is forbidden on Prometheus metrics.
func ValidateMetricAttributes(attrs ...attribute.KeyValue) error {
	var bad []string
	for _, kv := range attrs {
		if IsForbiddenMetricLabel(string(kv.Key)) {
			bad = append(bad, string(kv.Key))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%w: %s", ErrHighCardinalityLabel, strings.Join(bad, ", "))
	}
	return nil
}

// SanitizeMetricAttributes removes forbidden high-cardinality labels and returns
// the safe attribute slice alongside the list of dropped label keys.
func SanitizeMetricAttributes(attrs ...attribute.KeyValue) ([]attribute.KeyValue, []string) {
	clean := make([]attribute.KeyValue, 0, len(attrs))
	var dropped []string
	for _, kv := range attrs {
		if IsForbiddenMetricLabel(string(kv.Key)) {
			dropped = append(dropped, string(kv.Key))
			continue
		}
		clean = append(clean, kv)
	}
	return clean, dropped
}

// CardinalityPolicy controls how GuardedMeter handles forbidden metric labels.
type CardinalityPolicy int

const (
	// PolicyStrip removes forbidden labels before recording the metric datapoint.
	PolicyStrip CardinalityPolicy = iota
	// PolicyReject drops the entire metric recording when a forbidden label is present.
	PolicyReject
)

type guardConfig struct {
	policy      CardinalityPolicy
	onViolation func(keys []string)
}

// GuardOption configures a GuardedMeter.
type GuardOption func(*guardConfig)

// WithCardinalityPolicy sets the enforcement mode for a GuardedMeter.
func WithCardinalityPolicy(p CardinalityPolicy) GuardOption {
	return func(c *guardConfig) {
		c.policy = p
	}
}

// WithOnViolation registers a callback invoked whenever forbidden metric labels are detected.
func WithOnViolation(fn func(keys []string)) GuardOption {
	return func(c *guardConfig) {
		c.onViolation = fn
	}
}

// GuardedMeter decorates an OTel metric.Meter (GoF Decorator) to prevent
// unbounded high-cardinality labels from leaking onto Prometheus metrics.
type GuardedMeter struct {
	embedded.Meter
	inner metric.Meter
	cfg   guardConfig
}

// NewGuardedMeter wraps meter with Prometheus cardinality guardrails.
func NewGuardedMeter(meter metric.Meter, opts ...GuardOption) *GuardedMeter {
	cfg := guardConfig{policy: PolicyStrip}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return &GuardedMeter{inner: meter, cfg: cfg}
}

func (g *GuardedMeter) filter(attrs attribute.Set) (attribute.Set, bool) {
	if attrs.Len() == 0 {
		return attrs, true
	}
	clean, dropped := SanitizeMetricAttributes(attrs.ToSlice()...)
	if len(dropped) == 0 {
		return attrs, true
	}
	if g.cfg.onViolation != nil {
		g.cfg.onViolation(dropped)
	}
	if g.cfg.policy == PolicyReject {
		return attribute.Set{}, false
	}
	return attribute.NewSet(clean...), true
}

func (g *GuardedMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	inner, err := g.inner.Int64Counter(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedInt64Counter{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Int64UpDownCounter(name string, options ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	inner, err := g.inner.Int64UpDownCounter(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedInt64UpDownCounter{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	inner, err := g.inner.Int64Histogram(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedInt64Histogram{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	inner, err := g.inner.Int64Gauge(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedInt64Gauge{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Float64Counter(name string, options ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	inner, err := g.inner.Float64Counter(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedFloat64Counter{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Float64UpDownCounter(name string, options ...metric.Float64UpDownCounterOption) (metric.Float64UpDownCounter, error) {
	inner, err := g.inner.Float64UpDownCounter(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedFloat64UpDownCounter{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	inner, err := g.inner.Float64Histogram(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedFloat64Histogram{inner: inner, guard: g}, nil
}

func (g *GuardedMeter) Float64Gauge(name string, options ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	inner, err := g.inner.Float64Gauge(name, options...)
	if err != nil {
		return nil, err
	}
	return &guardedFloat64Gauge{inner: inner, guard: g}, nil
}

type guardedInt64Counter struct {
	embedded.Int64Counter
	inner metric.Int64Counter
	guard *GuardedMeter
}

func (c *guardedInt64Counter) Add(ctx context.Context, incr int64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	set, ok := c.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	c.inner.Add(ctx, incr, metric.WithAttributeSet(set))
}

func (c *guardedInt64Counter) Enabled(ctx context.Context) bool {
	return c.inner.Enabled(ctx)
}

type guardedInt64UpDownCounter struct {
	embedded.Int64UpDownCounter
	inner metric.Int64UpDownCounter
	guard *GuardedMeter
}

func (c *guardedInt64UpDownCounter) Add(ctx context.Context, incr int64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	set, ok := c.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	c.inner.Add(ctx, incr, metric.WithAttributeSet(set))
}

func (c *guardedInt64UpDownCounter) Enabled(ctx context.Context) bool {
	return c.inner.Enabled(ctx)
}

type guardedInt64Histogram struct {
	embedded.Int64Histogram
	inner metric.Int64Histogram
	guard *GuardedMeter
}

func (h *guardedInt64Histogram) Record(ctx context.Context, incr int64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	set, ok := h.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	h.inner.Record(ctx, incr, metric.WithAttributeSet(set))
}

func (h *guardedInt64Histogram) Enabled(ctx context.Context) bool {
	return h.inner.Enabled(ctx)
}

type guardedInt64Gauge struct {
	embedded.Int64Gauge
	inner metric.Int64Gauge
	guard *GuardedMeter
}

func (g *guardedInt64Gauge) Record(ctx context.Context, value int64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	set, ok := g.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	g.inner.Record(ctx, value, metric.WithAttributeSet(set))
}

func (g *guardedInt64Gauge) Enabled(ctx context.Context) bool {
	return g.inner.Enabled(ctx)
}

type guardedFloat64Counter struct {
	embedded.Float64Counter
	inner metric.Float64Counter
	guard *GuardedMeter
}

func (c *guardedFloat64Counter) Add(ctx context.Context, incr float64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	set, ok := c.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	c.inner.Add(ctx, incr, metric.WithAttributeSet(set))
}

func (c *guardedFloat64Counter) Enabled(ctx context.Context) bool {
	return c.inner.Enabled(ctx)
}

type guardedFloat64UpDownCounter struct {
	embedded.Float64UpDownCounter
	inner metric.Float64UpDownCounter
	guard *GuardedMeter
}

func (c *guardedFloat64UpDownCounter) Add(ctx context.Context, incr float64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	set, ok := c.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	c.inner.Add(ctx, incr, metric.WithAttributeSet(set))
}

func (c *guardedFloat64UpDownCounter) Enabled(ctx context.Context) bool {
	return c.inner.Enabled(ctx)
}

type guardedFloat64Histogram struct {
	embedded.Float64Histogram
	inner metric.Float64Histogram
	guard *GuardedMeter
}

func (h *guardedFloat64Histogram) Record(ctx context.Context, incr float64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	set, ok := h.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	h.inner.Record(ctx, incr, metric.WithAttributeSet(set))
}

func (h *guardedFloat64Histogram) Enabled(ctx context.Context) bool {
	return h.inner.Enabled(ctx)
}

type guardedFloat64Gauge struct {
	embedded.Float64Gauge
	inner metric.Float64Gauge
	guard *GuardedMeter
}

func (g *guardedFloat64Gauge) Record(ctx context.Context, value float64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	set, ok := g.guard.filter(cfg.Attributes())
	if !ok {
		return
	}
	g.inner.Record(ctx, value, metric.WithAttributeSet(set))
}

func (g *guardedFloat64Gauge) Enabled(ctx context.Context) bool {
	return g.inner.Enabled(ctx)
}
