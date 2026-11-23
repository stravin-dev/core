// Package telemetry provides OpenTelemetry tracing, metrics, W3C propagation,
// structured log correlation, and Prometheus cardinality guardrails for Stravin Core.
package telemetry

import (
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"

	"stravin.dev/core/internal/version"
)

// Standardized Stravin resource, span, log, and metric attribute keys.
const (
	AttrServiceName           = attribute.Key("service.name")
	AttrServiceVersion        = attribute.Key("service.version")
	AttrDeploymentEnvironment = attribute.Key("deployment.environment")
	AttrCloudRegion           = attribute.Key("cloud.region")
	AttrRegion                = attribute.Key("region")

	AttrHomeRegion    = attribute.Key("home_region")
	AttrTenantID      = attribute.Key("tenant_id")
	AttrRepoID        = attribute.Key("repo_id")
	AttrRefName       = attribute.Key("ref_name")
	AttrPackID        = attribute.Key("pack_id")
	AttrOID           = attribute.Key("oid")
	AttrActorID       = attribute.Key("actor_id")
	AttrRefShard      = attribute.Key("ref_shard")
	AttrManifestShard = attribute.Key("manifest_shard")
	AttrFailoverEpoch = attribute.Key("failover_epoch")
	AttrHookID        = attribute.Key("hook_id")
	AttrGitProtocol   = attribute.Key("git.protocol")
	AttrGitOperation  = attribute.Key("git.operation")
	AttrStatusCode    = attribute.Key("status_code")
	AttrErrorClass    = attribute.Key("error_class")
	AttrFailMode      = attribute.Key("fail_mode")
	AttrShardBucket   = attribute.Key("shard_bucket")
)

// ResourceConfig configures standardized OTel Resource attributes for a Stravin daemon.
type ResourceConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	Region         string
}

// NewResource builds an OpenTelemetry Resource with Stravin's standard attributes.
func NewResource(cfg ResourceConfig) (*resource.Resource, error) {
	if cfg.ServiceName == "" {
		return nil, errors.New("telemetry: service name is required")
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = version.String()
	}
	if cfg.Environment == "" {
		cfg.Environment = "local"
	}

	attrs := []attribute.KeyValue{
		AttrServiceName.String(cfg.ServiceName),
		AttrServiceVersion.String(cfg.ServiceVersion),
		AttrDeploymentEnvironment.String(cfg.Environment),
	}
	if cfg.Region != "" {
		attrs = append(attrs,
			AttrCloudRegion.String(cfg.Region),
			AttrRegion.String(cfg.Region),
		)
	}

	return resource.NewSchemaless(attrs...), nil
}

// SpanAttrBuilder constructs standardized span attribute slices using the GoF Builder pattern.
type SpanAttrBuilder struct {
	attrs []attribute.KeyValue
}

// NewSpanAttrs returns a new fluent SpanAttrBuilder.
func NewSpanAttrs() *SpanAttrBuilder {
	return &SpanAttrBuilder{}
}

func (b *SpanAttrBuilder) HomeRegion(v string) *SpanAttrBuilder {
	if v != "" {
		b.attrs = append(b.attrs, AttrHomeRegion.String(v))
	}
	return b
}

func (b *SpanAttrBuilder) TenantID(v string) *SpanAttrBuilder {
	if v != "" {
		b.attrs = append(b.attrs, AttrTenantID.String(v))
	}
	return b
}

func (b *SpanAttrBuilder) RepoID(v string) *SpanAttrBuilder {
	if v != "" {
		b.attrs = append(b.attrs, AttrRepoID.String(v))
	}
	return b
}

func (b *SpanAttrBuilder) RefShard(shard int) *SpanAttrBuilder {
	b.attrs = append(b.attrs, AttrRefShard.Int(shard))
	return b
}

func (b *SpanAttrBuilder) ManifestShard(shard int) *SpanAttrBuilder {
	b.attrs = append(b.attrs, AttrManifestShard.Int(shard))
	return b
}

func (b *SpanAttrBuilder) FailoverEpoch(epoch int64) *SpanAttrBuilder {
	b.attrs = append(b.attrs, AttrFailoverEpoch.Int64(epoch))
	return b
}

func (b *SpanAttrBuilder) HookID(v string) *SpanAttrBuilder {
	if v != "" {
		b.attrs = append(b.attrs, AttrHookID.String(v))
	}
	return b
}

func (b *SpanAttrBuilder) GitProtocol(v string) *SpanAttrBuilder {
	if v != "" {
		b.attrs = append(b.attrs, AttrGitProtocol.String(v))
	}
	return b
}

func (b *SpanAttrBuilder) GitOperation(v string) *SpanAttrBuilder {
	if v != "" {
		b.attrs = append(b.attrs, AttrGitOperation.String(v))
	}
	return b
}

// Build returns a copy of the accumulated span attributes.
func (b *SpanAttrBuilder) Build() []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(b.attrs))
	copy(out, b.attrs)
	return out
}
