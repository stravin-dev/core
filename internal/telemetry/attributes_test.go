package telemetry_test

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"stravin.dev/core/internal/telemetry"
)

func TestNewResource_StandardAttributes(t *testing.T) {
	res, err := telemetry.NewResource(telemetry.ResourceConfig{
		ServiceName:    "stravin-node",
		ServiceVersion: "1.0.0",
		Environment:    "local",
		Region:         "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewResource returned error: %v", err)
	}

	got := make(map[attribute.Key]string)
	for _, kv := range res.Attributes() {
		got[kv.Key] = kv.Value.AsString()
	}

	want := map[attribute.Key]string{
		"service.name":           "stravin-node",
		"service.version":        "1.0.0",
		"deployment.environment": "local",
		"cloud.region":           "us-east-1",
		"region":                 "us-east-1",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("resource attr %q = %q, want %q", k, got[k], v)
		}
	}
}

func TestNewResource_RequiresServiceName(t *testing.T) {
	if _, err := telemetry.NewResource(telemetry.ResourceConfig{}); err == nil {
		t.Fatal("expected error when ServiceName is empty")
	}
}

func TestSpanAttributesBuilder(t *testing.T) {
	attrs := telemetry.NewSpanAttrs().
		HomeRegion("us-east-1").
		TenantID("tenant-a").
		RepoID("repo-42").
		RefShard(12).
		ManifestShard(3).
		FailoverEpoch(7).
		HookID("hook-lint").
		GitProtocol("ssh").
		GitOperation("push").
		Build()

	got := make(map[attribute.Key]attribute.Value)
	for _, kv := range attrs {
		got[kv.Key] = kv.Value
	}

	if got[telemetry.AttrHomeRegion].AsString() != "us-east-1" {
		t.Errorf("home_region = %v, want us-east-1", got[telemetry.AttrHomeRegion])
	}
	if got[telemetry.AttrTenantID].AsString() != "tenant-a" {
		t.Errorf("tenant_id = %v, want tenant-a", got[telemetry.AttrTenantID])
	}
	if got[telemetry.AttrRepoID].AsString() != "repo-42" {
		t.Errorf("repo_id = %v, want repo-42", got[telemetry.AttrRepoID])
	}
	if got[telemetry.AttrRefShard].AsInt64() != 12 {
		t.Errorf("ref_shard = %v, want 12", got[telemetry.AttrRefShard])
	}
	if got[telemetry.AttrManifestShard].AsInt64() != 3 {
		t.Errorf("manifest_shard = %v, want 3", got[telemetry.AttrManifestShard])
	}
	if got[telemetry.AttrFailoverEpoch].AsInt64() != 7 {
		t.Errorf("failover_epoch = %v, want 7", got[telemetry.AttrFailoverEpoch])
	}
	if got[telemetry.AttrHookID].AsString() != "hook-lint" {
		t.Errorf("hook_id = %v, want hook-lint", got[telemetry.AttrHookID])
	}
	if got[telemetry.AttrGitProtocol].AsString() != "ssh" {
		t.Errorf("git.protocol = %v, want ssh", got[telemetry.AttrGitProtocol])
	}
	if got[telemetry.AttrGitOperation].AsString() != "push" {
		t.Errorf("git.operation = %v, want push", got[telemetry.AttrGitOperation])
	}
}
