package ddb

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
)

func TestObjectFormatAndBackwardCompatMeta(t *testing.T) {
	repo := mustRepoKey(t, "acme", "dual-hash")

	// Legacy DDB META item without "object_format" attribute must default to sha1.
	legacyItem := map[string]types.AttributeValue{
		AttrPK:            attrS(repo.DynamoPrefix()),
		AttrSK:            attrS("META"),
		"home_region":     attrS("us-east-1"),
		"failover_epoch":  attrNUint64(1),
		"ref_shards":      attrNInt(1),
		"manifest_shards": attrNInt(1),
		"write_state":     attrS(string(lease.ModeActive)),
		"default_branch":  attrS("refs/heads/main"),
	}
	legacyMeta, err := unmarshalRepoMeta(repo, legacyItem)
	if err != nil {
		t.Fatalf("unmarshalRepoMeta(legacy): %v", err)
	}
	if legacyMeta.ObjectFormat != ObjectFormatSHA1 || legacyMeta.EffectiveObjectFormat() != ObjectFormatSHA1 {
		t.Fatalf("legacy object_format = %q, want %q", legacyMeta.ObjectFormat, ObjectFormatSHA1)
	}

	// Explicit sha256 round-trip through marshal/unmarshal.
	sha256Meta := RepoMeta{
		Repo:           repo,
		HomeRegion:     lease.Region("us-east-1"),
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
		ObjectFormat:   ObjectFormatSHA256,
	}
	if err := sha256Meta.Validate(); err != nil {
		t.Fatalf("sha256Meta.Validate: %v", err)
	}
	key, _ := MetaKey(repo)
	item := marshalRepoMeta(key, sha256Meta)
	gotMeta, err := unmarshalRepoMeta(repo, item)
	if err != nil {
		t.Fatalf("unmarshalRepoMeta(sha256): %v", err)
	}
	if gotMeta.ObjectFormat != ObjectFormatSHA256 {
		t.Fatalf("round-trip object_format = %q, want %q", gotMeta.ObjectFormat, ObjectFormatSHA256)
	}

	// Invalid object_format rejected.
	badMeta := sha256Meta
	badMeta.ObjectFormat = ObjectFormat("md5")
	if err := badMeta.Validate(); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("badMeta.Validate = %v, want ErrInvalidInput", err)
	}
}

func TestValidateOIDDualHashAndPushTransactionSHA256(t *testing.T) {
	sha1OID := strings.Repeat("a", 40)
	sha256OID := strings.Repeat("b", 64)

	if err := ValidateOID(sha1OID); err != nil {
		t.Fatalf("ValidateOID(sha1): %v", err)
	}
	if err := ValidateOID(sha256OID); err != nil {
		t.Fatalf("ValidateOID(sha256): %v", err)
	}
	if err := ValidateOID(ZeroOIDSHA256); err != nil {
		t.Fatalf("ValidateOID(ZeroOIDSHA256): %v", err)
	}
	if err := ValidateOIDForFormat(sha1OID, ObjectFormatSHA1); err != nil {
		t.Fatalf("ValidateOIDForFormat(sha1, sha1): %v", err)
	}
	if err := ValidateOIDForFormat(sha256OID, ObjectFormatSHA256); err != nil {
		t.Fatalf("ValidateOIDForFormat(sha256, sha256): %v", err)
	}
	if err := ValidateOIDForFormat(sha1OID, ObjectFormatSHA256); !errors.Is(err, ErrInvalidOID) {
		t.Fatalf("ValidateOIDForFormat(sha1, sha256) = %v, want ErrInvalidOID", err)
	}
	if err := ValidateOIDForFormat(sha256OID, ObjectFormatSHA1); !errors.Is(err, ErrInvalidOID) {
		t.Fatalf("ValidateOIDForFormat(sha256, sha1) = %v, want ErrInvalidOID", err)
	}

	repo := mustRepoKey(t, "acme", "sha256-push")
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

	items, _, err := BuildPushTransaction(CommitPushInput{
		Repo:           repo,
		RefShards:      1,
		ManifestShards: 1,
		Epoch:          1,
		OriginRegion:   lease.Region("us-east-1"),
		ObjectFormat:   ObjectFormatSHA256,
		Updates: []RefUpdate{
			{RefName: "refs/heads/main", OldOID: ZeroOIDSHA256, NewOID: sha256OID},
			{RefName: "refs/heads/old", OldOID: sha256OID, NewOID: ZeroOIDSHA256},
		},
	}, "01ARZ3NDEKTSV4RRFFQ69G5FAV", now, "stravin-core")
	if err != nil {
		t.Fatalf("BuildPushTransaction(sha256): %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("got %d transact items, want 5", len(items))
	}
	// Verify prev_oid on create uses 64-zero SHA-256 null OID.
	if got := getString(items[0].Put.Item, "prev_oid"); got != ZeroOIDSHA256 {
		t.Fatalf("create prev_oid = %q, want ZeroOIDSHA256", got)
	}

	// Mismatched 40-hex OID on sha256 repo must be rejected.
	_, _, err = BuildPushTransaction(CommitPushInput{
		Repo:           repo,
		RefShards:      1,
		ManifestShards: 1,
		Epoch:          1,
		OriginRegion:   lease.Region("us-east-1"),
		ObjectFormat:   ObjectFormatSHA256,
		Updates: []RefUpdate{
			{RefName: "refs/heads/main", OldOID: ZeroOID, NewOID: sha1OID},
		},
	}, "01ARZ3NDEKTSV4RRFFQ69G5FAV", now, "stravin-core")
	if !errors.Is(err, ErrInvalidOID) {
		t.Fatalf("mismatched sha1 OID on sha256 push = %v, want ErrInvalidOID", err)
	}
}
