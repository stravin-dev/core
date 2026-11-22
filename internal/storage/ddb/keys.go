package ddb

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

const (
	AttrPK = "PK"
	AttrSK = "SK"

	MinRefShards      = 1
	MaxRefShards      = 64
	MinManifestShards = 1
	MaxManifestShards = 32

	// MaxAtomicPushRefs caps per-push ref updates to stay safely under
	// DynamoDB's 100-item TransactWriteItems limit (45*2 RS+RH + 1 MS = 91).
	MaxAtomicPushRefs = 45

	// ZeroOID is Git's 40-zero null object ID used for branch creation/deletion.
	ZeroOID = "0000000000000000000000000000000000000000"

	// RefHistoryTTL is the automatic DynamoDB TTL retention window for Ref History outbox items.
	RefHistoryTTL = 7 * 24 * time.Hour
)

// Key holds the single-table DynamoDB primary key (PK, SK).
type Key struct {
	PK string
	SK string
}

// AttributeMap encodes Key into a DynamoDB key attribute map.
func (k Key) AttributeMap() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		AttrPK: attrS(k.PK),
		AttrSK: attrS(k.SK),
	}
}

// ValidateRefShards enforces S in [1, 64].
func ValidateRefShards(refShards int) error {
	if refShards < MinRefShards || refShards > MaxRefShards {
		return fmt.Errorf("%w: ref_shards=%d (want %d..%d)", ErrInvalidShardCount, refShards, MinRefShards, MaxRefShards)
	}
	return nil
}

// ValidateManifestShards enforces M in [1, 32].
func ValidateManifestShards(manifestShards int) error {
	if manifestShards < MinManifestShards || manifestShards > MaxManifestShards {
		return fmt.Errorf("%w: manifest_shards=%d (want %d..%d)", ErrInvalidShardCount, manifestShards, MinManifestShards, MaxManifestShards)
	}
	return nil
}

// RefShard returns the deterministic FNV-1a shard index in [0, refShards).
func RefShard(refName string, refShards int) (int, error) {
	if err := validateNonEmpty("ref_name", refName); err != nil {
		return 0, err
	}
	if err := ValidateRefShards(refShards); err != nil {
		return 0, err
	}
	return deterministicShard(refName, refShards), nil
}

// ManifestShard returns the deterministic FNV-1a shard index in [0, manifestShards).
func ManifestShard(packID string, manifestShards int) (int, error) {
	if err := validateNonEmpty("pack_id", packID); err != nil {
		return 0, err
	}
	if err := ValidateManifestShards(manifestShards); err != nil {
		return 0, err
	}
	return deterministicShard(packID, manifestShards), nil
}

func deterministicShard(input string, shards int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(input))
	return int(h.Sum64() % uint64(shards))
}

// MetaKey returns PK=T#{tenant}#REPO#{repo}, SK=META.
func MetaKey(repo tenant.RepoKey) (Key, error) {
	if err := repo.Validate(); err != nil {
		return Key{}, err
	}
	return Key{PK: repo.DynamoPrefix(), SK: "META"}, nil
}

// RefStateShardPK returns PK=T#{tenant}#REPO#{repo}#RS#{shard}.
func RefStateShardPK(repo tenant.RepoKey, shard int) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	if shard < 0 || shard >= MaxRefShards {
		return "", fmt.Errorf("%w: ref shard=%d", ErrInvalidShardCount, shard)
	}
	return fmt.Sprintf("%s#RS#%d", repo.DynamoPrefix(), shard), nil
}

// RefStateKey returns PK=T#{tenant}#REPO#{repo}#RS#{s}, SK=REF#{ref_name}.
func RefStateKey(repo tenant.RepoKey, refName string, refShards int) (Key, error) {
	shard, err := RefShard(refName, refShards)
	if err != nil {
		return Key{}, err
	}
	pk, err := RefStateShardPK(repo, shard)
	if err != nil {
		return Key{}, err
	}
	return Key{PK: pk, SK: "REF#" + refName}, nil
}

// RefHistoryShardPK returns PK=T#{tenant}#REPO#{repo}#RH#{shard}.
func RefHistoryShardPK(repo tenant.RepoKey, shard int) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	if shard < 0 || shard >= MaxRefShards {
		return "", fmt.Errorf("%w: ref shard=%d", ErrInvalidShardCount, shard)
	}
	return fmt.Sprintf("%s#RH#%d", repo.DynamoPrefix(), shard), nil
}

// RefHistoryKey returns PK=T#{tenant}#REPO#{repo}#RH#{s}, SK=REF#{ref_name}#{ulid}.
func RefHistoryKey(repo tenant.RepoKey, refName, ulid string, refShards int) (Key, error) {
	if err := validateNonEmpty("ulid", ulid); err != nil {
		return Key{}, err
	}
	shard, err := RefShard(refName, refShards)
	if err != nil {
		return Key{}, err
	}
	pk, err := RefHistoryShardPK(repo, shard)
	if err != nil {
		return Key{}, err
	}
	return Key{PK: pk, SK: fmt.Sprintf("REF#%s#%s", refName, ulid)}, nil
}

// ManifestShardPK returns PK=T#{tenant}#REPO#{repo}#MS#{shard}.
func ManifestShardPK(repo tenant.RepoKey, shard int) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	if shard < 0 || shard >= MaxManifestShards {
		return "", fmt.Errorf("%w: manifest shard=%d", ErrInvalidShardCount, shard)
	}
	return fmt.Sprintf("%s#MS#%d", repo.DynamoPrefix(), shard), nil
}

// ManifestKey returns PK=T#{tenant}#REPO#{repo}#MS#{m}, SK=PACK#{ulid}#{pack_id}.
func ManifestKey(repo tenant.RepoKey, ulid, packID string, manifestShards int) (Key, error) {
	if err := validateNonEmpty("ulid", ulid); err != nil {
		return Key{}, err
	}
	shard, err := ManifestShard(packID, manifestShards)
	if err != nil {
		return Key{}, err
	}
	pk, err := ManifestShardPK(repo, shard)
	if err != nil {
		return Key{}, err
	}
	return Key{PK: pk, SK: fmt.Sprintf("PACK#%s#%s", ulid, packID)}, nil
}

// CompactionLeaseKey returns PK=T#{tenant}#REPO#{repo}#LEASE, SK=SHARD#{shard}.
func CompactionLeaseKey(repo tenant.RepoKey, shard int) (Key, error) {
	if err := repo.Validate(); err != nil {
		return Key{}, err
	}
	if shard < 0 || shard >= MaxManifestShards {
		return Key{}, fmt.Errorf("%w: manifest shard=%d", ErrInvalidShardCount, shard)
	}
	return Key{
		PK: fmt.Sprintf("%s#LEASE", repo.DynamoPrefix()),
		SK: fmt.Sprintf("SHARD#%d", shard),
	}, nil
}

// RegionalWriteLeaseKey returns PK=REGION#{region}#LEASE, SK=WRITE.
func RegionalWriteLeaseKey(region lease.Region) (Key, error) {
	if strings.TrimSpace(string(region)) == "" {
		return Key{}, lease.ErrEmptyRegion
	}
	return Key{
		PK: fmt.Sprintf("REGION#%s#LEASE", region),
		SK: "WRITE",
	}, nil
}

// RepoHooksKey returns PK=T#{tenant}#REPO#{repo}, SK=HOOKS.
func RepoHooksKey(repo tenant.RepoKey) (Key, error) {
	if err := repo.Validate(); err != nil {
		return Key{}, err
	}
	return Key{PK: repo.DynamoPrefix(), SK: "HOOKS"}, nil
}

// OrgHooksKey returns PK=T#{tenant}#ORG#{org_id}, SK=HOOKS.
func OrgHooksKey(tenantID tenant.TenantID, orgID string) (Key, error) {
	if err := tenantID.Validate(); err != nil {
		return Key{}, err
	}
	if err := validateNonEmpty("org_id", orgID); err != nil {
		return Key{}, err
	}
	return Key{PK: fmt.Sprintf("T#%s#ORG#%s", tenantID, orgID), SK: "HOOKS"}, nil
}

// WebhooksKey returns PK=T#{tenant}#REPO#{repo}, SK=WEBHOOKS.
func WebhooksKey(repo tenant.RepoKey) (Key, error) {
	if err := repo.Validate(); err != nil {
		return Key{}, err
	}
	return Key{PK: repo.DynamoPrefix(), SK: "WEBHOOKS"}, nil
}

// StreamCheckpointKey returns PK=STREAMS#CHECKPOINT, SK=SHARD#{shard_id}.
func StreamCheckpointKey(shardID string) (Key, error) {
	if err := validateNonEmpty("shard_id", shardID); err != nil {
		return Key{}, err
	}
	return Key{PK: "STREAMS#CHECKPOINT", SK: "SHARD#" + shardID}, nil
}

// ValidateOID verifies that oid is a 40-character lowercase hex Git object ID.
func ValidateOID(oid string) error {
	if len(oid) != 40 {
		return fmt.Errorf("%w: length=%d", ErrInvalidOID, len(oid))
	}
	for i := 0; i < len(oid); i++ {
		c := oid[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("%w: non-hex character at %d", ErrInvalidOID, i)
		}
	}
	return nil
}

const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewULID generates a 26-character lexicographically sortable Crockford Base32 ULID.
func NewULID(now time.Time) (string, error) {
	ms := uint64(now.UTC().UnixMilli())
	var raw [16]byte
	// 48-bit big-endian millisecond timestamp in raw[0:6].
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], ms)
	copy(raw[0:6], tsBuf[2:8])
	if _, err := rand.Read(raw[6:16]); err != nil {
		return "", fmt.Errorf("ddb: ulid entropy: %w", err)
	}

	var out [26]byte
	// 10 timestamp characters (48 bits).
	out[0] = ulidAlphabet[(raw[0]&0xE0)>>5]
	out[1] = ulidAlphabet[raw[0]&0x1F]
	out[2] = ulidAlphabet[(raw[1]&0xF8)>>3]
	out[3] = ulidAlphabet[((raw[1]&0x07)<<2)|((raw[2]&0xC0)>>6)]
	out[4] = ulidAlphabet[(raw[2]&0x3E)>>1]
	out[5] = ulidAlphabet[((raw[2]&0x01)<<4)|((raw[3]&0xF0)>>4)]
	out[6] = ulidAlphabet[((raw[3]&0x0F)<<1)|((raw[4]&0x80)>>7)]
	out[7] = ulidAlphabet[(raw[4]&0x7C)>>2]
	out[8] = ulidAlphabet[((raw[4]&0x03)<<3)|((raw[5]&0xE0)>>5)]
	out[9] = ulidAlphabet[raw[5]&0x1F]

	// 16 randomness characters (80 bits).
	out[10] = ulidAlphabet[(raw[6]&0xF8)>>3]
	out[11] = ulidAlphabet[((raw[6]&0x07)<<2)|((raw[7]&0xC0)>>6)]
	out[12] = ulidAlphabet[(raw[7]&0x3E)>>1]
	out[13] = ulidAlphabet[((raw[7]&0x01)<<4)|((raw[8]&0xF0)>>4)]
	out[14] = ulidAlphabet[((raw[8]&0x0F)<<1)|((raw[9]&0x80)>>7)]
	out[15] = ulidAlphabet[(raw[9]&0x7C)>>2]
	out[16] = ulidAlphabet[((raw[9]&0x03)<<3)|((raw[10]&0xE0)>>5)]
	out[17] = ulidAlphabet[raw[10]&0x1F]
	out[18] = ulidAlphabet[(raw[11]&0xF8)>>3]
	out[19] = ulidAlphabet[((raw[11]&0x07)<<2)|((raw[12]&0xC0)>>6)]
	out[20] = ulidAlphabet[(raw[12]&0x3E)>>1]
	out[21] = ulidAlphabet[((raw[12]&0x01)<<4)|((raw[13]&0xF0)>>4)]
	out[22] = ulidAlphabet[((raw[13]&0x0F)<<1)|((raw[14]&0x80)>>7)]
	out[23] = ulidAlphabet[(raw[14]&0x7C)>>2]
	out[24] = ulidAlphabet[((raw[14]&0x03)<<3)|((raw[15]&0xE0)>>5)]
	out[25] = ulidAlphabet[raw[15]&0x1F]

	return string(out[:]), nil
}

func validateNonEmpty(field, val string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%w: empty %s", ErrInvalidInput, field)
	}
	return nil
}

func attrS(v string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: v}
}

func attrNInt(v int) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.Itoa(v)}
}

func attrNInt64(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func attrNUint64(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

func attrB(v []byte) types.AttributeValue {
	cp := make([]byte, len(v))
	copy(cp, v)
	return &types.AttributeValueMemberB{Value: cp}
}

func attrBool(v bool) types.AttributeValue {
	return &types.AttributeValueMemberBOOL{Value: v}
}

func getString(item map[string]types.AttributeValue, key string) string {
	av, ok := item[key]
	if !ok {
		return ""
	}
	s, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return s.Value
}

func getInt(item map[string]types.AttributeValue, key string) (int, error) {
	n, err := getInt64(item, key)
	return int(n), err
}

func getInt64(item map[string]types.AttributeValue, key string) (int64, error) {
	av, ok := item[key]
	if !ok {
		return 0, nil
	}
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("ddb: attribute %q is not a number", key)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func getUint64(item map[string]types.AttributeValue, key string) (uint64, error) {
	av, ok := item[key]
	if !ok {
		return 0, nil
	}
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("ddb: attribute %q is not a number", key)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}

func getBytes(item map[string]types.AttributeValue, key string) []byte {
	av, ok := item[key]
	if !ok {
		return nil
	}
	b, ok := av.(*types.AttributeValueMemberB)
	if !ok {
		return nil
	}
	cp := make([]byte, len(b.Value))
	copy(cp, b.Value)
	return cp
}

func getBool(item map[string]types.AttributeValue, key string) bool {
	av, ok := item[key]
	if !ok {
		return false
	}
	b, ok := av.(*types.AttributeValueMemberBOOL)
	if !ok {
		return false
	}
	return b.Value
}

func getTime(item map[string]types.AttributeValue, key string) (time.Time, error) {
	raw := getString(item, key)
	if raw == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}
