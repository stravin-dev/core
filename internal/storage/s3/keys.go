package s3

import (
	"fmt"
	"strings"

	"stravin.dev/core/internal/tenant"
)

const sha256HexLength = 64

// PackKey returns "{pack_sha256[0:2]}/{tenant_id}/{repo_id}/packs/{pack_sha256}.pack".
func PackKey(repo tenant.RepoKey, packSHA256 string) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	sha, err := normalizeSHA256(packSHA256)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/packs/%s.pack", repo.S3Prefix(sha[:2]), sha), nil
}

// IndexKey returns "{pack_sha256[0:2]}/{tenant_id}/{repo_id}/packs/{pack_sha256}.idx".
func IndexKey(repo tenant.RepoKey, packSHA256 string) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	sha, err := normalizeSHA256(packSHA256)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/packs/%s.idx", repo.S3Prefix(sha[:2]), sha), nil
}

// AuxMIDXKey returns "{hash_prefix}/{tenant_id}/{repo_id}/aux/{manifest_epoch}.midx".
// hashPrefixOrSHA may be either a 2-character hex prefix or a full 64-character SHA-256 hex digest.
func AuxMIDXKey(repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string) (string, error) {
	return auxKey(repo, hashPrefixOrSHA, manifestEpoch, "midx")
}

// AuxGraphKey returns "{hash_prefix}/{tenant_id}/{repo_id}/aux/{manifest_epoch}.graph".
// hashPrefixOrSHA may be either a 2-character hex prefix or a full 64-character SHA-256 hex digest.
func AuxGraphKey(repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string) (string, error) {
	return auxKey(repo, hashPrefixOrSHA, manifestEpoch, "graph")
}

// WasmHookKey returns "hooks/{wasm_sha256}.wasm".
func WasmHookKey(wasmSHA256 string) (string, error) {
	sha, err := normalizeSHA256(wasmSHA256)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("hooks/%s.wasm", sha), nil
}

// BundleKey returns "bundles/{tenant_id}/{repo_id}/latest.bundle".
func BundleKey(repo tenant.RepoKey) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf("bundles/%s/%s/latest.bundle", repo.TenantID, repo.RepoID), nil
}

func auxKey(repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch, ext string) (string, error) {
	if err := repo.Validate(); err != nil {
		return "", err
	}
	prefix, err := normalizeHashPrefix(hashPrefixOrSHA)
	if err != nil {
		return "", err
	}
	if err := validateManifestEpoch(manifestEpoch); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/aux/%s.%s", repo.S3Prefix(prefix), manifestEpoch, ext), nil
}

func normalizeSHA256(raw string) (string, error) {
	lower := strings.ToLower(strings.TrimSpace(raw))
	if len(lower) != sha256HexLength || !isHex(lower) {
		return "", fmt.Errorf("%w: invalid sha256 %q", ErrInvalidInput, raw)
	}
	return lower, nil
}

func normalizeHashPrefix(raw string) (string, error) {
	lower := strings.ToLower(strings.TrimSpace(raw))
	switch len(lower) {
	case 2:
		if !isHex(lower) {
			return "", fmt.Errorf("%w: invalid hash prefix %q", ErrInvalidInput, raw)
		}
		return lower, nil
	case sha256HexLength:
		if !isHex(lower) {
			return "", fmt.Errorf("%w: invalid sha256 %q", ErrInvalidInput, raw)
		}
		return lower[:2], nil
	default:
		return "", fmt.Errorf("%w: invalid hash prefix %q", ErrInvalidInput, raw)
	}
}

func validateManifestEpoch(epoch string) error {
	if epoch == "" || strings.Contains(epoch, "..") {
		return fmt.Errorf("%w: invalid manifest epoch %q", ErrInvalidInput, epoch)
	}
	for i := 0; i < len(epoch); i++ {
		c := epoch[i]
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !isAlphaNum && c != '-' && c != '_' && c != '.' {
			return fmt.Errorf("%w: invalid manifest epoch %q", ErrInvalidInput, epoch)
		}
	}
	return nil
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func parseRepoPackKey(key string, repo tenant.RepoKey) (string, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		return "", false
	}
	prefix, tID, rID, kind, file := parts[0], parts[1], parts[2], parts[3], parts[4]
	if tID != string(repo.TenantID) || rID != string(repo.RepoID) || kind != "packs" {
		return "", false
	}
	if !strings.HasSuffix(file, ".pack") {
		return "", false
	}
	sha := strings.TrimSuffix(file, ".pack")
	if len(sha) != sha256HexLength || !isHex(sha) || prefix != sha[:2] {
		return "", false
	}
	return sha, true
}
