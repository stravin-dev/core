// Package cache manages disposable warm NVMe bare Git repositories under
// <root>/t/{tenant_id}/{repo_id} with per-repo singleflight materialization,
// partitioned per-tenant LRU byte quotas, reader-lease pinning, incremental
// manifest shard watermarks, and a pack.BaseResolver adapter.
package cache

import "errors"

var (
	// ErrEmptyRoot is returned when Config.Root is empty or whitespace.
	ErrEmptyRoot = errors.New("cache: empty root directory")
	// ErrInvalidConfig is returned when Config contains invalid quota parameters.
	ErrInvalidConfig = errors.New("cache: invalid configuration")
	// ErrInvalidPackID is returned when a pack identifier is empty or contains unsafe characters.
	ErrInvalidPackID = errors.New("cache: invalid pack id")
	// ErrRepoNotFound is returned when a requested repository is not present in the cache.
	ErrRepoNotFound = errors.New("cache: repository not found")
)
