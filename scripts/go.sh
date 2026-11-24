#!/usr/bin/env bash
set -euo pipefail

# Transparent Go toolchain wrapper.
# Uses host `go` when available; otherwise falls back to a pinned Docker
# golang:1.25-bookworm container (glibc + gcc for `go test -race`) with
# persistent GOMODCACHE and GOCACHE named volumes.

if command -v go >/dev/null 2>&1 && [ "${STRAVIN_FORCE_DOCKER_GO:-0}" != "1" ]; then
  exec go "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "error: neither 'go' nor 'docker' is available on PATH" >&2
  exit 1
fi

GO_IMAGE="${STRAVIN_GO_IMAGE:-golang:1.25-bookworm}"
MOD_VOL="${STRAVIN_GO_MOD_VOLUME:-stravin-go-mod-cache}"
BUILD_VOL="${STRAVIN_GO_BUILD_VOLUME:-stravin-go-build-cache}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DOCKER_ARGS=(
  run --rm
  -v "${REPO_ROOT}:/workspace"
  -v "${MOD_VOL}:/go/pkg/mod"
  -v "${BUILD_VOL}:/root/.cache/go-build"
  -w /workspace
  -e GOMODCACHE=/go/pkg/mod
  -e GOCACHE=/root/.cache/go-build
)

if [ -n "${CGO_ENABLED:-}" ]; then
  DOCKER_ARGS+=(-e "CGO_ENABLED=${CGO_ENABLED}")
fi
if [ -n "${GOOS:-}" ]; then
  DOCKER_ARGS+=(-e "GOOS=${GOOS}")
fi
if [ -n "${GOARCH:-}" ]; then
  DOCKER_ARGS+=(-e "GOARCH=${GOARCH}")
fi

exec docker "${DOCKER_ARGS[@]}" "${GO_IMAGE}" go "$@"
