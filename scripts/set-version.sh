#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 <semver> [target-file]" >&2
  exit 1
fi

raw_version="$1"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_file="${2:-${repo_root}/internal/version/version.go}"

semver_regex='^v?([0-9]+)\.([0-9]+)\.([0-9]+)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
if ! [[ "${raw_version}" =~ ${semver_regex} ]]; then
  echo "Invalid semver version: ${raw_version}" >&2
  exit 1
fi

normalized="v${raw_version#v}"

target_dir="$(dirname "${target_file}")"
mkdir -p "${target_dir}"
tmp_file="$(mktemp "${target_dir}/.version.go.XXXXXX")"
trap 'rm -f "${tmp_file}"' EXIT

cat >"${tmp_file}" <<EOF
// Package version holds the Stravin Core build version.
package version

// Version is the semantic version of Stravin Core in Go's vX.Y.Z convention.
// It is updated on release commits and may be overridden at link time via:
//
//	-ldflags "-X stravin.dev/core/internal/version.Version=vX.Y.Z"
var Version = "${normalized}"

// String returns the current Stravin Core version string.
func String() string {
	return Version
}
EOF

mv "${tmp_file}" "${target_file}"
echo "Updated ${target_file} to ${normalized}"
