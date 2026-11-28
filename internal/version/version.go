// Package version holds the Stravin Core build version.
package version

// Version is the semantic version of Stravin Core in Go's vX.Y.Z convention.
// It is updated on release commits and may be overridden at link time via:
//
//	-ldflags "-X stravin.dev/core/internal/version.Version=vX.Y.Z"
var Version = "v1.16.0"

// String returns the current Stravin Core version string.
func String() string {
	return Version
}
