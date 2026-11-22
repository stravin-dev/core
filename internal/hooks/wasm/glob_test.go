package wasm

import "testing"

func TestMatchRefGlob(t *testing.T) {
	tests := []struct {
		pattern string
		ref     string
		want    bool
	}{
		{pattern: "*", ref: "refs/heads/main", want: true},
		{pattern: "**", ref: "refs/heads/feat/nested/branch", want: true},
		{pattern: "", ref: "refs/heads/main", want: true},
		{pattern: "refs/heads/main", ref: "refs/heads/main", want: true},
		{pattern: "refs/heads/main", ref: "refs/heads/dev", want: false},
		{pattern: "refs/heads/*", ref: "refs/heads/main", want: true},
		{pattern: "refs/heads/*", ref: "refs/heads/feat/sub", want: false},
		{pattern: "refs/heads/**", ref: "refs/heads/main", want: true},
		{pattern: "refs/heads/**", ref: "refs/heads/feat/sub/deep", want: true},
		{pattern: "refs/heads/**", ref: "refs/tags/v1.0.0", want: false},
		{pattern: "refs/tags/v*", ref: "refs/tags/v1.2.3", want: true},
		{pattern: "refs/tags/v*", ref: "refs/tags/release-1", want: false},
		{pattern: "refs/heads/release-?", ref: "refs/heads/release-1", want: true},
		{pattern: "refs/heads/release-?", ref: "refs/heads/release-10", want: false},
		{pattern: "refs/heads/release-?", ref: "refs/heads/release-/", want: false},
	}

	for _, tc := range tests {
		got := MatchRefGlob(tc.pattern, tc.ref)
		if got != tc.want {
			t.Errorf("MatchRefGlob(%q, %q) = %v, want %v", tc.pattern, tc.ref, got, tc.want)
		}
	}
}
