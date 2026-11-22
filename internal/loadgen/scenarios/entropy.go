// Package scenarios implements 22 realistic developer and AI coding-agent
// Git hosting scenarios with a deterministic entropy engine for stravin-loadgen.
package scenarios

import (
	"fmt"
	"math/rand/v2"
	"path"
	"strings"
)

var (
	entropyTopics = []string{
		"auth", "pack", "cache", "edge", "hooks", "telemetry",
		"refs", "index", "delta", "gc", "router", "storage",
	}
	entropyVerbs = []string{
		"update", "refactor", "fix", "add", "optimize", "verify",
		"checkpoint", "backport", "clean", "sync",
	}
	entropyInvalidRefTemplates = []string{
		"refs/heads/invalid..ref-%08x",
		"refs/heads/bad~ref-%08x",
		"refs/heads/bad@{ref}-%08x",
		"refs/heads/bad:ref-%08x",
		"refs/heads/bad?ref-%08x",
	}
)

// Entropy generates deterministic scenario parameters, file contents, branch names,
// tags, binary blobs, and directory trees solely from a caller-provided *rand.Rand.
type Entropy struct {
	rng *rand.Rand
}

// NewEntropy wraps rng in a deterministic Entropy generator.
func NewEntropy(rng *rand.Rand) *Entropy {
	if rng == nil {
		rng = rand.New(rand.NewPCG(1, 1)) //#nosec G404 -- deterministic fallback PRNG for loadgen scenarios
	}
	return &Entropy{rng: rng}
}

// IntRange returns a deterministic integer in the inclusive range [minVal, maxVal].
func (e *Entropy) IntRange(minVal, maxVal int) int {
	if maxVal <= minVal {
		return minVal
	}
	return minVal + e.rng.IntN(maxVal-minVal+1)
}

// HexID returns a deterministic 8-character lowercase hex identifier.
func (e *Entropy) HexID() string {
	return fmt.Sprintf("%08x", e.rng.Uint32())
}

// Slug returns a deterministic identifier formatted as "<prefix>-<hex>".
func (e *Entropy) Slug(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, e.HexID())
}

// BranchName returns a deterministic valid Git branch name under loadgen/<prefix>/.
func (e *Entropy) BranchName(prefix string) string {
	topic := entropyTopics[e.rng.IntN(len(entropyTopics))]
	return fmt.Sprintf("loadgen/%s/%s-%s", prefix, topic, e.HexID())
}

// TagName returns a deterministic valid Git tag name.
func (e *Entropy) TagName(prefix string) string {
	major := e.IntRange(1, 9)
	minor := e.IntRange(0, 99)
	patch := e.IntRange(0, 99)
	return fmt.Sprintf("%s-v%d.%d.%d-%s", prefix, major, minor, patch, e.HexID())
}

// CommitMessage returns a deterministic realistic commit message.
func (e *Entropy) CommitMessage(scope string) string {
	verb := entropyVerbs[e.rng.IntN(len(entropyVerbs))]
	topic := entropyTopics[e.rng.IntN(len(entropyTopics))]
	return fmt.Sprintf("%s(%s): %s %s [%s]", verb, scope, verb, topic, e.HexID())
}

// TextPayload returns deterministic multi-line UTF-8 source/text content.
func (e *Entropy) TextPayload(lines int) []byte {
	if lines < 1 {
		lines = 1
	}
	var b strings.Builder
	b.Grow(lines * 48)
	for i := 0; i < lines; i++ {
		topic := entropyTopics[e.rng.IntN(len(entropyTopics))]
		fmt.Fprintf(&b, "line=%04d topic=%s val=%016x\n", i, topic, e.rng.Uint64())
	}
	return []byte(b.String())
}

// SmallDelta appends a small deterministic delta block to base so Git pack
// generation can produce a thin delta against the previous blob.
func (e *Entropy) SmallDelta(base []byte) []byte {
	delta := fmt.Sprintf("delta=%s val=%016x\n", e.HexID(), e.rng.Uint64())
	out := make([]byte, 0, len(base)+len(delta))
	out = append(out, base...)
	out = append(out, delta...)
	return out
}

// IncompressibleBlob generates sizeBytes of deterministic pseudo-random bytes
// that resist zlib compression (for testing large binary asset transfers).
func (e *Entropy) IncompressibleBlob(sizeBytes int) []byte {
	if sizeBytes <= 0 {
		return nil
	}
	buf := make([]byte, sizeBytes)
	i := 0
	for ; i+8 <= sizeBytes; i += 8 {
		v := e.rng.Uint64()
		buf[i] = byte(v)
		buf[i+1] = byte(v >> 8)
		buf[i+2] = byte(v >> 16)
		buf[i+3] = byte(v >> 24)
		buf[i+4] = byte(v >> 32)
		buf[i+5] = byte(v >> 40)
		buf[i+6] = byte(v >> 48)
		buf[i+7] = byte(v >> 56)
	}
	if i < sizeBytes {
		v := e.rng.Uint64()
		for ; i < sizeBytes; i++ {
			buf[i] = byte(v)
			v >>= 8
		}
	}
	return buf
}

// DeepTreePaths returns count deterministic nested relative paths at the given directory depth.
func (e *Entropy) DeepTreePaths(depth, count int) []string {
	if depth < 1 {
		depth = 1
	}
	if count < 1 {
		count = 1
	}
	paths := make([]string, 0, count)
	root := "tree-" + e.HexID()
	for i := 0; i < count; i++ {
		parts := make([]string, 0, depth+2)
		parts = append(parts, root)
		for d := 0; d < depth; d++ {
			topic := entropyTopics[e.rng.IntN(len(entropyTopics))]
			parts = append(parts, fmt.Sprintf("d%d-%s-%d", d, topic, e.IntRange(0, 2)))
		}
		parts = append(parts, fmt.Sprintf("file-%02d-%s.txt", i, e.HexID()))
		paths = append(paths, path.Join(parts...))
	}
	return paths
}

// InvalidRefName returns a deterministic syntactically invalid Git reference name.
func (e *Entropy) InvalidRefName() string {
	tmpl := entropyInvalidRefTemplates[e.rng.IntN(len(entropyInvalidRefTemplates))]
	return fmt.Sprintf(tmpl, e.rng.Uint32())
}
