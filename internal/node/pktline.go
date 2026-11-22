// Package node implements the stateless stravin-node virtual replica daemon:
// gRPC GitWire streaming, background regional write-lease/failover_epoch
// refresh, incremental DynamoDB ref and manifest watermark synchronization,
// concurrent S3 pack pull into the warm NVMe cache, pure-Go pkt-line ref
// advertisement strictly from DynamoDB, and sandboxed git-upload-pack execution.
package node

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"stravin.dev/core/internal/storage/ddb"
)

const (
	defaultServiceName = "git-upload-pack"
	defaultBranchRef   = "refs/heads/main"
	defaultGitAgent    = "stravin-node/1.0"
)

// FlushPkt is the standard Git pkt-line flush packet ("0000").
var FlushPkt = []byte("0000")

// EncodePktLine encodes payload as a single Git pkt-line with a 4-hex-digit
// big-endian length prefix including the 4 length bytes.
func EncodePktLine(payload []byte) []byte {
	n := len(payload) + 4
	out := make([]byte, n)
	copy(out[:4], fmt.Sprintf("%04x", n))
	copy(out[4:], payload)
	return out
}

// AdvertiseOptions configures Git pkt-line ref advertisement formatting.
type AdvertiseOptions struct {
	// IncludeServiceHeader prepends the Smart HTTP `# service=<name>\n` + flush-pkt header.
	IncludeServiceHeader bool
	// ServiceName defaults to "git-upload-pack".
	ServiceName string
	// Agent overrides the Git agent capability string.
	Agent string
}

// AdvertiseRefs encodes a standard Git protocol v1 pkt-line ref advertisement
// strictly from DynamoDB RefState records (never consulting local disk refs).
func AdvertiseRefs(refs []ddb.RefState, defaultBranch string, opts AdvertiseOptions) []byte {
	serviceName := strings.TrimSpace(opts.ServiceName)
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	agent := strings.TrimSpace(opts.Agent)
	if agent == "" {
		agent = defaultGitAgent
	}
	if strings.TrimSpace(defaultBranch) == "" {
		defaultBranch = defaultBranchRef
	}

	var buf bytes.Buffer
	if opts.IncludeServiceHeader {
		buf.Write(EncodePktLine([]byte("# service=" + serviceName + "\n")))
		buf.Write(FlushPkt)
	}

	sorted := make([]ddb.RefState, 0, len(refs))
	var headOID string
	for _, r := range refs {
		if r.RefName == "" || r.OID == "" || r.RefName == "HEAD" {
			continue
		}
		if r.RefName == defaultBranch {
			headOID = r.OID
		}
		sorted = append(sorted, r)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].RefName < sorted[j].RefName
	})

	caps := uploadPackCapabilities(agent, headOID != "", defaultBranch)

	if len(sorted) == 0 {
		line := fmt.Sprintf("%s capabilities^{}\x00%s\n", ddb.ZeroOID, caps)
		buf.Write(EncodePktLine([]byte(line)))
		buf.Write(FlushPkt)
		return buf.Bytes()
	}

	firstEmitted := false
	if headOID != "" {
		line := fmt.Sprintf("%s HEAD\x00%s\n", headOID, caps)
		buf.Write(EncodePktLine([]byte(line)))
		firstEmitted = true
	}

	for _, r := range sorted {
		if !firstEmitted {
			line := fmt.Sprintf("%s %s\x00%s\n", r.OID, r.RefName, caps)
			buf.Write(EncodePktLine([]byte(line)))
			firstEmitted = true
			continue
		}
		line := fmt.Sprintf("%s %s\n", r.OID, r.RefName)
		buf.Write(EncodePktLine([]byte(line)))
	}

	buf.Write(FlushPkt)
	return buf.Bytes()
}

func uploadPackCapabilities(agent string, hasHEAD bool, defaultBranch string) string {
	caps := []string{
		"multi_ack",
		"thin-pack",
		"side-band",
		"side-band-64k",
		"ofs-delta",
		"shallow",
		"deepen-since",
		"deepen-not",
		"deepen-relative",
		"no-progress",
		"include-tag",
		"multi_ack_detailed",
		"allow-tip-sha1-in-want",
		"allow-reachable-sha1-in-want",
		"no-done",
	}
	if hasHEAD {
		caps = append(caps, "symref=HEAD:"+defaultBranch)
	}
	caps = append(caps, "agent="+agent)
	return strings.Join(caps, " ")
}
