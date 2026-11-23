// Package node implements the stateless stravin-node virtual replica daemon:
// gRPC GitWire streaming, background regional write-lease/failover_epoch
// refresh, incremental DynamoDB ref and manifest watermark synchronization,
// concurrent S3 pack pull into the warm NVMe cache, pure-Go pkt-line ref
// advertisement strictly from DynamoDB, and sandboxed git-upload-pack execution.
package node

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"stravin.dev/core/internal/storage/ddb"
)

const (
	defaultServiceName = "git-upload-pack"
	receiveServiceName = "git-receive-pack"
	defaultBranchRef   = "refs/heads/main"
	defaultGitAgent    = "stravin-node/1.0"

	// Git side-band-64k channel identifiers.
	SideBandData   byte = 1
	SideBandStderr byte = 2
	SideBandFatal  byte = 3

	maxSideBandChunk = 65515
	maxPktLineLen    = 65520

	// MaxReceiveCommands caps the number of ref-update commands accepted while
	// streaming git-receive-pack pkt-lines, matching DynamoDB's atomic push limit.
	MaxReceiveCommands = ddb.MaxAtomicPushRefs
	// MaxRefNameLen caps the maximum length in bytes of a ref name in a receive command.
	MaxRefNameLen = 1024
	// MaxReceiveCommandLineLen caps the maximum byte length (including the 4-byte
	// hex length prefix) of a single git-receive-pack command pkt-line.
	MaxReceiveCommandLineLen = 4096
)

var (
	// FlushPkt is the standard Git pkt-line flush packet ("0000").
	FlushPkt = []byte("0000")

	// ErrTooManyReceiveCommands is returned when a receive-pack stream exceeds MaxReceiveCommands.
	ErrTooManyReceiveCommands = errors.New("node: too many receive-pack commands")
	// ErrRefNameTooLong is returned when a ref name exceeds MaxRefNameLen bytes.
	ErrRefNameTooLong = errors.New("node: ref name exceeds max length")
	// ErrReceiveCommandLineTooLong is returned when a receive command pkt-line exceeds MaxReceiveCommandLineLen bytes.
	ErrReceiveCommandLineTooLong = errors.New("node: receive-pack command line exceeds max length")
)

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
	// ObjectFormat specifies the repository's Git object hash format (defaults to sha1).
	ObjectFormat ddb.ObjectFormat
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

	isReceivePack := serviceName == receiveServiceName
	objFormat := ddb.NormalizeObjectFormat(opts.ObjectFormat)
	var caps string
	if isReceivePack {
		caps = receivePackCapabilities(agent, objFormat)
	} else {
		caps = uploadPackCapabilities(agent, headOID != "", defaultBranch, objFormat)
	}

	if len(sorted) == 0 {
		line := fmt.Sprintf("%s capabilities^{}\x00%s\n", ddb.ZeroOIDForFormat(objFormat), caps)
		buf.Write(EncodePktLine([]byte(line)))
		buf.Write(FlushPkt)
		return buf.Bytes()
	}

	firstEmitted := false
	if !isReceivePack && headOID != "" {
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

// AdvertiseV2Capabilities encodes a Git Wire Protocol v2 capability
// advertisement strictly from DynamoDB RepoMeta (never consulting local disk).
// Per gitprotocol-v2(5), Protocol v2 is advertised for git-upload-pack;
// git-receive-pack continues to use v0/v1 via AdvertiseRefs.
func AdvertiseV2Capabilities(opts AdvertiseOptions) []byte {
	serviceName := strings.TrimSpace(opts.ServiceName)
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	agent := strings.TrimSpace(opts.Agent)
	if agent == "" {
		agent = defaultGitAgent
	}
	objFormat := ddb.NormalizeObjectFormat(opts.ObjectFormat)

	var buf bytes.Buffer
	if opts.IncludeServiceHeader {
		buf.Write(EncodePktLine([]byte("# service=" + serviceName + "\n")))
		buf.Write(FlushPkt)
	}

	caps := []string{
		"version 2",
		"agent=" + agent,
		"ls-refs=unborn",
		"fetch=shallow wait-for-done filter",
		"server-option",
		"object-format=" + string(objFormat),
	}
	for _, capLine := range caps {
		buf.Write(EncodePktLine([]byte(capLine + "\n")))
	}
	buf.Write(FlushPkt)
	return buf.Bytes()
}

func receivePackCapabilities(agent string, objFormat ddb.ObjectFormat) string {
	caps := []string{
		"report-status",
		"delete-refs",
		"side-band-64k",
		"ofs-delta",
		"atomic",
	}
	if objFormat == ddb.ObjectFormatSHA256 {
		caps = append(caps, "object-format=sha256")
	}
	caps = append(caps, "agent="+agent)
	return strings.Join(caps, " ")
}

// EncodeSideBand multiplexes payload onto a Git side-band-64k channel (1=data, 2=stderr, 3=fatal).
func EncodeSideBand(band byte, payload []byte) []byte {
	var buf bytes.Buffer
	for len(payload) > 0 {
		n := len(payload)
		if n > maxSideBandChunk {
			n = maxSideBandChunk
		}
		frame := make([]byte, 1+n)
		frame[0] = band
		copy(frame[1:], payload[:n])
		buf.Write(EncodePktLine(frame))
		payload = payload[n:]
	}
	return buf.Bytes()
}

type refStatus struct {
	RefName string
	Err     string
}

// EncodeReportStatus formats a Git receive-pack report-status response, optionally
// multiplexed onto side-band-64k channels (channel 2 for stderr, channel 1 for status).
func EncodeReportStatus(unpackStatus string, statuses []refStatus, sideBand bool, stderrLines []string) []byte {
	var statusBuf bytes.Buffer
	statusBuf.Write(EncodePktLine([]byte(unpackStatus + "\n")))
	for _, st := range statuses {
		if st.Err == "" {
			statusBuf.Write(EncodePktLine([]byte("ok " + st.RefName + "\n")))
		} else {
			cleanErr := strings.ReplaceAll(strings.TrimSpace(st.Err), "\n", " ")
			statusBuf.Write(EncodePktLine([]byte(fmt.Sprintf("ng %s %s\n", st.RefName, cleanErr))))
		}
	}
	statusBuf.Write(FlushPkt)

	if !sideBand {
		return statusBuf.Bytes()
	}

	var out bytes.Buffer
	for _, line := range stderrLines {
		if line != "" {
			out.Write(EncodeSideBand(SideBandStderr, []byte(line)))
		}
	}
	out.Write(EncodeSideBand(SideBandData, statusBuf.Bytes()))
	out.Write(FlushPkt)
	return out.Bytes()
}

// ParseReceiveCommands reads Git receive-pack pkt-line ref-update commands up to
// the terminating flush-pkt ("0000"), returning parsed updates and client capabilities.
func ParseReceiveCommands(r io.Reader) ([]ddb.RefUpdate, map[string]bool, error) {
	var (
		cmds []ddb.RefUpdate
		caps = make(map[string]bool)
		lenB [4]byte
	)

	for {
		if _, err := io.ReadFull(r, lenB[:]); err != nil {
			if errors.Is(err, io.EOF) && len(cmds) == 0 {
				return nil, caps, io.EOF
			}
			return nil, nil, fmt.Errorf("read pkt-line length: %w", err)
		}
		pktLen, err := strconv.ParseInt(string(lenB[:]), 16, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid pkt-line length %q: %w", string(lenB[:]), err)
		}
		if pktLen == 0 {
			return cmds, caps, nil
		}
		if len(cmds) >= MaxReceiveCommands {
			return cmds, caps, fmt.Errorf("%w: exceeds max %d", ErrTooManyReceiveCommands, MaxReceiveCommands)
		}
		if pktLen < 4 || pktLen > maxPktLineLen {
			return nil, nil, fmt.Errorf("pkt-line length %d out of range", pktLen)
		}
		if pktLen > MaxReceiveCommandLineLen {
			return nil, nil, fmt.Errorf("%w: length %d exceeds max %d", ErrReceiveCommandLineTooLong, pktLen, MaxReceiveCommandLineLen)
		}

		payload := make([]byte, pktLen-4)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, nil, fmt.Errorf("read pkt-line payload: %w", err)
		}

		line := strings.TrimSuffix(string(payload), "\n")
		if len(cmds) == 0 {
			if mainPart, capPart, hasNUL := strings.Cut(line, "\x00"); hasNUL {
				line = mainPart
				for _, c := range strings.Fields(capPart) {
					caps[c] = true
				}
			}
		}

		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 {
			return nil, nil, fmt.Errorf("malformed ref update line %q", line)
		}
		oldOID, newOID, refName := fields[0], fields[1], fields[2]
		if err := ddb.ValidateOID(oldOID); err != nil {
			return nil, nil, fmt.Errorf("invalid old_oid %q: %w", oldOID, err)
		}
		if err := ddb.ValidateOID(newOID); err != nil {
			return nil, nil, fmt.Errorf("invalid new_oid %q: %w", newOID, err)
		}
		if len(refName) > MaxRefNameLen {
			return nil, nil, fmt.Errorf("%w: length %d exceeds max %d", ErrRefNameTooLong, len(refName), MaxRefNameLen)
		}
		if !isSafeRefName(refName) {
			return nil, nil, fmt.Errorf("unsafe ref name %q", refName)
		}

		cmds = append(cmds, ddb.RefUpdate{
			RefName: refName,
			OldOID:  oldOID,
			NewOID:  newOID,
		})
	}
}

func uploadPackCapabilities(agent string, hasHEAD bool, defaultBranch string, objFormat ddb.ObjectFormat) string {
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
	if objFormat == ddb.ObjectFormatSHA256 {
		caps = append(caps, "object-format=sha256")
	}
	if hasHEAD {
		caps = append(caps, "symref=HEAD:"+defaultBranch)
	}
	caps = append(caps, "agent="+agent)
	return strings.Join(caps, " ")
}
