package node

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
)

const (
	maxV2LsRefsReqBytes = 1 << 20 // 1 MiB cap for buffered v2 ls-refs requests
)

type v2CommandKind uint8

const (
	v2CmdOther v2CommandKind = iota
	v2CmdLsRefs
)

type v2LsRefsRequest struct {
	Peel        bool
	Symrefs     bool
	Unborn      bool
	RefPrefixes []string
}

// readBoundedV2Request reads frames from a stateless-RPC Protocol v2 stream
// to classify the command and buffer complete ls-refs (<= 1 MiB) requests.
// The returned bytes are always safe to replay verbatim into git-upload-pack
// stdin if the fast path declines to handle the request.
func readBoundedV2Request(stream rpc.GitWireStream) (buf []byte, completeEOF bool, kind v2CommandKind, err error) {
	var classified bool
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			if !classified {
				kind = classifyV2FirstPktLine(buf)
			}
			return buf, true, kind, nil
		}
		if recvErr != nil {
			return nil, false, v2CmdOther, recvErr
		}
		if len(frame.Data) > 0 {
			buf = append(buf, frame.Data...)
		}
		if frame.EOF {
			if !classified {
				kind = classifyV2FirstPktLine(buf)
			}
			return buf, true, kind, nil
		}
		if !classified && len(buf) >= 4 {
			pktLen, parseErr := strconv.ParseInt(string(buf[:4]), 16, 32)
			if parseErr != nil || pktLen < 4 || pktLen > maxV2PeekPktLen {
				return buf, false, v2CmdOther, nil
			}
			if len(buf) >= int(pktLen) {
				classified = true
				kind = classifyV2FirstPktLine(buf[:pktLen])
				if kind != v2CmdLsRefs {
					return buf, false, v2CmdOther, nil
				}
			}
		}
		if classified && len(buf) > maxV2LsRefsReqBytes {
			return buf, false, kind, nil
		}
	}
}

func classifyV2FirstPktLine(buf []byte) v2CommandKind {
	if len(buf) < 4 {
		return v2CmdOther
	}
	pktLen, err := strconv.ParseInt(string(buf[:4]), 16, 32)
	if err != nil || pktLen < 4 || int(pktLen) > len(buf) {
		return v2CmdOther
	}
	payload := strings.TrimSuffix(string(buf[4:pktLen]), "\n")
	if payload == "command=ls-refs" {
		return v2CmdLsRefs
	}
	return v2CmdOther
}

// nextPktLine reads one Git pkt-line from raw starting at pos.
// For special packets:
//   - "0000" (flush-pkt): pktLen=0, nextPos=pos+4, ok=true
//   - "0001" (delim-pkt): pktLen=1, nextPos=pos+4, ok=true
//   - "0002" (response-end-pkt): pktLen=2, nextPos=pos+4, ok=true
//
// For data packets (pktLen >= 4): returns payload bytes without the 4-byte length prefix.
func nextPktLine(raw []byte, pos int) (pktLen int, payload []byte, nextPos int, ok bool) {
	if pos+4 > len(raw) {
		return 0, nil, pos, false
	}
	n, err := strconv.ParseInt(string(raw[pos:pos+4]), 16, 32)
	if err != nil || n < 0 || n > maxPktLineLen {
		return 0, nil, pos, false
	}
	if n < 4 {
		return int(n), nil, pos + 4, true
	}
	end := pos + int(n)
	if end > len(raw) {
		return 0, nil, pos, false
	}
	return int(n), raw[pos+4 : end], end, true
}

// parseV2LsRefsRequest parses a complete Git Protocol v2 `command=ls-refs`
// request. It returns ok=false on any unrecognized argument, malformed
// pkt-line, missing flush-pkt, or trailing bytes so the caller falls back
// cleanly to git-upload-pack.
func parseV2LsRefsRequest(raw []byte) (v2LsRefsRequest, bool) {
	pktLen, payload, pos, ok := nextPktLine(raw, 0)
	if !ok || pktLen < 4 || strings.TrimSuffix(string(payload), "\n") != "command=ls-refs" {
		return v2LsRefsRequest{}, false
	}

	// Phase 1: capabilities until delim-pkt ("0001") or flush-pkt ("0000").
	sawDelim := false
	for pos < len(raw) {
		pktLen, _, nextPos, ok := nextPktLine(raw, pos)
		if !ok {
			return v2LsRefsRequest{}, false
		}
		pos = nextPos
		if pktLen == 0 {
			return v2LsRefsRequest{}, pos == len(raw)
		}
		if pktLen == 1 {
			sawDelim = true
			break
		}
		if pktLen < 4 {
			return v2LsRefsRequest{}, false
		}
	}
	if !sawDelim {
		return v2LsRefsRequest{}, false
	}

	// Phase 2: command arguments until flush-pkt ("0000").
	var req v2LsRefsRequest
	for pos < len(raw) {
		pktLen, payload, nextPos, ok := nextPktLine(raw, pos)
		if !ok {
			return v2LsRefsRequest{}, false
		}
		pos = nextPos
		if pktLen == 0 {
			return req, pos == len(raw)
		}
		if pktLen < 4 {
			return v2LsRefsRequest{}, false
		}
		line := strings.TrimSuffix(string(payload), "\n")
		switch line {
		case "peel":
			req.Peel = true
		case "symrefs":
			req.Symrefs = true
		case "unborn":
			req.Unborn = true
		default:
			prefix, hasPrefix := strings.CutPrefix(line, "ref-prefix ")
			if !hasPrefix {
				return v2LsRefsRequest{}, false
			}
			req.RefPrefixes = append(req.RefPrefixes, prefix)
		}
	}
	return v2LsRefsRequest{}, false
}

// encodeV2LsRefsResponse encodes a Git Protocol v2 `ls-refs` response strictly
// from DynamoDB RefState records. If peel was requested and any matching ref
// lives under refs/tags/, it returns needsFallback=true so git-upload-pack
// supplies exact peeled:<oid> attributes for annotated tags.
func encodeV2LsRefsResponse(refs []ddb.RefState, defaultBranch string, req v2LsRefsRequest) (payload []byte, needsFallback bool) {
	headTarget := strings.TrimSpace(defaultBranch)
	if headTarget == "" {
		headTarget = defaultBranchRef
	}

	var (
		headOID string
		matched = make([]ddb.RefState, 0, len(refs))
	)
	for _, r := range refs {
		if r.RefName == "" || r.OID == "" || r.RefName == "HEAD" {
			continue
		}
		if r.RefName == headTarget {
			headOID = r.OID
		}
		if !matchesAnyRefPrefix(r.RefName, req.RefPrefixes) {
			continue
		}
		if req.Peel && strings.HasPrefix(r.RefName, "refs/tags/") {
			return nil, true
		}
		matched = append(matched, r)
	}

	if !sort.SliceIsSorted(matched, func(i, j int) bool {
		return matched[i].RefName < matched[j].RefName
	}) {
		sort.Slice(matched, func(i, j int) bool {
			return matched[i].RefName < matched[j].RefName
		})
	}

	var buf bytes.Buffer
	if matchesAnyRefPrefix("HEAD", req.RefPrefixes) {
		if headOID != "" {
			if req.Symrefs {
				buf.Write(EncodePktLine([]byte(fmt.Sprintf("%s HEAD symref-target:%s\n", headOID, headTarget))))
			} else {
				buf.Write(EncodePktLine([]byte(fmt.Sprintf("%s HEAD\n", headOID))))
			}
		} else if req.Unborn {
			if req.Symrefs {
				buf.Write(EncodePktLine([]byte(fmt.Sprintf("unborn HEAD symref-target:%s\n", headTarget))))
			} else {
				buf.Write(EncodePktLine([]byte("unborn HEAD\n")))
			}
		}
	}

	for _, r := range matched {
		buf.Write(EncodePktLine([]byte(r.OID + " " + r.RefName + "\n")))
	}
	buf.Write(FlushPkt)
	return buf.Bytes(), false
}

func matchesAnyRefPrefix(refName string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if strings.HasPrefix(refName, p) {
			return true
		}
	}
	return false
}
