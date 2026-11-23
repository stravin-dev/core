package rpc

import "strings"

const (
	// MaxGitProtocolLen caps the byte length of the untrusted Git-Protocol value
	// accepted at the stravin-edge boundary and propagated over internal gRPC.
	MaxGitProtocolLen = 128

	// MaxGitProtocolTokens caps the number of colon-separated tokens in Git-Protocol.
	MaxGitProtocolTokens = 8

	// HeaderGitProtocol is the internal gRPC metadata key carrying the sanitized
	// Git-Protocol / GIT_PROTOCOL value from stravin-edge to stravin-node.
	HeaderGitProtocol = "x-stravin-git-protocol"
)

// SanitizeGitProtocol strictly validates an untrusted Git-Protocol header or
// SSH GIT_PROTOCOL environment value against an explicit token allowlist.
//
// It enforces MaxGitProtocolLen, rejects control characters, whitespace, NUL
// bytes, and empty segments, and permits only canonical colon-separated Git
// wire protocol tokens:
//   - version=0 | version=1 | version=2
//   - object-format=sha1 | object-format=sha256
//   - agent=<safe-token> where bytes are in [A-Za-z0-9._/-]
//
// It returns the canonical sanitized string and true on success, or ("", false)
// when the input is empty or malformed.
func SanitizeGitProtocol(raw string) (string, bool) {
	if raw == "" || len(raw) > MaxGitProtocolLen {
		return "", false
	}
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b <= 0x20 || b >= 0x7f {
			return "", false
		}
	}

	parts := strings.Split(raw, ":")
	if len(parts) == 0 || len(parts) > MaxGitProtocolTokens {
		return "", false
	}

	var (
		seenVersion      bool
		seenObjectFormat bool
		seenAgent        bool
		validated        = make([]string, 0, len(parts))
	)

	for _, tok := range parts {
		if tok == "" {
			return "", false
		}
		switch {
		case tok == "version=0" || tok == "version=1" || tok == "version=2":
			if seenVersion {
				return "", false
			}
			seenVersion = true
			validated = append(validated, tok)

		case tok == "object-format=sha1" || tok == "object-format=sha256":
			if seenObjectFormat {
				return "", false
			}
			seenObjectFormat = true
			validated = append(validated, tok)

		case strings.HasPrefix(tok, "agent="):
			if seenAgent {
				return "", false
			}
			val := strings.TrimPrefix(tok, "agent=")
			if !isSafeAgentValue(val) {
				return "", false
			}
			seenAgent = true
			validated = append(validated, tok)

		default:
			return "", false
		}
	}

	return strings.Join(validated, ":"), true
}

// GitProtocolVersion extracts the negotiated protocol version ("0", "1", or "2")
// from a sanitized Git-Protocol string, or "" when no version token is present.
func GitProtocolVersion(gitProtocol string) string {
	clean, ok := SanitizeGitProtocol(gitProtocol)
	if !ok {
		return ""
	}
	for _, tok := range strings.Split(clean, ":") {
		if v, found := strings.CutPrefix(tok, "version="); found {
			return v
		}
	}
	return ""
}

func isSafeAgentValue(v string) bool {
	if v == "" {
		return false
	}
	for i := 0; i < len(v); i++ {
		b := v[i]
		isAlphaNum := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
		isPunct := b == '.' || b == '_' || b == '/' || b == '-'
		if !isAlphaNum && !isPunct {
			return false
		}
	}
	return true
}
