package node

import (
	"bytes"
	"fmt"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"stravin.dev/core/internal/rpc"
)

const (
	emptyTreeSHA1   = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
	emptyTreeSHA256 = "6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321"
)

type rawDiffEntry struct {
	OldMode    string
	NewMode    string
	OldBlobOID string
	NewBlobOID string
	ChangeKind string
	OldPath    string
	Path       string
}

func isZeroOID(oid string) bool {
	if oid == "" {
		return true
	}
	for i := 0; i < len(oid); i++ {
		if oid[i] != '0' {
			return false
		}
	}
	return true
}

// parseRawDiffTree parses the NUL-delimited output of
// `git diff-tree -r -M -z --no-abbrev --raw <base> <head>`.
func parseRawDiffTree(data []byte) ([]rawDiffEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	tokens := bytes.Split(data, []byte{0})
	entries := make([]rawDiffEntry, 0)
	for i := 0; i < len(tokens); {
		tok := tokens[i]
		if len(tok) == 0 {
			i++
			continue
		}
		// Header token begins with ':' followed by:
		// <old_mode> <new_mode> <old_oid> <new_oid> <status>
		if tok[0] != ':' {
			return nil, fmt.Errorf("gitdata: unexpected diff-tree raw token %q", string(tok))
		}
		fields := strings.Fields(string(tok[1:]))
		if len(fields) < 5 {
			return nil, fmt.Errorf("gitdata: malformed diff-tree header %q", string(tok))
		}
		oldMode, newMode, oldOID, newOID, statusCode := fields[0], fields[1], fields[2], fields[3], fields[4]
		if isZeroOID(oldOID) {
			oldOID = ""
		}
		if isZeroOID(newOID) {
			newOID = ""
		}
		i++
		if i >= len(tokens) {
			return nil, fmt.Errorf("gitdata: missing path for diff-tree entry %q", statusCode)
		}

		entry := rawDiffEntry{
			OldMode:    oldMode,
			NewMode:    newMode,
			OldBlobOID: oldOID,
			NewBlobOID: newOID,
		}
		switch statusCode[0] {
		case 'A':
			entry.ChangeKind = "add"
			entry.Path = string(tokens[i])
			i++
		case 'D':
			entry.ChangeKind = "delete"
			entry.Path = string(tokens[i])
			i++
		case 'M', 'T':
			entry.ChangeKind = "modify"
			entry.Path = string(tokens[i])
			i++
		case 'R':
			entry.ChangeKind = "rename"
			entry.OldPath = string(tokens[i])
			i++
			if i >= len(tokens) {
				return nil, fmt.Errorf("gitdata: missing destination path for rename %q", entry.OldPath)
			}
			entry.Path = string(tokens[i])
			i++
		case 'C':
			entry.ChangeKind = "add"
			entry.OldPath = string(tokens[i])
			i++
			if i >= len(tokens) {
				return nil, fmt.Errorf("gitdata: missing destination path for copy %q", entry.OldPath)
			}
			entry.Path = string(tokens[i])
			i++
		default:
			entry.ChangeKind = "modify"
			entry.Path = string(tokens[i])
			i++
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// parseUnifiedPatch parses unified diff output into structured DiffHunks and
// counts total additions and deletions.
func parseUnifiedPatch(patch []byte) ([]rpc.DiffHunk, uint32, uint32) {
	if len(patch) == 0 {
		return []rpc.DiffHunk{}, 0, 0
	}
	lines := strings.Split(string(patch), "\n")
	// Drop trailing empty element produced by final newline.
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	var (
		hunks     = make([]rpc.DiffHunk, 0)
		curHunk   *rpc.DiffHunk
		oldLine   uint32
		newLine   uint32
		additions uint32
		deletions uint32
	)

	for _, line := range lines {
		if strings.HasPrefix(line, "@@ ") {
			os, ol, ns, nl, ok := parseHunkHeader(line)
			if !ok {
				continue
			}
			hunks = append(hunks, rpc.DiffHunk{
				OldStart: os,
				OldLines: ol,
				NewStart: ns,
				NewLines: nl,
				Header:   line,
				Lines:    make([]rpc.DiffLine, 0),
			})
			curHunk = &hunks[len(hunks)-1]
			oldLine = os
			newLine = ns
			continue
		}
		if curHunk == nil {
			continue
		}
		if strings.HasPrefix(line, "diff --git ") {
			curHunk = nil
			continue
		}
		if strings.HasPrefix(line, "\\ No newline at end of file") {
			continue
		}
		if len(line) == 0 {
			curHunk.Lines = append(curHunk.Lines, rpc.DiffLine{
				Kind:    "context",
				Content: "",
				OldLine: oldLine,
				NewLine: newLine,
			})
			oldLine++
			newLine++
			continue
		}
		switch line[0] {
		case '+':
			curHunk.Lines = append(curHunk.Lines, rpc.DiffLine{
				Kind:    "add",
				Content: line[1:],
				NewLine: newLine,
			})
			newLine++
			additions++
		case '-':
			curHunk.Lines = append(curHunk.Lines, rpc.DiffLine{
				Kind:    "delete",
				Content: line[1:],
				OldLine: oldLine,
			})
			oldLine++
			deletions++
		case ' ':
			curHunk.Lines = append(curHunk.Lines, rpc.DiffLine{
				Kind:    "context",
				Content: line[1:],
				OldLine: oldLine,
				NewLine: newLine,
			})
			oldLine++
			newLine++
		}
	}
	return hunks, additions, deletions
}

// parseHunkHeader parses "@@ -oldStart[,oldLines] +newStart[,newLines] @@..."
func parseHunkHeader(header string) (oldStart, oldLines, newStart, newLines uint32, ok bool) {
	rest, found := strings.CutPrefix(header, "@@ ")
	if !found {
		return 0, 0, 0, 0, false
	}
	endIdx := strings.Index(rest, " @@")
	if endIdx < 0 {
		return 0, 0, 0, 0, false
	}
	parts := strings.Fields(rest[:endIdx])
	if len(parts) < 2 {
		return 0, 0, 0, 0, false
	}
	os, ol, ok1 := parseHunkRange(strings.TrimPrefix(parts[0], "-"))
	ns, nl, ok2 := parseHunkRange(strings.TrimPrefix(parts[1], "+"))
	if !ok1 || !ok2 {
		return 0, 0, 0, 0, false
	}
	return os, ol, ns, nl, true
}

func parseHunkRange(spec string) (start, count uint32, ok bool) {
	if s, c, hasComma := strings.Cut(spec, ","); hasComma {
		sv, err1 := strconv.ParseUint(s, 10, 32)
		cv, err2 := strconv.ParseUint(c, 10, 32)
		if err1 != nil || err2 != nil {
			return 0, 0, false
		}
		return uint32(sv), uint32(cv), true
	}
	sv, err := strconv.ParseUint(spec, 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return uint32(sv), 1, true
}

// parseLsTreeOutput parses NUL-delimited `git ls-tree -l -z` output into TreeEntry items.
func parseLsTreeOutput(data []byte, basePath string) ([]rpc.TreeEntry, error) {
	if len(data) == 0 {
		return []rpc.TreeEntry{}, nil
	}
	records := bytes.Split(data, []byte{0})
	entries := make([]rpc.TreeEntry, 0, len(records))
	for _, rec := range records {
		if len(rec) == 0 {
			continue
		}
		metaPart, namePart, ok := bytes.Cut(rec, []byte{'\t'})
		if !ok {
			return nil, fmt.Errorf("gitdata: malformed ls-tree record %q", string(rec))
		}
		fields := strings.Fields(string(metaPart))
		if len(fields) < 4 {
			return nil, fmt.Errorf("gitdata: malformed ls-tree metadata %q", string(metaPart))
		}
		var size uint64
		if fields[3] != "-" {
			parsed, err := strconv.ParseUint(fields[3], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("gitdata: parse ls-tree size %q: %w", fields[3], err)
			}
			size = parsed
		}
		relPath := string(namePart)
		fullPath := relPath
		if basePath != "" {
			fullPath = basePath + "/" + relPath
		}
		entries = append(entries, rpc.TreeEntry{
			Name:      path.Base(fullPath),
			Path:      fullPath,
			Mode:      fields[0],
			Type:      fields[1],
			OID:       fields[2],
			SizeBytes: size,
		})
	}
	return entries, nil
}

// detectContentMetadata determines whether sample is binary and resolves a clean MIME type.
func detectContentMetadata(filePath string, sample []byte) (mimeType string, isBinary bool) {
	hasNul := bytes.IndexByte(sample, 0) >= 0
	sniffed := ""
	if len(sample) > 0 {
		rawSniff := http.DetectContentType(sample)
		if mediaType, _, err := mime.ParseMediaType(rawSniff); err == nil {
			sniffed = mediaType
		} else {
			sniffed = rawSniff
		}
	}

	isBinary = hasNul || isBinaryMediaType(sniffed)
	if !isBinary && len(sample) > 0 && sniffed == "application/octet-stream" && !utf8.Valid(sample) {
		isBinary = true
	}

	if isBinary {
		if sniffed != "" && sniffed != "application/octet-stream" && !strings.HasPrefix(sniffed, "text/") {
			return sniffed, true
		}
		if extType := extMimeType(filePath); extType != "" && !strings.HasPrefix(extType, "text/") {
			return extType, true
		}
		return "application/octet-stream", true
	}

	if extType := extMimeType(filePath); extType != "" {
		return extType, false
	}
	if sniffed != "" {
		return sniffed, false
	}
	return "text/plain", false
}

func isBinaryMediaType(mt string) bool {
	if mt == "" {
		return false
	}
	if strings.HasPrefix(mt, "image/") && mt != "image/svg+xml" {
		return true
	}
	if strings.HasPrefix(mt, "audio/") || strings.HasPrefix(mt, "video/") {
		return true
	}
	switch mt {
	case "application/pdf", "application/zip", "application/x-gzip", "application/gzip",
		"application/wasm", "application/x-rar-compressed", "application/x-7z-compressed",
		"application/vnd.ms-fontobject", "font/woff", "font/woff2", "font/ttf", "font/otf":
		return true
	}
	return false
}

var codeExtMimeTypes = map[string]string{
	".go":    "text/x-go",
	".rs":    "text/x-rust",
	".py":    "text/x-python",
	".ts":    "text/typescript",
	".tsx":   "text/typescript",
	".js":    "text/javascript",
	".jsx":   "text/javascript",
	".md":    "text/markdown",
	".json":  "application/json",
	".yaml":  "application/yaml",
	".yml":   "application/yaml",
	".toml":  "application/toml",
	".proto": "text/x-protobuf",
	".sh":    "text/x-shellscript",
	".sql":   "text/x-sql",
	".css":   "text/css",
	".html":  "text/html",
	".htm":   "text/html",
	".xml":   "application/xml",
	".svg":   "image/svg+xml",
	".txt":   "text/plain",
	".c":     "text/x-c",
	".h":     "text/x-c",
	".cc":    "text/x-c++",
	".cpp":   "text/x-c++",
	".java":  "text/x-java",
	".rb":    "text/x-ruby",
	".png":   "image/png",
	".jpg":   "image/jpeg",
	".jpeg":  "image/jpeg",
	".gif":   "image/gif",
	".webp":  "image/webp",
	".wasm":  "application/wasm",
	".pdf":   "application/pdf",
}

func extMimeType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == "" {
		return ""
	}
	if mt, ok := codeExtMimeTypes[ext]; ok {
		return mt
	}
	raw := mime.TypeByExtension(ext)
	if raw == "" {
		return ""
	}
	if mediaType, _, err := mime.ParseMediaType(raw); err == nil {
		return mediaType
	}
	return raw
}

func sanitizeTreePath(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if strings.ContainsAny(raw, "\x00\r\n\\") {
		return "", fmt.Errorf("invalid tree path %q", raw)
	}
	trimmed := strings.Trim(raw, "/")
	if trimmed == "" {
		return "", nil
	}
	if strings.HasPrefix(trimmed, "-") {
		return "", fmt.Errorf("invalid tree path %q", raw)
	}
	for _, seg := range strings.Split(trimmed, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("invalid tree path segment in %q", raw)
		}
	}
	return trimmed, nil
}

func validateRefPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	if len(prefix) > 1024 || strings.HasPrefix(prefix, "-") || strings.ContainsAny(prefix, "\x00\r\n\\ \t") || strings.Contains(prefix, "..") {
		return fmt.Errorf("invalid ref prefix %q", prefix)
	}
	return nil
}

func isSafeRefOrOID(ref string) bool {
	if ref == "" || len(ref) > 1024 || strings.HasPrefix(ref, "-") {
		return false
	}
	if strings.ContainsAny(ref, "\x00\r\n\\ \t") {
		return false
	}
	if strings.Contains(ref, "..") || strings.Contains(ref, "@{") {
		return false
	}
	return true
}

func normalizeDefaultBranch(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "refs/") {
		return trimmed
	}
	return "refs/heads/" + trimmed
}

func classifyRef(refName string) (kind, shortName string) {
	if s, ok := strings.CutPrefix(refName, "refs/heads/"); ok {
		return "branch", s
	}
	if s, ok := strings.CutPrefix(refName, "refs/tags/"); ok {
		return "tag", s
	}
	return "other", strings.TrimPrefix(refName, "refs/")
}

func shortOID(oid string) string {
	if len(oid) > 7 {
		return oid[:7]
	}
	return oid
}

const commitLogFieldsPerRecord = 11

// parseCommitLogOutput parses NUL-delimited `git log -z` output using commitLogFormat.
func parseCommitLogOutput(data []byte) ([]rpc.CommitEntry, error) {
	if len(data) == 0 {
		return []rpc.CommitEntry{}, nil
	}
	trimmed := bytes.TrimSuffix(data, []byte{0})
	fields := bytes.Split(trimmed, []byte{0})
	if len(fields)%commitLogFieldsPerRecord != 0 {
		return nil, fmt.Errorf("gitdata: malformed commit log output (%d fields)", len(fields))
	}
	count := len(fields) / commitLogFieldsPerRecord
	commits := make([]rpc.CommitEntry, 0, count)
	for i := 0; i < len(fields); i += commitLogFieldsPerRecord {
		oid := string(fields[i])
		parents := strings.Fields(string(fields[i+2]))
		if parents == nil {
			parents = []string{}
		}
		commits = append(commits, rpc.CommitEntry{
			OID:            oid,
			ShortOID:       shortOID(oid),
			TreeOID:        string(fields[i+1]),
			ParentOIDs:     parents,
			AuthorName:     string(fields[i+3]),
			AuthorEmail:    string(fields[i+4]),
			AuthorTime:     string(fields[i+5]),
			CommitterName:  string(fields[i+6]),
			CommitterEmail: string(fields[i+7]),
			CommitterTime:  string(fields[i+8]),
			Subject:        string(fields[i+9]),
			Body:           strings.TrimRight(string(fields[i+10]), "\r\n"),
		})
	}
	return commits, nil
}

// extractCommitSignature extracts the raw GPG/SSH signature armor and exact
// signed commit payload (with the gpgsig header removed) from a raw Git commit object.
func extractCommitSignature(raw []byte) (rawSig, signedPayload string) {
	if len(raw) == 0 {
		return "", ""
	}
	var headerSection, bodySection []byte
	if idx := bytes.Index(raw, []byte("\n\n")); idx >= 0 {
		headerSection = raw[:idx+1]
		bodySection = raw[idx+1:]
	} else {
		headerSection = raw
	}

	var (
		sigBuf     bytes.Buffer
		payloadBuf bytes.Buffer
		inSig      bool
		foundSig   bool
	)

	rem := headerSection
	for len(rem) > 0 {
		nl := bytes.IndexByte(rem, '\n')
		var line []byte
		if nl >= 0 {
			line = rem[:nl+1]
			rem = rem[nl+1:]
		} else {
			line = rem
			rem = nil
		}

		if inSig {
			if len(line) > 0 && line[0] == ' ' {
				sigBuf.Write(line[1:])
				continue
			}
			inSig = false
		}

		if after, ok := bytes.CutPrefix(line, []byte("gpgsig ")); ok {
			foundSig = true
			inSig = true
			sigBuf.Write(after)
			continue
		}
		if after, ok := bytes.CutPrefix(line, []byte("gpgsig-sha256 ")); ok {
			foundSig = true
			inSig = true
			sigBuf.Write(after)
			continue
		}

		payloadBuf.Write(line)
	}

	if !foundSig {
		return "", ""
	}
	payloadBuf.Write(bodySection)
	return strings.TrimRight(sigBuf.String(), "\n"), payloadBuf.String()
}

// parseCommitSummaryOutput parses a single NUL-delimited `git log -z -n 1` summary record.
func parseCommitSummaryOutput(data []byte) (rpc.CommitSummary, bool) {
	if len(data) == 0 {
		return rpc.CommitSummary{}, false
	}
	trimmed := bytes.TrimSuffix(data, []byte{0})
	fields := bytes.Split(trimmed, []byte{0})
	if len(fields) < 4 {
		return rpc.CommitSummary{}, false
	}
	oid := string(fields[0])
	if oid == "" {
		return rpc.CommitSummary{}, false
	}
	return rpc.CommitSummary{
		OID:        oid,
		ShortOID:   shortOID(oid),
		AuthorName: string(fields[1]),
		AuthorTime: string(fields[2]),
		Subject:    string(fields[3]),
	}, true
}

// parseNameOnlyLogOutput maps each immediate directory entry in wanted to its
// latest touching CommitSummary from `git log -z --name-only` output.
func parseNameOnlyLogOutput(data []byte, basePath string, wanted map[string]struct{}) map[string]rpc.CommitSummary {
	found := make(map[string]rpc.CommitSummary, len(wanted))
	if len(data) == 0 || len(wanted) == 0 {
		return found
	}
	records := bytes.Split(data, []byte{0x1e})
	for _, rec := range records {
		if len(rec) == 0 {
			continue
		}
		fields := bytes.Split(rec, []byte{0})
		if len(fields) < 5 {
			continue
		}
		oid := string(fields[0])
		if oid == "" {
			continue
		}
		summary := rpc.CommitSummary{
			OID:        oid,
			ShortOID:   shortOID(oid),
			AuthorName: string(fields[1]),
			AuthorTime: string(fields[2]),
			Subject:    string(fields[3]),
		}
		for _, rawPath := range fields[4:] {
			p := strings.TrimLeft(string(rawPath), "\r\n")
			if p == "" {
				continue
			}
			entryName := immediateChildEntry(p, basePath)
			if entryName == "" {
				continue
			}
			if _, ok := wanted[entryName]; !ok {
				continue
			}
			if _, exists := found[entryName]; !exists {
				found[entryName] = summary
				if len(found) == len(wanted) {
					return found
				}
			}
		}
	}
	return found
}

func immediateChildEntry(filePath, basePath string) string {
	rel := filePath
	if basePath != "" {
		prefix := basePath + "/"
		if !strings.HasPrefix(filePath, prefix) {
			return ""
		}
		rel = strings.TrimPrefix(filePath, prefix)
	}
	if rel == "" {
		return ""
	}
	if slash := strings.IndexByte(rel, '/'); slash >= 0 {
		return rel[:slash]
	}
	return rel
}
