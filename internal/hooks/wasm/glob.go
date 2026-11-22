package wasm

// MatchRefGlob reports whether ref matches a Git ref glob pattern.
//
// Supported wildcards:
//   - "" or "*" or "**" matches any ref
//   - "**" matches zero or more characters including '/'
//   - "*" matches zero or more characters except '/'
//   - "?" matches any single character except '/'
func MatchRefGlob(pattern, ref string) bool {
	if pattern == "" || pattern == "*" || pattern == "**" {
		return true
	}
	return matchGlobBytes(pattern, ref)
}

func matchGlobBytes(pattern, text string) bool {
	pLen, tLen := len(pattern), len(text)
	// prev[j] reports whether pattern[:i] matches text[:j].
	prev := make([]bool, tLen+1)
	curr := make([]bool, tLen+1)
	prev[0] = true

	for i := 0; i < pLen; i++ {
		clear(curr)
		switch {
		case pattern[i] == '*' && i+1 < pLen && pattern[i+1] == '*':
			// "**" matches zero or more characters including '/'.
			i++
			matched := false
			for j := 0; j <= tLen; j++ {
				if prev[j] {
					matched = true
				}
				curr[j] = matched
			}
		case pattern[i] == '*':
			// "*" matches zero or more non-'/' characters.
			matched := false
			for j := 0; j <= tLen; j++ {
				if j > 0 && text[j-1] == '/' {
					matched = false
				}
				if prev[j] {
					matched = true
				}
				curr[j] = matched
			}
		default:
			for j := 1; j <= tLen; j++ {
				if !prev[j-1] {
					continue
				}
				if pattern[i] == '?' {
					curr[j] = text[j-1] != '/'
				} else {
					curr[j] = pattern[i] == text[j-1]
				}
			}
		}
		prev, curr = curr, prev
	}

	return prev[tLen]
}

func filterMatchingUpdates(pattern string, updates []RefUpdate) []RefUpdate {
	matched := make([]RefUpdate, 0, len(updates))
	for _, u := range updates {
		if MatchRefGlob(pattern, u.Ref) {
			matched = append(matched, u)
		}
	}
	return matched
}
