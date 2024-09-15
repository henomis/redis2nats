package nats

// matchPattern checks if the string 's' matches the pattern 'pattern'.
func matchPattern(s string, pattern string) bool {
	si, pi := 0, 0
	sLen, pLen := len(s), len(pattern)

	for si < sLen || pi < pLen {
		if pi < pLen {
			switch pattern[pi] {
			case '?': // Match exactly one character
				if si >= sLen {
					return false
				}
				si++
				pi++
			case '*': // Match zero or more characters
				// Try to match as much as possible
				if pi == pLen-1 {
					return true // '*' at the end matches everything
				}
				pi++
				for si <= sLen {
					if matchPattern(s[si:], pattern[pi:]) {
						return true
					}
					si++
				}
				return false
			case '[': // Handle character sets or ranges
				pi++
				notSet := false
				if pi < pLen && pattern[pi] == '^' {
					notSet = true // Negation inside the brackets
					pi++
				}

				matched := false
				inRange := false
				for pi < pLen && pattern[pi] != ']' {
					if pi+2 < pLen && pattern[pi+1] == '-' && pattern[pi+2] != ']' {
						// Handle range like [a-z]
						if s[si] >= pattern[pi] && s[si] <= pattern[pi+2] {
							inRange = true
						}
						pi += 3
					} else {
						// Single character inside the set
						if s[si] == pattern[pi] {
							inRange = true
						}
						pi++
					}
				}
				if pi < pLen && pattern[pi] == ']' {
					pi++
				}
				matched = (notSet && !inRange) || (!notSet && inRange)
				if !matched {
					return false
				}
				si++
			default: // Exact character match
				if si >= sLen || s[si] != pattern[pi] {
					return false
				}
				si++
				pi++
			}
		} else {
			return false
		}
	}
	return si == sLen && pi == pLen
}
