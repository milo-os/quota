package enforcement

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// nameHashLen is the number of hex characters of the original name's hash
// appended when a name has to be rewritten.
const nameHashLen = 10

// sanitizeObjectName returns name unchanged when it is a valid DNS subdomain
// of at most maxLen characters. Otherwise it lowercases it, replaces
// characters a subdomain cannot hold with '-', and appends a hash of the
// original name, truncating so the result fits maxLen. The hash keeps
// distinct inputs that sanitize to the same text (for example "Web" and
// "web", or two long names sharing a prefix) distinct.
func sanitizeObjectName(name string, maxLen int) string {
	if len(name) <= maxLen && len(validation.IsDNS1123Subdomain(name)) == 0 {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	hash := hex.EncodeToString(sum[:])[:nameHashLen]

	body := subdomainChars(name)
	if limit := maxLen - nameHashLen - 1; len(body) > limit {
		body = trimToSubdomain(body[:limit])
	}
	if body == "" {
		return hash
	}
	return body + "-" + hash
}

// sanitizeGenerateName makes prefix safe to pass to a name generator that
// appends random lowercase alphanumerics. A prefix that is already valid is
// kept; otherwise it is lowercased and every character outside [a-z0-9-]
// becomes '-'. The generator truncates long prefixes itself.
func sanitizeGenerateName(prefix string) string {
	if len(validation.IsDNS1123Subdomain(prefix+"x")) == 0 {
		return prefix
	}
	var b strings.Builder
	for _, r := range strings.ToLower(prefix) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.TrimLeft(b.String(), "-")
}

// subdomainChars lowercases s, maps characters outside [a-z0-9.-] to '-',
// and trims each dot-separated label so it starts and ends alphanumeric,
// dropping labels left empty.
func subdomainChars(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return trimToSubdomain(b.String())
}

// trimToSubdomain trims '-' from both ends of each label of an already
// character-safe string and drops empty labels.
func trimToSubdomain(s string) string {
	labels := strings.Split(s, ".")
	out := labels[:0]
	for _, l := range labels {
		if l = strings.Trim(l, "-"); l != "" {
			out = append(out, l)
		}
	}
	return strings.Join(out, ".")
}

// ClaimName returns the QuotaClaim name admission uses for a rendered,
// pinned claim name: name itself when it is a valid DNS subdomain, otherwise
// a sanitized form with a hash of name appended. Anything else that derives
// claim names from ClaimCreationPolicy templates must use it so both address
// the same claim.
func ClaimName(name string) string {
	return sanitizeObjectName(name, validation.DNS1123SubdomainMaxLength)
}
