package scheduler

import (
	"regexp"
	"strings"
)

const (
	HashcatUnsupportedReasonModuleMissing     = "hashcat_module_missing"
	HashcatUnsupportedReasonModeUnsupported   = "hashcat_mode_unsupported"
	HashcatUnsupportedReasonAttackUnsupported = "hashcat_attack_mode_unsupported"
	HashcatUnsupportedReasonTokenShape        = "hashcat_token_shape_unsupported"
	HashcatUnsupportedReasonHashFormat        = "hashcat_hash_format_unsupported"
	HashcatUnsupportedReasonKernelUnsupported = "hashcat_kernel_unsupported"

	maxHashcatEvidenceBytes = 8192
	maxHashcatSummaryBytes  = 512
	redactedHashcatLine     = "[redacted-sensitive-hashcat-output]"
)

var (
	longHexTokenRegex    = regexp.MustCompile(`(?i)\b[0-9a-f]{32,}\b`)
	longBase64TokenRegex = regexp.MustCompile(`\b[A-Za-z0-9+/]{48,}={0,2}\b`)
	metamaskLineRegex    = regexp.MustCompile(`(?i)\$metamask[^\s]*`)
	longDigitTokenRegex  = regexp.MustCompile(`\b\d{16,}\b`)
)

type boundedLineBuffer struct {
	maxBytes int
	lines    []string
	bytes    int
}

func newBoundedLineBuffer(maxBytes int) *boundedLineBuffer {
	if maxBytes <= 0 {
		maxBytes = maxHashcatEvidenceBytes
	}
	return &boundedLineBuffer{maxBytes: maxBytes}
}

func (b *boundedLineBuffer) Add(line string) {
	if b == nil {
		return
	}
	safe := sanitizeHashcatEvidenceLine(line)
	if safe == "" {
		return
	}
	b.lines = append(b.lines, safe)
	b.bytes += len(safe) + 1
	for b.bytes > b.maxBytes && len(b.lines) > 0 {
		b.bytes -= len(b.lines[0]) + 1
		b.lines = b.lines[1:]
	}
}

func (b *boundedLineBuffer) String() string {
	if b == nil || len(b.lines) == 0 {
		return ""
	}
	return strings.Join(b.lines, "\n")
}

func sanitizeHashcatEvidenceLine(line string) string {
	cleaned := strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, line))
	if cleaned == "" {
		return ""
	}
	lower := strings.ToLower(cleaned)
	if strings.Contains(lower, "plaintext") || strings.Contains(lower, "password") || strings.Contains(lower, "candidate") {
		return redactedHashcatLine
	}
	if strings.Contains(lower, "$metamask") || strings.Contains(lower, "$bitcoin") || strings.Contains(lower, "$electrum") {
		return redactedHashcatLine
	}
	cleaned = metamaskLineRegex.ReplaceAllString(cleaned, "[redacted-hash]")
	cleaned = longHexTokenRegex.ReplaceAllString(cleaned, "[redacted-hex]")
	cleaned = longBase64TokenRegex.ReplaceAllString(cleaned, "[redacted-base64]")
	cleaned = longDigitTokenRegex.ReplaceAllString(cleaned, "[redacted-number]")
	return truncateForLog(cleaned, 220)
}

func sanitizeHashcatEvidenceSummary(text string) string {
	buffer := newBoundedLineBuffer(maxHashcatSummaryBytes)
	for _, line := range strings.Split(text, "\n") {
		buffer.Add(line)
	}
	return truncateForLog(buffer.String(), maxHashcatSummaryBytes)
}

func classifyHashcatUnsupported(text string) (string, bool) {
	lower := strings.ToLower(text)
	switch {
	case containsAny(lower, "module not found", "module is missing", "module loading failed", "could not load module", "failed to load module"):
		return HashcatUnsupportedReasonModuleMissing, true
	case containsAny(lower, "unknown hash mode", "unsupported hash mode", "hash mode not supported", "invalid hash mode"):
		return HashcatUnsupportedReasonModeUnsupported, true
	case containsAny(lower, "unsupported attack mode", "attack mode rejected", "invalid attack mode", "attack mode not supported"):
		return HashcatUnsupportedReasonAttackUnsupported, true
	case containsAny(lower, "token length exception", "token length", "token encoding exception"):
		return HashcatUnsupportedReasonTokenShape, true
	case containsAny(lower, "separator unmatched", "signature unmatched", "hashfile format exception", "hashfile is in wrong format"):
		return HashcatUnsupportedReasonHashFormat, true
	case containsAny(lower, "kernel unavailable", "no kernel available", "kernel not found", "opencl kernel", "module kernel"):
		return HashcatUnsupportedReasonKernelUnsupported, true
	default:
		return "", false
	}
}

func containsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}
