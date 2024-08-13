package oracleparser

// Special case normalization rules for Turkish/Azeri lowercase dotless-i and
import (
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// uppercase dotted-i. Fold both dotted and dotless 'i' into the ascii i/I, so
// our case-insensitive comparison functions can be locale-invariant. This
// mapping implements case-insensitivity for Turkish and other latin-derived
// languages simultaneously, with the additional quirk that it is also
// insensitive to the dottedness of the i's
var normalize = unicode.SpecialCase{
	unicode.CaseRange{
		Lo: 0x0130,
		Hi: 0x0130,
		Delta: [unicode.MaxCase]rune{
			0x49 - 0x130, // Upper
			0x69 - 0x130, // Lower
			0x49 - 0x130, // Title
		},
	},
	unicode.CaseRange{
		Lo: 0x0131,
		Hi: 0x0131,
		Delta: [unicode.MaxCase]rune{
			0x49 - 0x131, // Upper
			0x69 - 0x131, // Lower
			0x49 - 0x131, // Title
		},
	},
}

// NormalizeName normalizes to lowercase and Unicode Normalization
// Form C (NFC).
func NormalizeName(n string) string {
	lower := strings.Map(normalize.ToLower, n)
	if isASCII(lower) {
		return lower
	}
	return norm.NFC.String(lower)
}

// NormalizeString normalizes to Unicode Normalization Form C (NFC).
// This function is specifically for double quoted identifiers.
func NormalizeString(s string) string {
	if isASCII(s) {
		return s
	}
	return norm.NFC.String(s)
}

// isASCII returns true if all the characters in s are ASCII.
func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// IsDigit returns true if the character is between 0 and 9.
func IsDigit(ch int) bool {
	return ch >= '0' && ch <= '9'
}

// IsHexDigit returns true if the character is a valid hexadecimal digit.
func IsHexDigit(ch int) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'f') ||
		(ch >= 'A' && ch <= 'F')
}

// IsBareIdentifier returns true if the input string is a permissible bare SQL
// identifier.
func IsBareIdentifier(s string) bool {
	if len(s) == 0 || !IsIdentStart(int(s[0])) || (s[0] >= 'A' && s[0] <= 'Z') {
		return false
	}
	// Keep track of whether the input string is all ASCII. If it is, we don't
	// have to bother running the full Normalize() function at the end, which is
	// quite expensive.
	isASCII := s[0] < utf8.RuneSelf
	for i := 1; i < len(s); i++ {
		if !IsIdentMiddle(int(s[i])) {
			return false
		}
		if s[i] >= 'A' && s[i] <= 'Z' {
			// Non-lowercase identifiers aren't permissible.
			return false
		}
		if s[i] >= utf8.RuneSelf {
			isASCII = false
		}
	}
	return isASCII || NormalizeName(s) == s
}

// IsIdentStart returns true if the character is valid at the start of an identifier.
func IsIdentStart(ch int) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 128 && ch <= 255) ||
		(ch == '_')
}

// IsIdentMiddle returns true if the character is valid inside an identifier.
func IsIdentMiddle(ch int) bool {
	return IsIdentStart(ch) || IsDigit(ch) || ch == '$'
}
