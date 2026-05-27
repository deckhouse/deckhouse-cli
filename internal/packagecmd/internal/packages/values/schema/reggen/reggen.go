/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package reggen produces example strings that match a Perl-flavored regular
// expression. It is used by the defaults package to synthesize placeholder
// values for string fields constrained by an OpenAPI `pattern`.
//
// The algorithm is a structural walk over the parsed regex syntax tree. It
// covers the constructs that appear in OpenAPI schemas (literals, character
// classes, anchors, capture groups, repetition, alternation). Patterns that
// use constructs outside that set return an error.
//
// Output is deterministic: the same (regex, limit) pair always yields the
// same string. The RNG is seeded from a hash of the inputs, which is enough
// for example-data use and reproduces across processes.
//
// Adapted from https://github.com/lucasjones/reggen.
package reggen

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"regexp/syntax"
	"strings"
)

// runeRangeEnd is the upper bound of the Unicode code-point space. A
// character class spanning to this value is treated as a negation result
// (effectively "any character except some set"), which we handle by
// sampling from printable ASCII instead of the full Unicode range.
const runeRangeEnd = 0x10ffff

// printableASCII is the alphabet used when synthesizing matches for `.`
// and for inverse character classes. Restricting to printable ASCII keeps
// generated values readable in YAML and JSON output.
const printableASCII = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n\r"

// printableASCIINoNL drops the trailing "\n\r" from printableASCII for use
// with `.` (any character except newline).
var printableASCIINoNL = printableASCII[:len(printableASCII)-2]

// Generate produces a string matching the given Perl-flavored regex.
//
// `limit` bounds the unbounded repetition operators (*, +, {m,}) and caps
// the upper end of bounded ones ({m,n}). For example, `[0-9]+` with limit=10
// produces at most 10 digits. When a regex requires more repetitions than
// `limit` allows (e.g. `{12}` with limit=8), the regex's lower bound wins
// so the output still matches.
//
// Output is deterministic for a given (regex, limit) pair.
//
// Errors are returned when the regex fails to parse or contains a construct
// the generator does not support.
func Generate(regex string, limit int) (string, error) {
	parsed, err := syntax.Parse(regex, syntax.Perl)
	if err != nil {
		return "", fmt.Errorf("parse regex %q: %w", regex, err)
	}

	if limit < 1 {
		limit = 1
	}

	rng := rand.New(rand.NewSource(seedFor(regex, limit))) //nolint:gosec // non-cryptographic, seeded for determinism.

	var b strings.Builder
	if err := emit(&b, rng, parsed, limit); err != nil {
		return "", err
	}

	return b.String(), nil
}

// seedFor produces a stable RNG seed from the inputs so that two calls to
// Generate with the same arguments always yield the same string, across
// processes and across machines.
func seedFor(regex string, limit int) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(regex))

	return int64(h.Sum64()) ^ int64(limit)
}

// emit walks the regex syntax tree and writes one matching string to b.
// It returns an error only when the parser produces an Op the generator
// does not know how to handle; structurally invalid regexes are rejected
// earlier by syntax.Parse.
func emit(b *strings.Builder, rng *rand.Rand, re *syntax.Regexp, limit int) error {
	switch re.Op {
	case syntax.OpNoMatch, syntax.OpEmptyMatch,
		syntax.OpBeginLine, syntax.OpEndLine,
		syntax.OpBeginText, syntax.OpEndText,
		syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		// Zero-width nodes contribute no characters.
		return nil

	case syntax.OpLiteral:
		for _, r := range re.Rune {
			b.WriteRune(r)
		}

		return nil

	case syntax.OpCharClass:
		return emitCharClass(b, rng, re)

	case syntax.OpAnyChar:
		b.WriteByte(printableASCII[rng.Intn(len(printableASCII))])
		return nil

	case syntax.OpAnyCharNotNL:
		b.WriteByte(printableASCIINoNL[rng.Intn(len(printableASCIINoNL))])
		return nil

	case syntax.OpCapture:
		return emit(b, rng, re.Sub0[0], limit)

	case syntax.OpStar, syntax.OpPlus:
		// `*` is zero-or-more, `+` is one-or-more. With a finite limit we
		// always emit `limit` repetitions: under-producing would make
		// `[a-z]+` collapse to an empty string, which is rarely useful as
		// example data.
		return emitRepeat(b, rng, re, limit, limit)

	case syntax.OpQuest:
		// `?` is zero-or-one. Always emit one for the same reason as above.
		return emitRepeat(b, rng, re, 1, limit)

	case syntax.OpRepeat:
		count := chooseRepeatCount(re, limit)
		return emitRepeat(b, rng, re, count, limit)

	case syntax.OpConcat:
		for _, sub := range re.Sub {
			if err := emit(b, rng, sub, limit); err != nil {
				return err
			}
		}

		return nil

	case syntax.OpAlternate:
		return emit(b, rng, re.Sub[rng.Intn(len(re.Sub))], limit)
	}

	return fmt.Errorf("reggen: unsupported regex op %v", re.Op)
}

// chooseRepeatCount picks how many times to repeat for an OpRepeat node.
// We prefer the upper bound for predictable, longer-looking output. The
// regex's lower bound always wins over `limit` so the result still matches
// patterns like `[a-z]{12}` even when the caller passed a smaller limit.
func chooseRepeatCount(re *syntax.Regexp, limit int) int {
	max := re.Max
	if max < 0 || max > limit {
		// Max == -1 marks an open upper bound (e.g. `{2,}`); clamp to limit.
		max = limit
	}

	if re.Min > max {
		max = re.Min
	}

	return max
}

// emitRepeat writes the regex's body `count` times. Sub-expressions inside
// the loop still see the outer `limit`, so nested repetitions stay bounded.
func emitRepeat(b *strings.Builder, rng *rand.Rand, re *syntax.Regexp, count, limit int) error {
	for range count {
		for _, sub := range re.Sub {
			if err := emit(b, rng, sub, limit); err != nil {
				return err
			}
		}
	}

	return nil
}

// emitCharClass picks one rune from a positive character class and writes
// it to b. When the class spans to runeRangeEnd it is the byproduct of a
// negation; we fall back to printable ASCII so the output stays readable.
func emitCharClass(b *strings.Builder, rng *rand.Rand, re *syntax.Regexp) error {
	total := 0

	for i := 0; i < len(re.Rune); i += 2 {
		if re.Rune[i+1] == runeRangeEnd {
			return emitInverseCharClass(b, rng, re)
		}

		total += int(re.Rune[i+1]-re.Rune[i]) + 1
	}

	if total == 0 {
		return nil
	}

	pick := rng.Intn(total)
	cursor := 0

	for i := 0; i < len(re.Rune); i += 2 {
		size := int(re.Rune[i+1]-re.Rune[i]) + 1
		if cursor+size > pick {
			b.WriteRune(re.Rune[i] + rune(pick-cursor))
			return nil
		}

		cursor += size
	}

	return nil
}

// emitInverseCharClass handles classes whose ranges include runeRangeEnd
// (typically the product of negation). We sample printable ASCII restricted
// to characters that fall inside one of the class's ranges.
func emitInverseCharClass(b *strings.Builder, rng *rand.Rand, re *syntax.Regexp) error {
	var candidates []byte

	for i := range len(printableASCII) {
		c := printableASCII[i]
		for j := 0; j < len(re.Rune); j += 2 {
			if rune(c) >= re.Rune[j] && rune(c) <= re.Rune[j+1] {
				candidates = append(candidates, c)
				break
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	b.WriteByte(candidates[rng.Intn(len(candidates))])

	return nil
}
