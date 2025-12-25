/*
Copyright 2024 Flant JSC

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

package mirror

import (
	"fmt"
	"sort"
	"strings"
)

// Mismatch represents a digest mismatch between source and target
type Mismatch struct {
	// Ref is the normalized image reference (path:tag)
	Ref string

	// ExpectedDigest is the digest from the source registry
	ExpectedDigest string

	// ActualDigest is the digest from the target registry (empty if not found)
	ActualDigest string

	// Type indicates the type of mismatch
	Type MismatchType
}

// MismatchType categorizes the type of mismatch
type MismatchType int

const (
	// MismatchTypeMissing means the image is missing in the target
	MismatchTypeMissing MismatchType = iota

	// MismatchTypeDigestDifferent means the digests don't match
	MismatchTypeDigestDifferent

	// MismatchTypeExtra means the image exists in target but not in source
	MismatchTypeExtra
)

func (t MismatchType) String() string {
	switch t {
	case MismatchTypeMissing:
		return "MISSING"
	case MismatchTypeDigestDifferent:
		return "DIGEST_MISMATCH"
	case MismatchTypeExtra:
		return "EXTRA"
	default:
		return "UNKNOWN"
	}
}

// String returns a human-readable representation of the mismatch
func (m Mismatch) String() string {
	switch m.Type {
	case MismatchTypeMissing:
		return fmt.Sprintf("[%s] %s: expected %s, not found in target",
			m.Type, m.Ref, m.ExpectedDigest)
	case MismatchTypeDigestDifferent:
		return fmt.Sprintf("[%s] %s: expected %s, got %s",
			m.Type, m.Ref, m.ExpectedDigest, m.ActualDigest)
	case MismatchTypeExtra:
		return fmt.Sprintf("[%s] %s: unexpected image with digest %s",
			m.Type, m.Ref, m.ActualDigest)
	default:
		return fmt.Sprintf("[%s] %s", m.Type, m.Ref)
	}
}

// CompareResult contains the result of digest comparison
type CompareResult struct {
	// Mismatches lists all found mismatches
	Mismatches []Mismatch

	// MatchedCount is the number of images that matched
	MatchedCount int

	// TotalExpected is the total number of expected images
	TotalExpected int

	// TotalActual is the total number of actual images
	TotalActual int
}

// IsMatch returns true if all digests matched
func (r *CompareResult) IsMatch() bool {
	return len(r.Mismatches) == 0
}

// MissingCount returns the number of missing images
func (r *CompareResult) MissingCount() int {
	count := 0
	for _, m := range r.Mismatches {
		if m.Type == MismatchTypeMissing {
			count++
		}
	}
	return count
}

// DigestMismatchCount returns the number of digest mismatches
func (r *CompareResult) DigestMismatchCount() int {
	count := 0
	for _, m := range r.Mismatches {
		if m.Type == MismatchTypeDigestDifferent {
			count++
		}
	}
	return count
}

// String returns a summary of the comparison
func (r *CompareResult) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Comparison Summary:\n"))
	sb.WriteString(fmt.Sprintf("  Expected: %d images\n", r.TotalExpected))
	sb.WriteString(fmt.Sprintf("  Actual:   %d images\n", r.TotalActual))
	sb.WriteString(fmt.Sprintf("  Matched:  %d images\n", r.MatchedCount))
	sb.WriteString(fmt.Sprintf("  Missing:  %d images\n", r.MissingCount()))
	sb.WriteString(fmt.Sprintf("  Digest mismatches: %d\n", r.DigestMismatchCount()))

	if !r.IsMatch() {
		sb.WriteString("\nMismatches:\n")
		for _, m := range r.Mismatches {
			sb.WriteString("  " + m.String() + "\n")
		}
	}

	return sb.String()
}

// Compare compares two digest maps and returns mismatches
// Both maps should have normalized references (using NormalizeDigests)
func Compare(expected, actual DigestMap) *CompareResult {
	result := &CompareResult{
		Mismatches:    make([]Mismatch, 0),
		TotalExpected: len(expected),
		TotalActual:   len(actual),
	}

	// Check all expected images
	for ref, expectedDigest := range expected {
		actualDigest, exists := actual[ref]

		if !exists {
			result.Mismatches = append(result.Mismatches, Mismatch{
				Ref:            ref,
				ExpectedDigest: expectedDigest,
				Type:           MismatchTypeMissing,
			})
			continue
		}

		if expectedDigest != actualDigest {
			result.Mismatches = append(result.Mismatches, Mismatch{
				Ref:            ref,
				ExpectedDigest: expectedDigest,
				ActualDigest:   actualDigest,
				Type:           MismatchTypeDigestDifferent,
			})
			continue
		}

		result.MatchedCount++
	}

	// Sort mismatches by ref for consistent output
	sort.Slice(result.Mismatches, func(i, j int) bool {
		return result.Mismatches[i].Ref < result.Mismatches[j].Ref
	})

	return result
}

// CompareStrict also reports extra images in the target
func CompareStrict(expected, actual DigestMap) *CompareResult {
	result := Compare(expected, actual)

	// Find extra images in actual that don't exist in expected
	for ref, actualDigest := range actual {
		if _, exists := expected[ref]; !exists {
			result.Mismatches = append(result.Mismatches, Mismatch{
				Ref:          ref,
				ActualDigest: actualDigest,
				Type:         MismatchTypeExtra,
			})
		}
	}

	// Re-sort after adding extras
	sort.Slice(result.Mismatches, func(i, j int) bool {
		return result.Mismatches[i].Ref < result.Mismatches[j].Ref
	})

	return result
}

// FormatMismatches formats mismatches for test output
func FormatMismatches(mismatches []Mismatch) string {
	if len(mismatches) == 0 {
		return "no mismatches"
	}

	var sb strings.Builder
	for i, m := range mismatches {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(m.String())

		// Limit output for very long lists
		if i >= 50 {
			sb.WriteString(fmt.Sprintf("\n... and %d more mismatches", len(mismatches)-i-1))
			break
		}
	}
	return sb.String()
}
