/*
Copyright 2026 Flant JSC

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

package diagnostic

// HelpfulError is an error enriched with possible causes and actionable solutions.
// It implements the error interface so it can propagate up the call chain
// and be printed once at the top level, avoiding double output.
type HelpfulError struct {
	Category    string   // e.g. "DNS resolution failed for 'registry.example.com'"
	OriginalErr error    // the underlying error
	Causes      []string // "Possible causes" shown to the user
	Solutions   []string // "How to fix" shown to the user
}

// Error returns a plain-text representation suitable for logging and error wrapping.
// Use Format() for user-facing terminal output.
func (e *HelpfulError) Error() string {
	if e.OriginalErr == nil {
		return e.Category
	}
	return e.Category + ": " + e.OriginalErr.Error()
}

// Unwrap returns the original error so errors.Is/errors.As work through the wrapper.
func (e *HelpfulError) Unwrap() error {
	return e.OriginalErr
}
