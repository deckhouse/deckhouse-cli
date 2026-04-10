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

// Package diagnostic provides [HelpfulError] - a wrapper around standard Go errors
// that adds possible causes and actionable solutions for the user.
//
// When a command returns a [HelpfulError], the top-level handler in cmd/d8/root.go
// detects it via [errors.As] and prints a formatted diagnostic instead of a raw error.
// If an error is not wrapped in [HelpfulError], it is printed as usual.
//
// # Creating a HelpfulError
//
// Option 1: use a domain-specific classifier that wraps known errors
// (see [github.com/deckhouse/deckhouse-cli/pkg/registry/errdiag] for the registry
// implementation):
//
//	if diag := errdiag.Classify(err); diag != nil {
//	    return diag
//	}
//
// Option 2: wrap an error directly:
//
//	return &diagnostic.HelpfulError{
//	    Category:    "ETCD snapshot failed",
//	    OriginalErr: err,
//	    Causes:      []string{"ETCD cluster is unreachable"},
//	    Solutions:   []string{"Check ETCD health: etcdctl endpoint health"},
//	}
//
// Causes and Solutions are optional - empty slices are silently omitted from output.
//
// # How fields map to Format() output
//
//	error: ETCD snapshot failed                          <-- Category
//	  ╰─▶ dial tcp 10.0.0.1:2379: connection refused    <-- OriginalErr.Error()
//
//	  Possible causes:                                   <-- Causes
//	    * ETCD cluster is unreachable
//
//	  How to fix:                                        <-- Solutions
//	    * Check ETCD health: etcdctl endpoint health
//
// # How it propagates
//
// [HelpfulError] implements the error interface. It propagates up the call chain
// like any other error. The original error is preserved via [HelpfulError.Unwrap],
// so [errors.Is] and [errors.As] work through the wrapper.
//
// In cmd/d8/root.go:
//
//	var helpErr *diagnostic.HelpfulError
//	if errors.As(err, &helpErr) {
//	    fmt.Fprint(os.Stderr, helpErr.Format()) // colored output, once
//	}
//
// [HelpfulError.Error] returns plain text (safe for logs).
// [HelpfulError.Format] returns colored terminal output (TTY-aware, respects NO_COLOR).
//
// # Adding a new domain classifier
//
// To add diagnostics for a new domain (e.g. backup), create a Classify function
// that wraps known errors into *[HelpfulError]:
//
//	// internal/backup/errdiag/classify.go
//	func Classify(err error) *diagnostic.HelpfulError {
//	    if isETCDError(err) {
//	        return &diagnostic.HelpfulError{
//	            Category: "ETCD connection failed", OriginalErr: err,
//	            Causes:   []string{"ETCD cluster is unreachable"},
//	            Solutions: []string{"Check ETCD health"},
//	        }
//	    }
//	    return nil
//	}
//
// Then call it at the command level, same pattern as registry:
//
//	if diag := errdiag.Classify(err); diag != nil {
//	    return diag
//	}
//
// # Important: classify at the command level, not in root.go
//
// Each command must call its own domain classifier. root.go only catches
// [HelpfulError] via [errors.As] - it does not import or call any classifier.
// This prevents false classification: a DNS error from "d8 backup" must not
// be classified with registry-specific advice like "--tls-skip-verify".
package diagnostic
