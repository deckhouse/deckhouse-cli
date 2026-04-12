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
// Option 1: use a command-specific errdetect package
// (see internal/mirror/cmd/pull/errdetect for an example):
//
//	if diag := errdetect.Diagnose(err); diag != nil {
//	    return diag
//	}
//
// Option 2: wrap an error directly:
//
//	return &diagnostic.HelpfulError{
//	    Category:    "ETCD snapshot failed",
//	    OriginalErr: err,
//	    Suggestions: []diagnostic.Suggestion{
//	        {
//	            Cause:     "ETCD cluster is unreachable",
//	            Solutions: []string{"Check ETCD health: etcdctl endpoint health"},
//	        },
//	    },
//	}
//
// Suggestions are optional - an empty slice is silently omitted from output.
// Each Suggestion pairs a cause with its specific solutions.
//
// # How fields map to Format() output
//
//	error: ETCD snapshot failed                <-- Category
//	  ╰─▶ save snapshot                         <-- OriginalErr chain (unwrapped)
//	    ╰─▶ dial tcp 10.0.0.1:2379
//	      ╰─▶ connection refused
//
//	  * ETCD cluster is unreachable             <-- Suggestion.Cause
//	    -> Check ETCD health: etcdctl ...       <-- Suggestion.Solutions
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
// # Adding diagnostics to a new command
//
// Create an errdetect package next to your command with a Diagnose function:
//
//	// internal/backup/cmd/snapshot/errdetect/diagnose.go
//	func Diagnose(err error) *diagnostic.HelpfulError {
//	    if isETCDError(err) {
//	        return &diagnostic.HelpfulError{
//	            Category: "ETCD connection failed", OriginalErr: err,
//	            Suggestions: []diagnostic.Suggestion{
//	                {Cause: "ETCD cluster is unreachable", Solutions: []string{"Check ETCD health"}},
//	            },
//	        }
//	    }
//	    return nil
//	}
//
// Then call it at the command level:
//
//	if diag := errdetect.Diagnose(err); diag != nil {
//	    return diag
//	}
//
// # Important: diagnose at the command level, not in root.go
//
// Each command must call its own errdetect package. root.go only catches
// [HelpfulError] via [errors.As] - it does not import or call any classifier.
// This prevents false classification: a DNS error from "d8 backup" must not
// be diagnosed with registry-specific advice like "--tls-skip-verify".
package diagnostic
