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

package htpasswd

import "fmt"

// Apache htpasswd process exit codes. d8 mirrors the ones a script is likely to
// branch on so 'd8 tools htpasswd' can stand in for 'htpasswd'. File-access
// failures (missing file, permission, I/O) keep the default exit code 1 by
// being returned unwrapped, which also matches htpasswd.
// See https://httpd.apache.org/docs/current/programs/htpasswd.html.
const (
	exitUsage    = 2 // usage/syntax: bad flags, conflicting flags, wrong arg count, bad flag value
	exitVerify   = 3 // password verification failed, prompt mismatch, or hash-encode failure
	exitOverflow = 5 // username too long
	exitBadUser  = 6 // illegal character in username, or user not found
)

// exitError wraps an error with a process exit code. The CLI root
// (cmd/d8/root.go) looks for an ExitCode() int method via errors.As and exits
// with that status; errors without it exit 1 as before, so this only affects
// 'd8 tools htpasswd'.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }
func (e *exitError) ExitCode() int { return e.code }

// coded wraps err with the given exit code. A nil err returns nil so it is safe
// to wrap a call result directly.
func coded(code int, err error) error {
	if err == nil {
		return nil
	}

	return &exitError{code: code, err: err}
}

// usageErr, verifyErr, overflowErr and badUserErr build a coded error from a
// printf-style message, one per htpasswd exit-code class.
func usageErr(format string, a ...any) error {
	return &exitError{exitUsage, fmt.Errorf(format, a...)}
}

func verifyErr(format string, a ...any) error {
	return &exitError{exitVerify, fmt.Errorf(format, a...)}
}

func overflowErr(format string, a ...any) error {
	return &exitError{exitOverflow, fmt.Errorf(format, a...)}
}

func badUserErr(format string, a ...any) error {
	return &exitError{exitBadUser, fmt.Errorf(format, a...)}
}
