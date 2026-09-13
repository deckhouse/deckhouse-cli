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

package dataplane

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// IsFatalDataPlaneError reports whether err must end a transfer immediately,
// no matter how many bytes the attempt delivered before it. It is the ONE
// place this set is written down; Retrier.Resume consults it BEFORE it
// consults progress, and IsTransientDataPlaneError below is defined against
// it so the two can never disagree.
//
// The set is deliberately small, and every member is a statement about the
// request rather than about the link:
//
//   - context cancellation or deadline: the caller asked to stop, and an
//     aborted request surfaces through the HTTP transport looking exactly
//     like an ordinary broken connection;
//   - ErrExportUnauthorized: credentials the exporter rejected once it will
//     reject again, so retrying only multiplies the rejection;
//   - ErrContentRangeMismatch: the body cannot be trusted at the offset the
//     caller intended to write it, so continuing from it would corrupt the
//     destination rather than merely waste time.
//
// Everything else — including errors this package has never seen — is left to
// IsTransientDataPlaneError and to the progress rule in Retrier.Resume.
func IsFatalDataPlaneError(err error) bool {
	if err == nil {
		return false
	}

	// Cancellation/deadline must be recognized here so that the net.Error
	// timeout check in IsTransientDataPlaneError can never see them:
	// context.DeadlineExceeded itself implements net.Error with
	// Timeout() == true, so a timeout-first order would misclassify an
	// intentional cancellation/deadline as retryable.
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrExportUnauthorized) ||
		errors.Is(err, ErrContentRangeMismatch)
}

// IsTransientDataPlaneError reports whether err is a RECOGNIZED transient
// transport failure on the volume data plane — one where re-issuing the Range
// GET from the caller's durable resume offset is the correct response.
//
// It fails CLOSED: anything not explicitly listed is not recognized here. That
// is a statement about this function only, not about the transfer: an
// unrecognized error that arrives AFTER the attempt delivered bytes is still
// retried, by the progress rule in Retrier.Resume. The reason that rule cannot
// live here is that the failure it was written for — an HTTP/2 session torn
// down mid-body — carries a type net/http keeps in its own unexported copy of
// the http2 package, so no errors.As or errors.Is from outside can reach it.
// A classifier that could only ever grow by naming types would keep missing it.
func IsTransientDataPlaneError(err error) bool {
	if err == nil {
		return false
	}

	// Contractual errors and cancellation are never transient: they describe
	// a request the server actively rejected, a response that cannot be
	// trusted, or a caller that asked to stop — not a broken transport worth
	// re-issuing the same request against.
	if IsFatalDataPlaneError(err) {
		return false
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// io.Copy treats a clean io.EOF as ordinary successful completion, so
	// this can only reach us wrapped by a transport layer that itself
	// decided the stream ended abnormally.
	if errors.Is(err, io.EOF) {
		return true
	}

	if errors.Is(err, ErrDataPlaneIdle) {
		return true
	}

	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Not recognized here. Whether the progress rule in Retrier.Resume picks
	// them up instead depends on whether the attempt had delivered anything
	// yet, which is a property of WHEN they strike, not of what they are:
	//   - syscall.ECONNREFUSED: the export never accepted the connection at
	//     all, not a broken mid-stream transport. With one request per attempt
	//     — the shape every Fetcher method issues — it can only strike before
	//     a body exists, so nothing retries it, and retrying would just repeat
	//     a connection nobody is listening on.
	//   - HTTP 5xx status codes: RangeGet turns a non-206 status into an
	//     ordinary status error before it hands back a body, so the attempt
	//     that saw it delivered nothing either.
	//   - a local *os.PathError: FAILING TO OPEN the destination strikes
	//     before any byte is delivered and is therefore fatal on the first
	//     attempt. A write that fails PART WAY THROUGH (a full disk, say) is
	//     the other case: bytes were delivered before it, so it costs the
	//     retry budget before failing loudly. That is the accepted price of
	//     not naming error types — see Retrier.Resume.
	return false
}
