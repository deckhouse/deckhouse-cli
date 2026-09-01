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

package exporter

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// IsTransientDataPlaneError reports whether err is a transient transport
// failure on the volume data plane — one where re-issuing the Range GET from
// the caller's durable resume offset is the correct response.
//
// It fails CLOSED: anything not explicitly listed is treated as fatal, so a
// misclassification costs a loud failure, never a silent retry loop that
// masks a real defect. In particular, sentinels defined outside this package
// (volume.ErrShortChunkRead, any *os.PathError from local disk I/O) are
// non-transient by construction, because the default is false.
func IsTransientDataPlaneError(err error) bool {
	if err == nil {
		return false
	}

	// Cancellation/deadline must be checked BEFORE the net.Error timeout
	// check below: context.DeadlineExceeded itself implements net.Error
	// with Timeout() == true, so checking timeouts first would
	// misclassify an intentional cancellation/deadline as retryable.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Contractual errors are never transient: they describe a request the
	// server actively rejected or a response that cannot be trusted, not a
	// broken transport worth re-issuing the same request against.
	if errors.Is(err, ErrExportUnauthorized) || errors.Is(err, ErrContentRangeMismatch) {
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

	// Deliberately NOT retried:
	//   - syscall.ECONNREFUSED: the export never accepted the connection at
	//     all, not a broken mid-stream transport — retrying just repeats a
	//     connection nobody is listening on.
	//   - HTTP 5xx status codes: RangeGet turns a non-206 status into an
	//     ordinary status error, and none has ever been observed in
	//     practice (0 occurrences across 122k lines of ingress logs).
	//   - volume.ErrShortChunkRead: a clean short read means the server lied
	//     about the range it promised, which is unreachable from this
	//     package and not a transport failure to retry.
	return false
}
