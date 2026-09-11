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
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
)

func TestIsTransientDataPlaneError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error is not transient", err: nil, want: false},
		{
			name: "unexpected EOF is transient",
			err:  fmt.Errorf("stream: %w", io.ErrUnexpectedEOF),
			want: true,
		},
		{
			name: "clean EOF wrapped by a transport layer is transient",
			err:  fmt.Errorf("read: %w", io.EOF),
			want: true,
		},
		{
			name: "idle watchdog trip is transient",
			err:  fmt.Errorf("read chunk: %w", ErrDataPlaneIdle),
			want: true,
		},
		{
			name: "ECONNRESET is transient",
			err:  fmt.Errorf("read: %w", syscall.ECONNRESET),
			want: true,
		},
		{
			name: "ECONNABORTED is transient",
			err:  fmt.Errorf("read: %w", syscall.ECONNABORTED),
			want: true,
		},
		{
			name: "EPIPE is transient",
			err:  fmt.Errorf("write: %w", syscall.EPIPE),
			want: true,
		},
		{
			name: "ETIMEDOUT is transient",
			err:  fmt.Errorf("dial: %w", syscall.ETIMEDOUT),
			want: true,
		},
		{
			name: "a net.Error reporting Timeout() is transient",
			err:  fmt.Errorf("http do: %w", &fakeNetError{timeout: true}),
			want: true,
		},
		{
			name: "a net.Error not reporting Timeout() is fatal",
			err:  fmt.Errorf("http do: %w", &fakeNetError{timeout: false}),
			want: false,
		},
		{
			name: "ErrExportUnauthorized is fatal",
			err:  fmt.Errorf("range get: %w", ErrExportUnauthorized),
			want: false,
		},
		{
			name: "ErrContentRangeMismatch is fatal",
			err:  fmt.Errorf("range get: %w", ErrContentRangeMismatch),
			want: false,
		},
		{
			name: "context.Canceled is fatal, never retried",
			err:  fmt.Errorf("do: %w", context.Canceled),
			want: false,
		},
		{
			// Regression guard: context.DeadlineExceeded satisfies
			// net.Error with Timeout() == true, so this must be checked
			// (and rejected) BEFORE the net.Error branch, not fall through
			// to it and be misclassified as transient.
			name: "context.DeadlineExceeded is fatal despite satisfying net.Error",
			err:  fmt.Errorf("do: %w", context.DeadlineExceeded),
			want: false,
		},
		{
			name: "a wrapped *url.Error without Timeout() is fatal",
			err: &url.Error{
				Op:  "Get",
				URL: "https://example.invalid/api/v1/block",
				Err: errors.New("no route to host"),
			},
			want: false,
		},
		{
			name: "ECONNREFUSED is fatal: the export never accepted the connection",
			err:  fmt.Errorf("dial: %w", syscall.ECONNREFUSED),
			want: false,
		},
		{
			name: "a local *os.PathError is fatal",
			err:  &os.PathError{Op: "open", Path: "/tmp/chunk.part", Err: errors.New("permission denied")},
			want: false,
		},
		{
			name: "an arbitrary error is fatal by default",
			err:  errors.New("simulated interrupt: connection dropped mid-chunk"),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := IsTransientDataPlaneError(tc.err)
			if got != tc.want {
				t.Errorf("IsTransientDataPlaneError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// fakeNetError is a minimal net.Error stand-in that lets tests control
// Timeout() independently of any real network condition.
type fakeNetError struct {
	timeout bool
}

func (e *fakeNetError) Error() string   { return "fake net error" }
func (e *fakeNetError) Timeout() bool   { return e.timeout }
func (e *fakeNetError) Temporary() bool { return e.timeout }

var _ net.Error = (*fakeNetError)(nil)
