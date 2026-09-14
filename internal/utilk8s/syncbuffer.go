package utilk8s

import (
	"bytes"
	"sync"
)

// syncBuffer is a goroutine-safe sink for the output of a remote command.
//
// It is needed because remotecommand.Executor.StreamWithContext returns as
// soon as the context is done (client-go tools/remotecommand/spdy.go) without
// joining the goroutines that io.Copy the remote streams into the writers
// passed in StreamOptions. After a command times out those goroutines may keep
// writing, so the writer outlives the call and a plain bytes.Buffer would be
// accessed concurrently: a racing Bytes() can return a slice already grown past
// the bytes actually copied into it, and a late Write can resurrect a buffer the
// caller considers finished.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer and is safe to call concurrently with the readers
// below.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

// Bytes returns a copy of everything written so far. The copy keeps the caller
// isolated from writes a late stream goroutine may still perform.
func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return bytes.Clone(b.buf.Bytes())
}

// String returns everything written so far as a string.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
