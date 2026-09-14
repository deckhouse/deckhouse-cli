package utilk8s

import (
	"strings"
	"sync"
	"testing"
)

// TestSyncBufferConcurrentAccess reproduces the situation left behind by a
// timed out StreamWithContext: a stream goroutine keeps writing into the
// buffer while the collecting goroutine reads it. Run with -race.
func TestSyncBufferConcurrentAccess(t *testing.T) {
	var buf syncBuffer

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 1000; i++ {
			if _, err := buf.Write([]byte(strings.Repeat("a", 64))); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 1000; i++ {
			for _, b := range buf.Bytes() {
				if b != 'a' {
					t.Errorf("Bytes() exposed a byte that was never written: %q", b)
					return
				}
			}

			_ = buf.String()
		}
	}()

	wg.Wait()
}

// TestSyncBufferBytesIsSnapshot guards that a collected command output cannot
// be mutated or extended by a late write from an abandoned stream goroutine.
func TestSyncBufferBytesIsSnapshot(t *testing.T) {
	var buf syncBuffer

	if _, err := buf.Write([]byte("first")); err != nil {
		t.Fatalf("write: %v", err)
	}

	snapshot := buf.Bytes()

	if _, err := buf.Write([]byte("late")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := string(snapshot); got != "first" {
		t.Errorf("snapshot changed after a later write: got %q, want %q", got, "first")
	}

	if got := string(buf.Bytes()); got != "firstlate" {
		t.Errorf("buffer content: got %q, want %q", got, "firstlate")
	}
}
