package diag

import (
	"sync"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
)

// store is the shared, goroutine-safe log of all findings collected during a lint run.
// Multiple Collector handles (scoped copies) write to the same store concurrently.
type store struct {
	mu   sync.RWMutex
	logs []errLog
}

// errLog is a single finding: the diagnostic context where it was emitted, its
// severity level, and the human-readable message.
type errLog struct {
	context
	level   lint.Level
	message string
}

func (s *store) addLog(log errLog) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs = append(s.logs, log)
}

// getLogs returns a snapshot copy of all logs so callers can iterate without
// holding the lock.
func (s *store) getLogs() []errLog {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]errLog, 0, len(s.logs))
	result = append(result, s.logs...)

	return result
}
