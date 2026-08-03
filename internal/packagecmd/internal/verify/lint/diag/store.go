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

// ruleScope identifies the rule a log belongs to. Remote verification lints several
// images into one store, so source is part of the identity: the same rule run against
// the bundle image and against the release image are two independent results.
type ruleScope struct {
	source   string
	linterID string
	ruleID   string
}

// scope returns the rule identity of the log.
func (l errLog) scope() ruleScope {
	return ruleScope{
		source:   l.source,
		linterID: l.linterID,
		ruleID:   l.ruleID,
	}
}

func (s *store) addLog(log errLog) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs = append(s.logs, log)
}

// addPassOnce records log unless its rule already produced one — either a finding or an
// earlier pass. The lookup and the append share a single lock, so images linted
// concurrently cannot both conclude that the same rule was clean.
func (s *store) addPassOnce(log errLog) {
	s.mu.Lock()
	defer s.mu.Unlock()

	scope := log.scope()

	for _, existing := range s.logs {
		if existing.scope() == scope {
			return
		}
	}

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
