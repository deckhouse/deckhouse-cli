package lint

// LinterSettings holds the configured severity for an entire linter.
type LinterSettings struct {
	Impact *Level
}

// SetLevel parses level and stores it as the linter's impact severity, defaulting to Error when level is empty.
func (s *LinterSettings) SetLevel(level string) {
	if level != "" {
		impact := parseLevel(level)
		s.Impact = &impact

		return
	}

	lvl := Error
	s.Impact = &lvl
}

// RuleSettings holds the configured severity for an individual rule.
type RuleSettings struct {
	Impact *Level
}

// SetLevel parses level or, if empty, fallback and stores the result as the rule's impact severity, defaulting to Error when both are empty.
func (s *RuleSettings) SetLevel(level string, fallback *Level) {
	if level != "" {
		s.Impact = parseLevel(level).Ptr()
		return
	}

	if fallback != nil {
		s.Impact = fallback
		return
	}

	lvl := Error
	s.Impact = &lvl
}
