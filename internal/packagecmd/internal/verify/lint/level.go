package lint

import (
	"fmt"
	"strings"
)

// Level represents the severity of a lint diagnostic.
type Level int

// Severity levels ordered from least to most severe.
const (
	Ignored Level = iota // diagnostic is suppressed
	Warn                 // non-blocking advisory
	Error                // blocking issue
)

// stringLevelMappings maps each Level to its canonical string representation.
var stringLevelMappings = map[Level]string{
	Ignored: "ignored",
	Warn:    "warn",
	Error:   "error",
}

// levelStringMappings maps canonical string names to their corresponding Level.
var levelStringMappings = map[string]Level{
	"ignored": Ignored,
	"warn":    Warn,
	"error":   Error,
}

// parseLevel converts a string to a Level, defaulting to Error for unknown or empty values.
func parseLevel(str string) Level {
	if str == "" {
		return Error
	}

	normalized := strings.ToLower(strings.TrimSpace(str))

	lvl, ok := levelStringMappings[normalized]
	if !ok {
		if normalized != "" {
			fmt.Printf("WARN invalid level %q, fallback to \"error\"\n", str)
		}

		return Error
	}

	return lvl
}

// String returns the canonical string representation of the Level.
func (l Level) String() string {
	str, ok := stringLevelMappings[l]
	if !ok {
		return "error"
	}

	return str
}

// Ptr returns pointer of the level
func (l Level) Ptr() *Level {
	return &l
}
