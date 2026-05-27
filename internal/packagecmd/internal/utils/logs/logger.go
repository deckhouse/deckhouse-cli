package logs

import (
	"fmt"
	"io"
	"os"
)

const (
	colorGreen  = "\033[32m"
	colorReset  = "\033[0m"
	colorOrange = "\033[38;5;208m"
	colorRed    = "\033[31m"
)

// Logger wraps slog.Logger with additional functionality
type Logger struct {
	verbose bool
	output  io.Writer
}

// New creates a new enhanced logger
func New(verbose bool) *Logger {
	return &Logger{
		verbose: verbose,
		output:  os.Stdout,
	}
}

// Error logs a step message (always visible) in red
func (l *Logger) Error(msg string, args ...any) {
	_, _ = fmt.Fprintf(l.output, "%s[ERROR] %s%s\n", colorRed, fmt.Sprintf(msg, args...), colorReset)
}

// Warn logs a step message (always visible) in orange
func (l *Logger) Warn(msg string, args ...any) {
	_, _ = fmt.Fprintf(l.output, "%s[WARNING] %s%s\n", colorOrange, fmt.Sprintf(msg, args...), colorReset)
}

// Info logs a step message (always visible) in green
func (l *Logger) Info(msg string, args ...any) {
	_, _ = fmt.Fprintf(l.output, "%s%s%s\n", colorGreen, fmt.Sprintf(msg, args...), colorReset)
}

// Debug logs detailed step information (only in verbose mode)
func (l *Logger) Debug(msg string, args ...any) {
	if l.verbose {
		_, _ = fmt.Fprintf(l.output, "[DEBUG] %s\n", fmt.Sprintf(msg, args...))
	}
}
