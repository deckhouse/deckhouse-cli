package log

import (
	"log"
	"os"
	"strconv"
)

// DebugLogLevel returns level of debug logging requested by the user via MIRROR_DEBUG_LOG environment variable.
// Level is expressed as a number in range [0, 4] where:
// - 0 means no debug log at all;
// - 1 means crane progress logging to stderr;
// - 2 means 1 + crane warnings are printed to stderr;
// - 3 means 2 + mirror debug messages are printed to stdout;
// - 4 means 3 + registry requests and responses are printed to stderr.
func DebugLogLevel() int {
	debugLogStr := os.Getenv("MIRROR_DEBUG_LOG")
	if debugLogStr == "" {
		return 0
	}

	debugLogLevel, err := strconv.Atoi(debugLogStr)
	if err != nil {
		log.Printf("Invalid $MIRROR_DEBUG_LOG: %v\nUse 1 for progress logging, 2 for warnings, 3 for debug messages or 4 for connection logging. Each level also enables previous ones.\n", err)
		return 0
	}

	return debugLogLevel
}
