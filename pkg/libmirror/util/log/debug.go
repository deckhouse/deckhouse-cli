/*
Copyright 2024 Flant JSC

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
