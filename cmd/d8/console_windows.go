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

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableVirtualTerminal lets the Windows console render the ANSI escape sequences d8 commands write to stdout.
// It does nothing when stdout is not a console, e.g. when it is redirected to a file.
func enableVirtualTerminal() {
	stdout := windows.Handle(os.Stdout.Fd())

	var mode uint32
	if err := windows.GetConsoleMode(stdout, &mode); err != nil {
		return
	}

	_ = windows.SetConsoleMode(stdout, mode|windows.ENABLE_PROCESSED_OUTPUT|windows.ENABLE_WRAP_AT_EOL_OUTPUT|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
}
