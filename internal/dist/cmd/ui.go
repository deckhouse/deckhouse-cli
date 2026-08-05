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

package distcmd

import "github.com/fatih/color"

// Semantic output palette shared by the dist commands: green = the version
// being moved to, cyan+bold = the active version, faint = a superseded
// version. fatih/color drops ANSI on a non-TTY and under NO_COLOR.
var (
	okMark = color.New(color.FgGreen, color.Bold)
	verNew = color.New(color.FgGreen)
	verCur = color.New(color.FgCyan, color.Bold)
	verOld = color.New(color.Faint)
)

// Summary accents, following the semantic palette convention of
// internal/mirror/summaryui. Apply colours AFTER width padding: the escape
// codes are zero-width on screen but count toward fmt's field widths.
var (
	sumTitle = color.New(color.FgCyan, color.Bold).SprintFunc() // section headers
	sumLabel = color.New(color.FgCyan).SprintFunc()             // field labels (scan anchors)
	sumGood  = color.New(color.FgGreen).SprintFunc()            // up to date
	sumWarn  = color.New(color.FgYellow).SprintFunc()           // attention (outdated, degraded)
	sumDim   = color.New(color.Faint).SprintFunc()              // secondary text and hints
)
