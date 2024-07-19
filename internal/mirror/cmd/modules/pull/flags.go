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

package pull

import (
	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVarP(
		&ModulesDirectory,
		"modules-dir",
		"d",
		"./d8-modules",
		"Path to modules directory.",
	)
	flagSet.StringVarP(
		&ModuleSourcePath,
		"module-source",
		"m",
		"",
		"Path to ModuleSource YAML document describing where to pull modules from.",
	)
	flagSet.StringVarP(
		&ModulesFilter,
		"filter",
		"f",
		"",
		"Filter which modules starting with which version to pull. Format is \"moduleName@v1.2.3\" separated by ';' where version after @ is the earliest pulled version of the module.",
	)
	flagSet.BoolVar(
		&SkipTLSVerify,
		"tls-skip-verify",
		false,
		"Disable TLS certificate validation.",
	)
}
