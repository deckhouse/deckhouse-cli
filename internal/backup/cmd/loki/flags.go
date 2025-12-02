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

package loki

import (
	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVarP(
		&config.StartTimestamp,
		"start", "s",
		"",
		"Start timestamp for log dumping. Format: 2006-01-02 15:04:05 (UTC).",
	)
	flagSet.StringVarP(
		&config.EndTimestamp,
		"end", "e",
		"",
		"End timestamp for log dumping. Format: 2006-01-02 15:04:05 (UTC).",
	)
	flagSet.StringVarP(
		&config.Limit,
		"limit", "l",
		"5000",
		"Limit number of log entries per query.",
	)
	flagSet.IntVarP(
		&config.ChunkDays,
		"chunk-days", "c",
		1,
		"Number of days per chunk for pagination.",
	)
}
