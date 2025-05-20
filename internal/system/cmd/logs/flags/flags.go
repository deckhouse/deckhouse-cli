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

package flags

import (
	"github.com/spf13/pflag"
)

var (
	Tail      int64
	Follow    bool
	Since     string
	SinceTime string
)

func AddFlags(flagSet *pflag.FlagSet) {
	flagSet.Int64Var(&Tail, "tail", 100, "default value number output strings logs from Deckhouse container.")
	flagSet.BoolVarP(&Follow, "follow", "f", false, "Specify if the logs should be streamed.")
	flagSet.StringVar(&Since, "since", "", "Show logs newer than a relative duration like 5s, 2m, or 1h.")
	flagSet.StringVar(&SinceTime, "since-time", "", "Show logs after a specific timestamp, e.g. --since-time='2025-05-19 12:00:00'")
}
