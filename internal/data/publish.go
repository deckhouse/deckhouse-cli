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

package dataio

import (
	"fmt"

	"github.com/spf13/pflag"
)

// PublishFlag represents the three-state publish flag:
//   - Explicit=true, Value=true: user explicitly requested public (published) access
//   - Explicit=true, Value=false: user explicitly requested internal (in-cluster) access
//   - Explicit=false: auto-detect mode (Value is meaningless)
type PublishFlag struct {
	Explicit bool
	Value    bool
}

// ParsePublishFlag reads --publish as a three-state value.
// Explicit is true only when user provided --publish/--publish=true/--publish=false.
func ParsePublishFlag(flags *pflag.FlagSet) (PublishFlag, error) {
	if flags == nil {
		return PublishFlag{}, fmt.Errorf("publish flag parse: nil flag set")
	}

	value, err := flags.GetBool("publish")
	if err != nil {
		return PublishFlag{}, fmt.Errorf("publish flag parse: %w", err)
	}

	return PublishFlag{
		Explicit: flags.Changed("publish"),
		Value:    value,
	}, nil
}
