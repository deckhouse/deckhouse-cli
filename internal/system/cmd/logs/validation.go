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

package logs

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func ValidateParameters(cmd *cobra.Command, args []string) error {
	if Tail < -1 {
		return fmt.Errorf("Invalid --tail must be greater than or equal to -1.")
	}
	if Since != "" {
		_, err := time.ParseDuration(Since)
		if err != nil {
			return fmt.Errorf("Invalid --since value: %v\n", err)
		}
	}
	if SinceTime != "" {
		_, err := time.Parse(time.DateTime, SinceTime)
		if err != nil {
			return fmt.Errorf("Invalid --since-time value: %v\n", err)
		}
	}
	if Since != "" && SinceTime != "" {
		return fmt.Errorf("Only one of --since-time or --since may be used.")
	}

	return nil
}
