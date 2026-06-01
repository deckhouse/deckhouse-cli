/*
Copyright 2025 Flant JSC

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

package cmd

import (
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/tools/sigmigrate"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestAddFlags_DefaultThreadsMatchesCoreConstant(t *testing.T) {
	flags := pflag.NewFlagSet("sig-migrate", pflag.ContinueOnError)
	addFlags(flags)

	threads, err := flags.GetInt("threads")
	require.NoError(t, err)
	require.Equal(t, sigmigrate.DefaultWorkerCount, threads)
}

func TestAddFlags_DefaultLogLevel(t *testing.T) {
	flags := pflag.NewFlagSet("sig-migrate", pflag.ContinueOnError)
	addFlags(flags)

	logLevel, err := flags.GetString("log-level")
	require.NoError(t, err)
	require.Equal(t, "DEBUG", logLevel)
}
