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

package push

import (
	"os"
	"path/filepath"

	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVarP(
		&RegistryUsername,
		"registry-login",
		"u",
		os.Getenv("D8_MIRROR_REGISTRY_LOGIN"),
		"Username to log into the target registry.",
	)
	flagSet.StringVarP(
		&RegistryPassword,
		"registry-password",
		"p",
		os.Getenv("D8_MIRROR_REGISTRY_PASSWORD"),
		"Password to log into the target registry.",
	)
	flagSet.BoolVar(
		&TLSSkipVerify,
		"tls-skip-verify",
		false,
		"Disable TLS certificate validation.",
	)
	flagSet.BoolVar(
		&Insecure,
		"insecure",
		false,
		"Interact with registries over HTTP.",
	)
	flagSet.StringVar(
		&TempDir,
		"tmp-dir",
		filepath.Join(os.TempDir(), "d8", "mirror"),
		"Temporary directory to use for image pushing",
	)
}
