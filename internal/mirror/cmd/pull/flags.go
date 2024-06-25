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
	"os"

	"github.com/spf13/pflag"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&SourceRegistryRepo,
		"source",
		enterpriseEditionRepo,
		"Source registry to pull Deckhouse images from.",
	)
	flagSet.StringVar(
		&SourceRegistryLogin,
		"source-login",
		os.Getenv("D8_MIRROR_SOURCE_LOGIN"),
		"Source registry login.",
	)
	flagSet.StringVar(
		&SourceRegistryPassword,
		"source-password",
		os.Getenv("D8_MIRROR_SOURCE_PASSWORD"),
		"Source registry password.",
	)
	flagSet.StringVarP(
		&DeckhouseLicenseToken,
		"license",
		"l",
		os.Getenv("D8_MIRROR_LICENSE_TOKEN"),
		"Deckhouse license key. Shortcut for --source-login=license-token --source-password=<>.",
	)
	flagSet.StringVarP(
		&minVersionString,
		"min-version",
		"m",
		"",
		"Minimal Deckhouse release to copy. Ignored if above current Rock Solid release. Conflicts with --release.",
	)
	flagSet.StringVar(
		&specificReleaseString,
		"release",
		"",
		"Specific Deckhouse release to copy. Conflicts with --min-version. WARNING!: Clusters installed with this option will not be able to automatically update due to lack of release-channels information in bundle and, as such, will require special attention and manual intervention during updates.",
	)
	flagSet.Int64VarP(
		&ImagesBundleChunkSizeGB,
		"images-bundle-chunk-size",
		"c",
		0,
		"Split resulting bundle file into chunks of at most N gigabytes",
	)
	flagSet.BoolVar(
		&DoGOSTDigest,
		"gost-digest",
		false,
		"Calculate GOST R 34.11-2012 STREEBOG digest for downloaded bundle",
	)
	flagSet.BoolVar(
		&DontContinuePartialPull,
		"no-pull-resume",
		false,
		"Do not continue last unfinished pull operation and start from scratch.",
	)
	flagSet.BoolVar(
		&NoModules,
		"no-modules",
		false,
		"Do not pull Deckhouse modules into bundle.",
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
}
