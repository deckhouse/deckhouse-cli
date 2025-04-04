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

package pull

import (
	"os"

	"github.com/spf13/pflag"
)

const (
	deckhouseRegistryHost     = "registry.deckhouse.ru"
	enterpriseEditionRepoPath = "/deckhouse/ee"

	enterpriseEditionRepo = deckhouseRegistryHost + enterpriseEditionRepoPath
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
	flagSet.StringVar(
		&sinceVersionString,
		"since-version",
		"",
		"Minimal Deckhouse release to pull. Ignored if above current Rock Solid release. Conflicts with --deckhouse-tag.",
	)
	flagSet.StringVar(
		&DeckhouseTag,
		"deckhouse-tag",
		"",
		"Specific Deckhouse build tag to pull. Conflicts with --since-version. WARNING!: Clusters installed with this option will not be able to automatically update due to lack of release-channels information in bundle and, as such, will require special attention and manual intervention during updates.",
	)
	flagSet.StringArrayVarP(
		&ModulesWhitelist,
		"include-module",
		"i",
		nil,
		`Whitelist specific modules for downloading. Format is "module-name[@version]". Use one flag per each module. Disables blacklisting by --exclude-module."`,
	)
	flagSet.StringArrayVarP(
		&ModulesBlacklist,
		"exclude-module",
		"e",
		nil,
		`Blacklist specific modules from downloading. Format is "module-name[@version]". Use one flag per each module. Overridden by use of --include-module."`,
	)
	flagSet.StringVar(
		&ModulesPathSuffix,
		"modules-path-suffix",
		"/modules",
		"Suffix to append to source repo path to locate modules.",
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
		&ForcePull,
		"force",
		false,
		"Overwrite existing bundle packages if they are conflicting with current pull operation.",
	)
	flagSet.BoolVar(
		&NoPullResume,
		"no-pull-resume",
		false,
		"Do not continue last unfinished pull operation and start from scratch.",
	)
	flagSet.BoolVar(
		&NoPlatform,
		"no-platform",
		false,
		"Do not pull Deckhouse Kubernetes Platform into bundle.",
	)
	flagSet.BoolVar(
		&NoSecurityDB,
		"no-security-db",
		false,
		"Do not pull security databases into bundle.",
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
	flagSet.StringVar(
		&TempDir,
		"tmp-dir",
		"",
		"Path to a temporary directory to use for image pulling and pushing. All processing is done in this directory, so make sure there is enough free disk space to accommodate the entire bundle you are downloading;",
	)
}
