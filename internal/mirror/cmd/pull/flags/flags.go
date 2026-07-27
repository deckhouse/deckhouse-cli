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

package flags

import (
	"fmt"
	"os"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/pflag"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
)

const (
	deckhouseRegistryHost     = "registry.deckhouse.ru"
	enterpriseEditionRepoPath = "/deckhouse/ee"

	EnterpriseEditionRepo = deckhouseRegistryHost + enterpriseEditionRepoPath
)

// CLI Parameters
var (
	TempDir string

	Insecure      bool
	TLSSkipVerify bool
	ForcePull     bool

	ImagesBundlePath        string
	ImagesBundleChunkSizeGB int64

	SinceVersionString string
	SinceVersion       *semver.Version

	PlatformConstraintString string
	PlatformConstraint       modules.VersionConstraint

	DeckhouseTag string
	InstallerTag string

	ModulesPathSuffix string
	ModulesWhitelist  []string
	ModulesBlacklist  []string

	PackagesWhitelist []string
	PackagesBlacklist []string

	SourceRegistryRepo     = EnterpriseEditionRepo // Fallback to EE if nothing was given as source.
	SourceRegistryLogin    string
	SourceRegistryPassword string
	DeckhouseLicenseToken  string

	DoGOSTDigest  bool
	NoPullResume  bool
	IgnoreSuspend bool

	NoPlatform      bool
	NoSecurityDB    bool
	NoModules       bool
	NoPackages      bool
	NoInstaller     bool
	OnlyExtraImages bool
	SkipVexImages   bool

	// ProxyRegistry switches platform/module release discovery from the
	// catalog-based ListTags path to a sequential probe of explicit
	// version tags. It exists for proxy/caching registries that do NOT
	// implement the registry catalog API but DO serve manifests for tags
	// they have cached. Requires --include-platform and/or --include-module
	// so the probe has a defined entry point — without those flags the
	// probe would have to start from 0.0.0 and the bundle would always
	// come back empty.
	ProxyRegistry bool

	DryRun bool

	// VerboseSummary lists every module and package in the end-of-pull summary
	// with its resolved versions (plus a VEX count when it has VEX attestations).
	// Without it, only the aggregate counts are printed. It changes the printout
	// only, not which images are pulled.
	VerboseSummary bool

	MirrorTimeout time.Duration = -1
)

func AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&SourceRegistryRepo,
		"source",
		EnterpriseEditionRepo,
		"Source registry to pull Deckhouse images from (format: registry-host[:port]/path).",
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
		&SinceVersionString,
		"since-version",
		"",
		"Minimal Deckhouse release to pull. Ignored if above current Rock Solid release. Conflicts with --deckhouse-tag.",
	)
	flagSet.StringVar(
		&PlatformConstraintString,
		"include-platform",
		"",
		`Select platform releases to download by a semver constraint. Same dialect as the version part of --include-module.
Conflicts with --since-version and --deckhouse-tag.

Always quote the value: > and < are shell redirections.

A constraint keeps the latest patch in each minor it covers. Versions named with >= or <= are kept as well. An exact tag (=) pins one release and propagates it to the release channels, just like --deckhouse-tag.

Given platform releases v1.63.x .. v1.71.x:
  ">=1.64 <=1.68"   latest patch per minor in v1.64..v1.68, plus v1.64.0 and v1.68.0
  "~1.65.0"         latest v1.65.x patch
  "^1.65.0"         latest patch per minor from v1.65.x up
  "1.65.0"          same as ^1.65.0 here: platform majors are always >= 1
  "=v1.65.3"        only v1.65.3, published to every release channel
  "=v1.65.3+stable" only v1.65.3, published to stable`,
	)
	flagSet.StringVar(
		&DeckhouseTag,
		"deckhouse-tag",
		"",
		"Specific Deckhouse build tag to pull. Conflicts with --since-version and --include-platform. If registry contains release channel image for specified tag, all release channels in the bundle will be pointed to it.",
	)
	flagSet.StringVar(
		&InstallerTag,
		"installer-tag",
		"latest",
		"Specific Deckhouse installer build tag to pull. If not specified, the latest tag for the installer will be pulled from the registry path.",
	)
	flagSet.StringArrayVarP(
		&ModulesWhitelist,
		"include-module",
		"i",
		nil,
		`Whitelist specific modules for downloading. Format is "module-name[@constraint]". Use one flag per each module. Disables blacklisting by --exclude-module.

Quote the whole value when the constraint uses >= or <=: --include-module "module-name@>=1.3.0". Unquoted, the shell takes > as a redirection and d8 receives a module with no version.

A constraint keeps the latest patch in each minor it covers, plus whatever the release channels point at. Versions named with >= or <= are kept as well. An exact tag (=) pins one tag and publishes it to the release channels.

Given v1.0.0, v1.1.0, v1.2.0, v1.3.0, v1.3.3, v1.4.0, v1.4.1:
  module-name                     only what the release channels point at, like a pull with no filters
  module-name@1.3.0               >=1.3.0 <2.0.0, same major line: v1.3.3, v1.4.1
  module-name@~1.3.0              >=1.3.0 <1.4.0: v1.3.3
  module-name@^1.3.0              >=1.3.0 <2.0.0: v1.3.3, v1.4.1
  "module-name@>=1.3.0"           the same, plus the named v1.3.0
  "module-name@>=1.3.0 <=1.4.0"   v1.3.0, v1.3.3, v1.4.0
  module-name@=v1.3.0             only v1.3.0, published to every release channel
  module-name@=v1.3.0+stable      only v1.3.0, published to stable
  module-name@=bobV1              only the bobV1 tag

For a 0.x module the bare form spans the whole 0.x line (0.4.0 means >=0.4.0 <1.0.0) while the caret locks the minor (^0.4.0 means >=0.4.0 <0.5.0).`,
	)
	flagSet.StringArrayVarP(
		&ModulesBlacklist,
		"exclude-module",
		"e",
		nil,
		`Blacklist specific modules from downloading. Format is "module-name[@constraint]", the same dialect as --include-module, quoting included. Use one flag per each module. Overridden by use of --include-module.`,
	)
	flagSet.StringVar(
		&ModulesPathSuffix,
		"modules-path-suffix",
		"/modules",
		"Suffix to append to source repo path to locate modules.",
	)
	flagSet.StringArrayVar(
		&PackagesWhitelist,
		"include-package",
		nil,
		`Whitelist specific packages for downloading. Format is "package-name[@constraint]", the same dialect as --include-module, quoting included. Use one flag per each package. Disables blacklisting by --exclude-package.

Packages live under the packages/ registry segment, with release metadata under packages/<name>/version.`,
	)
	flagSet.StringArrayVar(
		&PackagesBlacklist,
		"exclude-package",
		nil,
		`Blacklist specific packages from downloading. Format is "package-name[@constraint]", the same dialect as --include-module, quoting included. Use one flag per each package. Overridden by use of --include-package.`,
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
		&IgnoreSuspend,
		"ignore-suspend",
		false,
		"Ignore suspended release channels and continue mirroring. Use with caution.",
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
		&NoPackages,
		"no-packages",
		false,
		"Do not pull Deckhouse packages into bundle.",
	)
	flagSet.BoolVar(
		&NoInstaller,
		"no-installer",
		false,
		"Do not pull Deckhouse installer into bundle.",
	)
	flagSet.BoolVar(
		&OnlyExtraImages,
		"only-extra-images",
		false,
		"Pull only extra images for modules (additional images like security databases, scanners, etc.) without pulling main module images.",
	)
	flagSet.BoolVar(
		&SkipVexImages,
		"skip-vex-images",
		false,
		"Do not pull VEX images.",
	)
	flagSet.BoolVar(
		&ProxyRegistry,
		"proxy-registry",
		false,
		`Pull from a proxy/caching registry that does not implement the registry catalog API.

Instead of calling the registry's "list tags" endpoint (which proxy registries typically return empty), this mode probes individual tags by incrementing patch -> minor -> major from the version explicitly named via --include-platform / --include-module. The probe stops once both a new patch and a new minor of the current major fail to resolve, then attempts the next major; if that also fails the probe terminates and downloads what was discovered.

Requires --include-platform when platform is not skipped via --no-platform, and at least one --include-module when modules are not skipped via --no-modules. --exclude-module and --no-platform are respected.

Cannot be combined with --deckhouse-tag or --since-version (use --include-platform's lower bound instead).`,
	)
	flagSet.BoolVar(
		&DryRun,
		"dry-run",
		false,
		"Print what would be pulled without downloading any images. Useful for fast validation of flags and filters.",
	)
	flagSet.BoolVar(
		&VerboseSummary,
		"verbose-summary",
		false,
		"List every module and package in the end-of-pull summary with its resolved versions (and VEX count, when present), instead of just the totals. Output only - it does not change what is pulled.",
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

func ParseEnvironmentVariables() {
	if timeoutStr := os.Getenv("D8_MIRROR_TIMEOUT"); timeoutStr != "" {
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			// TODO: Add logger
			fmt.Println("Failed to parse timeout duration from environment variable D8_MIRROR_TIMEOUT: ", err)
		}

		if err == nil && timeout >= 0 {
			MirrorTimeout = timeout
		}
	}
}
