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
		&SinceVersionString,
		"since-version",
		"",
		"Minimal Deckhouse release to pull. Ignored if above current Rock Solid release. Conflicts with --deckhouse-tag.",
	)
	flagSet.StringVar(
		&PlatformConstraintString,
		"include-platform",
		"",
		`Select platform releases to download by a semver constraint expression, using the same dialect as --include-module's version part.
Conflicts with --since-version and --deckhouse-tag.

Semver constraints (caret, tilde, range) keep only the highest patch in each (major, minor) series, mirroring the release-discovery rules used for full pulls.
Versions explicitly named with an inclusive boundary operator (>= or <=) are always preserved — that boundary is part of the user's request and must round-trip even when a newer patch exists in the same minor.
Use the exact-tag form (=) when you need to pin a specific tag, optionally propagating it to a release channel via the +channel suffix.

Examples (available platform versions: v1.63.x, v1.64.x, v1.65.x, v1.66.x, v1.67.x, v1.68.x, v1.69.x, v1.70.x, v1.71.x):

--include-platform ">=1.64 <=1.68" → bounded range: latest patch per minor in v1.64..v1.68, anchors v1.64.0 and v1.68.0 always preserved if present in the registry.

--include-platform "~1.65.0" → semver ~ constraint (>=1.65.0 <1.66.0): latest v1.65.x patch only.

--include-platform "^1.65.0" → semver ^ constraint (>=1.65.0 <2.0.0): latest patch per minor starting at v1.65.x.

--include-platform "1.65.0" → implicit caret (^1.65.0): same as above; shorthand kept for parity with --include-module.

--include-platform "=v1.65.3" → exact-tag pin: only v1.65.3 is pulled and propagated to all default release channels, just like --deckhouse-tag.

--include-platform "=v1.65.3+stable" → exact-tag pin with channel suffix: only v1.65.3 is pulled (channel propagation matches --deckhouse-tag).`,
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
		`Whitelist specific modules for downloading. Use one flag per each module. Disables blacklisting by --exclude-module."

Semver constraints (caret, tilde, range) keep only the highest patch in each (major, minor) series, mirroring how platform releases are discovered.
Versions explicitly named with an inclusive boundary operator (>= or <=) are always preserved — that boundary is part of the user's request and must round-trip even when a newer patch exists in the same minor.
Use the exact-tag form (=) when you need a specific older patch unconditionally.

Example:
Available versions for <module-name>: v1.0.0, v1.1.0, v1.2.0, v1.3.0, v1.3.3, v1.4.0, v1.4.1

module-name@1.3.0 → semver ^ constraint (^1.3.0): keep latest patch per minor — includes v1.3.3 (1.3.x) and v1.4.1 (1.4.x). Versions currently pinned by release channels are pulled in addition.

module-name@~1.3.0 → semver ~ constraint (>=1.3.0 <1.4.0): keep latest patch per minor in range — includes v1.3.3. Versions currently pinned by release channels are pulled in addition.

module-name@>=1.3.0 → range constraint with explicit >= anchor: keep latest patch per minor AND the named anchor v1.3.0 — includes v1.3.0 (anchor), v1.3.3 (1.3.x latest), v1.4.1 (1.4.x latest).

module-name@>=1.3.0 <=1.4.0 → both anchors honoured: includes v1.3.0, v1.3.3, v1.4.0; v1.4.1 is excluded by the upper bound.

module-name@=v1.3.0 → exact tag match: include only v1.3.0 and publish it to all release channels (alpha, beta, early-access, stable, rock-solid).

module-name@=bobV1 → exact tag match: include only bobV1 and publish it to all release channels (alpha, beta, early-access, stable, rock-solid).

module-name@=v1.3.0+stable → exact tag match: include only v1.3.0 and and publish it to stable channel
		`,
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
	flagSet.StringArrayVar(
		&PackagesWhitelist,
		"include-package",
		nil,
		`Whitelist specific packages for downloading. Use one flag per each package. Disables blacklisting by --exclude-package.

Packages are mirrored exactly like modules (same name@version constraint dialect), but live under the packages/ registry segment with their release metadata under packages/<name>/version.`,
	)
	flagSet.StringArrayVar(
		&PackagesBlacklist,
		"exclude-package",
		nil,
		`Blacklist specific packages from downloading. Format is "package-name[@version]". Use one flag per each package. Overridden by use of --include-package.`,
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
