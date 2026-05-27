package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/Masterminds/semver/v3"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/execute"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/find"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/logs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/registry"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/templates"
)

const (
	// reportFile is the build report file written by delivery-kit.
	reportFile = "build_report.json"

	// commandCli is the delivery-kit entrypoint command.
	commandCli execute.Command = "d8"

	// argDeliveryPlugin selects the delivery-kit d8 plugin.
	argDeliveryPlugin execute.Arg = "delivery-kit"
	// argBuild selects the build subcommand.
	argBuild execute.Arg = "build"
	// argContainerRegistry selects the container registry subcommand group.
	argContainerRegistry execute.Arg = "cr"
	// argLogin selects the registry login subcommand.
	argLogin execute.Arg = "login"

	// flagPlatform pins the target build platform.
	flagPlatform execute.Arg = "--platform=linux/amd64"
	// flagSaveBuildReport enables writing build metadata to disk.
	flagSaveBuildReport execute.Arg = "--save-build-report"
	// flagBuildReportPath sets the build report output path.
	flagBuildReportPath execute.Arg = "--build-report-path=" + reportFile
	// flagSkipSpecStage skips package image publishing for local builds.
	flagSkipSpecStage execute.Arg = "--skip-image-spec-stage"
	// flagRegistryUsername passes the registry username.
	flagRegistryUsername execute.Arg = "--username"
	// flagRegistryPassword passes the registry password or token.
	flagRegistryPassword execute.Arg = "--password"
	// flagDevBuild enables development build mode.
	flagDevBuild execute.Arg = "--dev"

	// envPackageVersion is consumed by werf templates as the package version.
	envPackageVersion = "PACKAGE_TAG"
	// envWerfRepository points werf to the destination image repository.
	envWerfRepository = "WERF_REPO"
	// envSignManifest enables signing of image manifests.
	envSignManifest = "WERF_SIGN_MANIFEST"
	// envSignELFFiles enables signing of ELF files.
	envSignELFFiles = "WERF_SIGN_ELF_FILES"
	// envBsignELFFiles enables signing of ELF files with bsign (for Astra Linux ZPS).
	envBsignELFFiles = "WERF_BSIGN_ELF_FILES"
	// envAnnotateLayersWithDmVerityRootHash enables annotating image layers with dm-verity root hashes.
	envAnnotateLayersWithDmVerityRootHash = "WERF_ANNOTATE_LAYERS_WITH_DM_VERITY_ROOT_HASH"
	// envWerfSignCert is the certificate used for signing images (file path or Base64-encoded value).
	envWerfSignCert = "WERF_SIGN_CERT"
	// envWerfSignKey is the private key used for signing (file path, Base64-encoded value, or Vault URL, e.g. hashivault://dh-2025-aug-ec).
	envWerfSignKey = "WERF_SIGN_KEY"
	// envSignIntermediates holds intermediate certificates used in the signing chain (file path or Base64-encoded value).
	envSignIntermediates = "WERF_SIGN_INTERMEDIATES"

	// imagePackage is the build report key for the package image.
	imagePackage = "package"
	// imageRelease is the build report key for the release image.
	imageRelease = "release"

	// helmignoreFile is generated during build and consumed by Helm packaging.
	helmignoreFile = ".helmignore"

	// chartsDir is the root directory containing chart dependencies.
	chartsDir = "charts"

	// templatesDir is the root directory containing Helm templates.
	templatesDir = "templates"
)

// Options configures registry authentication, build behavior, and image signing.
type Options struct {
	// Credentials contains registry destination and authentication settings.
	Credentials Credentials
	// Force allows rebuilding and publishing a version that already exists.
	Force bool
	// Debug keeps generated build files in the package root after build.
	Debug bool
	// Sign configures image signing during the build.
	Sign SignOptions
}

// SignOptions configures optional image signing.
type SignOptions struct {
	// Enabled turns on image manifest signing.
	Enabled bool
	// Cert is a signing certificate path or base64-encoded certificate.
	Cert string
	// Key is a signing key path, base64-encoded key, or vault path.
	Key string
}

// Credentials configures registry destination and authentication.
type Credentials struct {
	// Repository is the target container registry repository.
	Repository string
	// Username is the registry login username.
	Username string
	// Token is the registry login password or token.
	Token string
}

// buildReport contains image metadata emitted by delivery-kit.
type buildReport struct {
	Images map[string]imageReport `json:"Images"`
}

// imageReport contains the built Docker image reference for one image.
type imageReport struct {
	DockerImageName string `json:"DockerImageName"`
}

// werfTemplateData contains values rendered into build-time werf templates.
type werfTemplateData struct {
	HelmIgnored []string
}

// Build orchestrates the complete package build and publish workflow.
// It authenticates with the registry, builds images, and publishes versioned artifacts.
func Build(ctx context.Context, version string, opts Options, logger *logs.Logger) error {
	path, err := find.PackageDir()
	if err != nil {
		return err
	}

	pkg, err := getPackageNameByDir(path)
	if err != nil {
		return fmt.Errorf("get package name: %w", err)
	}

	// Validate semantic version
	if _, err = semver.NewVersion(version); err != nil {
		return fmt.Errorf("invalid semantic version '%s': %w", version, err)
	}

	// Construct full repository path with package name
	repo := opts.Credentials.Repository
	if len(repo) > 0 {
		repo = fmt.Sprintf("%s/%s", repo, pkg)
	}

	if len(opts.Credentials.Username) > 0 && len(opts.Credentials.Token) > 0 {
		logger.Info("✨ Login registry '%s'", repo)

		if err = login(ctx, repo, opts.Credentials.Username, opts.Credentials.Token); err != nil {
			return fmt.Errorf("login registry: %w", err)
		}
	}

	repoLog := "local"
	if len(repo) > 0 {
		repoLog = repo
		if err = registry.Exists(ctx, fmt.Sprintf("%s:%s", repo, version)); !opts.Force && err == nil {
			logger.Info("✅ Version '%s' already exists in the registry", version)
			return nil
		}
	}

	ignored, err := getHelmIgnored(path)
	if err != nil {
		return fmt.Errorf("get helm ignored: %w", err)
	}

	if err = templates.Render(templates.WerfFS, path, templates.Options{Data: werfTemplateData{HelmIgnored: ignored}}); err != nil {
		return fmt.Errorf("render templates: %w", err)
	}

	defer func() {
		if !opts.Debug {
			if err = templates.Clean(templates.WerfFS, path); err != nil {
				logger.Error("failed to clean templates: %s", err.Error())
			}
		}
	}()

	if opts.Sign.Enabled {
		logger.Info("✨ Signing images with certificate...")
	}

	logger.Info("✨ Build and push images to '%s'...", repoLog)

	if err = build(ctx, repo, path, version, opts.Sign); err != nil {
		return fmt.Errorf("failed to build package: %w", err)
	}

	logger.Info("✅ Images built and pushed")

	// Skip publishing steps for local builds
	if repoLog != "local" {
		if err = registry.PushPackageIndex(ctx, repo); err != nil {
			return fmt.Errorf("failed to register index: %w", err)
		}

		logger.Info("✨ Publish version '%s'...", version)

		if err = publishVersionImage(ctx, repo, version, path); err != nil {
			return fmt.Errorf("failed to publish version: %w", err)
		}
	}

	logger.Info("✅ Package '%s' built", pkg)

	return nil
}

// login authenticates with the container registry using the d8 delivery-kit CLI.
func login(ctx context.Context, registry, username, token string) error {
	args := []execute.Arg{
		argDeliveryPlugin,
		argContainerRegistry,
		argLogin,

		execute.Arg(registry),

		flagRegistryUsername,
		execute.Arg(username),

		flagRegistryPassword,
		execute.Arg(token),
	}

	return commandCli.Execute(ctx, execute.WithArgs(args...))
}

// build executes the package build process using d8 delivery-kit.
// For local builds, it skips the image-spec-stage; for registry builds, it sets WERF_REPO.
func build(ctx context.Context, registry, packageDir, version string, signOpts SignOptions) error {
	args := []execute.Arg{
		argDeliveryPlugin,
		argBuild,
		flagPlatform,
		flagSaveBuildReport,
		flagBuildReportPath,
		flagDevBuild,
	}

	env := []execute.Env{
		execute.NewEnv(envPackageVersion, version),
	}

	if signOpts.Enabled {
		env = append(
			env,
			execute.NewEnv(envSignManifest, "1"),
			execute.NewEnv(envAnnotateLayersWithDmVerityRootHash, "1"),
			execute.NewEnv(envWerfSignCert, signOpts.Cert),
			execute.NewEnv(envWerfSignKey, signOpts.Key),
			// Intermediates are required, but we may not have them
			execute.NewEnv(envSignIntermediates, signOpts.Cert),
			// We don't need to sign ELF files now
			execute.NewEnv(envSignELFFiles, "0"),
			execute.NewEnv(envBsignELFFiles, "0"),
		)
	}

	if len(registry) > 0 {
		env = append(env, execute.NewEnv(envWerfRepository, registry))
	} else {
		// Skip image spec stage for local builds
		args = append(args, flagSkipSpecStage)
	}

	return commandCli.Execute(ctx, execute.WithArgs(args...), execute.WithEnv(env...), execute.WithPath(packageDir))
}

// publishVersionImage reads the build report and copies bundle and release images to their final destinations.
// Bundle image is tagged as repo:version, release image as repo/version:version.
func publishVersionImage(ctx context.Context, repo, version, path string) error {
	raw, err := os.ReadFile(filepath.Join(path, reportFile))
	if err != nil {
		return fmt.Errorf("failed to read report file: %w", err)
	}

	report := new(buildReport)
	if err = json.Unmarshal(raw, report); err != nil {
		return fmt.Errorf("failed to unmarshal build report: %w", err)
	}

	// Copy bundle image to repo:version
	bundle, ok := report.Images[imagePackage]
	if !ok {
		return fmt.Errorf("failed to find bundle image")
	}

	src := bundle.DockerImageName
	dest := fmt.Sprintf("%s:%s", repo, version)

	if err = registry.Copy(ctx, src, dest); err != nil {
		return fmt.Errorf("failed to copy image: %w", err)
	}

	// Copy release image to repo/version:version
	release, ok := report.Images[imageRelease]
	if !ok {
		return fmt.Errorf("failed to find release image")
	}

	src = release.DockerImageName
	dest = fmt.Sprintf("%s/version:%s", repo, version)

	if err = registry.Copy(ctx, src, dest); err != nil {
		return fmt.Errorf("failed to copy image: %w", err)
	}

	return nil
}

// getPackageNameByDir parses package definition from package dir and returns the name.
func getPackageNameByDir(path string) (string, error) {
	content, err := os.ReadFile(filepath.Join(path, "package.yaml"))
	if err != nil {
		return "", fmt.Errorf("read package.yaml: %w", err)
	}

	// definition contains the package fields needed to route build artifacts.
	type definition struct {
		Name string `yaml:"name" json:"name"`
	}

	def := new(definition)
	if err = yaml.Unmarshal(content, def); err != nil {
		return "", fmt.Errorf("unmarshal package.yaml: %w", err)
	}

	if def.Name == "" {
		return "", fmt.Errorf("name field missing in package.yaml")
	}

	return def.Name, nil
}

// getHelmIgnored returns package root entries excluded from Helm packages.
func getHelmIgnored(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read package dir: %w", err)
	}

	ignored := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == helmignoreFile || name == chartsDir || name == templatesDir {
			continue
		}

		if entry.IsDir() {
			name += "/"
		}

		ignored = append(ignored, name)
	}

	sort.Strings(ignored)

	return ignored, nil
}
