package settings

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/docs"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/icon"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/images"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
)

// lintSettingsFile is a file that stores linters settings
var lintSettingsFile = ".pkglint.yaml"

// Scope names one verification target. Each scope selects an independent branch of
// the .pkglint.yaml document, so the same linter can carry different severities
// depending on whether it inspects a directory, a bundle image or a release image.
//
// The type is an alias of lint.Scope rather than its own: linters declare which scopes
// they are processed in, and lint is the only package all of them can import without a
// cycle. It is re-exported here so callers working with settings need not import both.
type Scope = lint.Scope

// Verification scopes, re-exported from lint.
const (
	ScopeStatic  = lint.ScopeStatic
	ScopeBundle  = lint.ScopeBundle
	ScopeRelease = lint.ScopeRelease
)

// Root contains runtime-ready linter settings for a single scope, after defaults
// and config values are applied.
type Root struct {
	// Templates contains runtime settings for the application templates linter.
	Templates templates.LinterSettings
	// Documentation contains runtime settings for the documentation linter.
	Documentation docs.LinterSettings
	// Images contains runtime settings for the Docker image linter.
	Images images.LinterSettings
	// Icon contains runtime settings for the package-icon linter.
	Icon icon.LinterSettings
	// OSS contains runtime settings for the oss metadata linter.
	OSS oss.LinterSettings
}

// Settings holds runtime-ready linter settings for every verification scope.
type Settings struct {
	roots map[Scope]*Root
}

// Scope returns the runtime settings for scope. Unknown scopes fall back to the
// static scope so a caller can never end up with nil settings.
func (s *Settings) Scope(scope Scope) *Root {
	if root, ok := s.roots[scope]; ok {
		return root
	}

	return s.roots[ScopeStatic]
}

// Load reads lint settings and returns runtime-ready settings for every scope.
// When configPath is set, it reads exactly that file. Otherwise it searches for
// .pkglint.yaml from dir through its parents and falls back to default settings.
func Load(dir, configPath string) (*Settings, error) {
	cfg := new(Config)
	if err := load(cfg, dir, configPath); err != nil {
		var notFoundErr viper.ConfigFileNotFoundError
		if configPath == "" && errors.As(err, &notFoundErr) {
			return newSettings(Config{}), nil
		}

		return nil, err
	}

	return newSettings(*cfg), nil
}

// Defaults returns the runtime settings a scope gets when .pkglint.yaml leaves it
// out entirely: every linter and every rule at its built-in severity. It is the
// single source the doc command reads default severities from, so documentation
// cannot drift from what verify actually falls back to.
func Defaults() *Root {
	return remapLintersSettings(LintersSettings{})
}

// newSettings converts a decoded config into per-scope runtime settings.
// A zero Config yields the default severities for every scope, because each
// unset Impact remaps to its documented per-rule default.
func newSettings(cfg Config) *Settings {
	return &Settings{
		roots: map[Scope]*Root{
			ScopeStatic:  remapLintersSettings(cfg.Static.Linters),
			ScopeBundle:  remapLintersSettings(cfg.Remote.Bundle.Linters),
			ScopeRelease: remapLintersSettings(cfg.Remote.Release.Linters),
		},
	}
}

// remapLintersSettings converts decoded linter settings into runtime linter settings,
// substituting each rule's default severity wherever the config leaves impact unset.
func remapLintersSettings(cfg LintersSettings) *Root {
	r := new(Root)

	r.Templates.SetLevel(cfg.Templates.Impact)
	r.Templates.RulesSettings.PDB.SetLevel(cfg.Templates.Rules.PDB.Impact, lint.Error.Ptr())
	r.Templates.RulesSettings.ServicePort.SetLevel(cfg.Templates.Rules.ServicePort.Impact, lint.Error.Ptr())
	r.Templates.RulesSettings.VPA.SetLevel(cfg.Templates.Rules.VPA.Impact, lint.Error.Ptr())

	r.Documentation.SetLevel(cfg.Documentation.Impact)
	r.Documentation.RulesSettings.Readme.SetLevel(cfg.Documentation.Rules.Readme.Impact, lint.Error.Ptr())
	r.Documentation.RulesSettings.Bilingual.SetLevel(cfg.Documentation.Rules.Bilingual.Impact, lint.Error.Ptr())
	r.Documentation.RulesSettings.CyrillicInEnglish.SetLevel(cfg.Documentation.Rules.CyrillicInEnglish.Impact, lint.Error.Ptr())

	r.Images.SetLevel(cfg.Images.Impact)
	r.Images.RulesSettings.Patches.SetLevel(cfg.Images.Rules.Patches.Impact, lint.Error.Ptr())
	r.Images.RulesSettings.ImageName.SetLevel(cfg.Images.Rules.ImageName.Impact, lint.Error.Ptr())

	r.Icon.SetLevel(cfg.Icon.Impact)
	r.Icon.RulesSettings.Ext.SetLevel(cfg.Icon.Rules.Ext.Impact, lint.Error.Ptr())
	r.Icon.RulesSettings.Size.SetLevel(cfg.Icon.Rules.Size.Impact, lint.Error.Ptr())
	r.Icon.RulesSettings.Shape.SetLevel(cfg.Icon.Rules.Shape.Impact, lint.Error.Ptr())

	r.OSS.SetLevel(cfg.OSS.Impact)
	r.OSS.RulesSettings.Parse.SetLevel(cfg.OSS.Rules.Parse.Impact, lint.Error.Ptr())
	r.OSS.RulesSettings.Fields.SetLevel(cfg.OSS.Rules.Fields.Impact, lint.Error.Ptr())
	r.OSS.RulesSettings.Version.SetLevel(cfg.OSS.Rules.Version.Impact, lint.Error.Ptr())

	return r
}

// load decodes lint settings into cfg.
// If configPath is set, it reads that file. Otherwise it searches dir and its
// parents for .pkglint.yaml. If no file is found, cfg is still unmarshaled so
// Viper defaults can be applied.
func load(cfg any, dir, configPath string) error {
	vi := viper.NewWithOptions()
	vi.SetConfigType("yaml")

	if configPath != "" {
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			absConfigPath = filepath.Clean(configPath)
		}

		vi.SetConfigFile(absConfigPath)

		return readConfig(vi, cfg, false)
	}

	absPath, err := filepath.Abs(dir)
	if err != nil {
		absPath = filepath.Clean(dir)
	}

	currentDir := absPath
	for {
		configPath := filepath.Join(currentDir, lintSettingsFile)
		if _, err := os.Stat(configPath); err == nil {
			vi.SetConfigFile(configPath)
			break
		}

		parent := filepath.Dir(currentDir)
		if currentDir == parent || parent == "" {
			break
		}

		currentDir = parent
	}

	if vi.ConfigFileUsed() == "" {
		return vi.Unmarshal(cfg, customDecoderHook())
	}

	return readConfig(vi, cfg, true)
}

// readConfig reads the selected Viper config file and decodes it into cfg.
func readConfig(vi *viper.Viper, cfg any, allowMissing bool) error {
	if err := vi.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if allowMissing && errors.As(err, &configFileNotFoundError) {
			return vi.Unmarshal(cfg, customDecoderHook())
		}

		return fmt.Errorf("can't read viper config: %w", err)
	}

	return vi.Unmarshal(cfg, customDecoderHook())
}

// customDecoderHook returns decoder hooks used when unmarshaling package lint settings.
func customDecoderHook() viper.DecoderConfigOption {
	return viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		mapstructure.TextUnmarshallerHookFunc(),
	))
}
