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
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/layout"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
)

// lintSettingsFile is a file that stores linters settings
var lintSettingsFile = ".pkglint.yaml"

// Root contains runtime-ready linter settings after defaults and config values are applied.
type Root struct {
	// Layout contains runtime settings for the application layout linter.
	Layout layout.LinterSettings
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

// LoadRoot loads lint settings and returns runtime-ready linter settings.
// When configPath is set, it reads exactly that file. Otherwise it searches for
// .pkglint.yaml from path through its parents and falls back to default settings.
func LoadRoot(path, configPath string) (*Root, error) {
	cfg := new(Config)
	if err := load(cfg, path, configPath); err != nil {
		var notFoundErr viper.ConfigFileNotFoundError
		if configPath == "" && errors.As(err, &notFoundErr) {
			return defaultLintersSettings(), nil
		}

		return nil, err
	}

	return remapLintersSettings(*cfg), nil
}

// defaultLintersSettings returns runtime linter settings used when no .pkglint.yaml file exists.
func defaultLintersSettings() *Root {
	r := new(Root)

	r.Layout.Impact = lint.Error.Ptr()
	r.Layout.RulesSettings.NoWerf.SetLevel("error", nil)
	r.Layout.RulesSettings.NoChart.SetLevel("warn", nil)
	r.Layout.RulesSettings.NoHelmignore.SetLevel("error", nil)
	r.Layout.RulesSettings.Gitignore.SetLevel("warn", nil)
	r.Layout.RulesSettings.Changelog.SetLevel("error", nil)
	r.Layout.RulesSettings.Docs.SetLevel("error", nil)
	r.Layout.RulesSettings.Icon.SetLevel("warn", nil)

	r.Templates.Impact = lint.Error.Ptr()
	r.Templates.RulesSettings.PDB.SetLevel("error", nil)
	r.Templates.RulesSettings.ServicePort.SetLevel("error", nil)
	r.Templates.RulesSettings.VPA.SetLevel("error", nil)

	r.Documentation.Impact = lint.Error.Ptr()
	r.Documentation.RulesSettings.Readme.SetLevel("error", nil)
	r.Documentation.RulesSettings.Bilingual.SetLevel("error", nil)
	r.Documentation.RulesSettings.CyrillicInEnglish.SetLevel("error", nil)

	r.Images.Impact = lint.Error.Ptr()
	r.Images.RulesSettings.Patches.SetLevel("error", nil)

	r.Icon.Impact = lint.Error.Ptr()
	r.Icon.RulesSettings.Ext.SetLevel("error", nil)
	r.Icon.RulesSettings.Size.SetLevel("error", nil)
	r.Icon.RulesSettings.Shape.SetLevel("error", nil)

	r.OSS.Impact = lint.Error.Ptr()
	r.OSS.RulesSettings.Parse.SetLevel("error", nil)
	r.OSS.RulesSettings.Fields.SetLevel("error", nil)
	r.OSS.RulesSettings.Version.SetLevel("error", nil)

	return r
}

// remapLintersSettings converts decoded package lint settings into runtime linter settings.
func remapLintersSettings(cfg Config) *Root {
	r := new(Root)

	r.Layout.SetLevel(cfg.Linters.Layout.Impact)
	r.Layout.RulesSettings.NoWerf.SetLevel(cfg.Linters.Layout.Rules.NoWerf.Impact, lint.Error.Ptr())
	r.Layout.RulesSettings.NoChart.SetLevel(cfg.Linters.Layout.Rules.NoChart.Impact, lint.Warn.Ptr())
	r.Layout.RulesSettings.NoHelmignore.SetLevel(cfg.Linters.Layout.Rules.NoHelmignore.Impact, lint.Error.Ptr())
	r.Layout.RulesSettings.Gitignore.SetLevel(cfg.Linters.Layout.Rules.Gitignore.Impact, lint.Warn.Ptr())
	r.Layout.RulesSettings.Changelog.SetLevel(cfg.Linters.Layout.Rules.Changelog.Impact, lint.Error.Ptr())
	r.Layout.RulesSettings.Docs.SetLevel(cfg.Linters.Layout.Rules.Docs.Impact, lint.Error.Ptr())
	r.Layout.RulesSettings.Icon.SetLevel(cfg.Linters.Layout.Rules.Icon.Impact, lint.Warn.Ptr())

	r.Templates.SetLevel(cfg.Linters.Templates.Impact)
	r.Templates.RulesSettings.PDB.SetLevel(cfg.Linters.Templates.Rules.PDB.Impact, lint.Error.Ptr())
	r.Templates.RulesSettings.ServicePort.SetLevel(cfg.Linters.Templates.Rules.ServicePort.Impact, lint.Error.Ptr())
	r.Templates.RulesSettings.VPA.SetLevel(cfg.Linters.Templates.Rules.VPA.Impact, lint.Error.Ptr())

	r.Documentation.SetLevel(cfg.Linters.Documentation.Impact)
	r.Documentation.RulesSettings.Readme.SetLevel(cfg.Linters.Documentation.Rules.Readme.Impact, lint.Error.Ptr())
	r.Documentation.RulesSettings.Bilingual.SetLevel(cfg.Linters.Documentation.Rules.Bilingual.Impact, lint.Error.Ptr())
	r.Documentation.RulesSettings.CyrillicInEnglish.SetLevel(cfg.Linters.Documentation.Rules.CyrillicInEnglish.Impact, lint.Error.Ptr())

	r.Images.SetLevel(cfg.Linters.Images.Impact)
	r.Images.RulesSettings.Patches.SetLevel(cfg.Linters.Images.Rules.Patches.Impact, lint.Error.Ptr())

	r.Icon.SetLevel(cfg.Linters.Icon.Impact)
	r.Icon.RulesSettings.Ext.SetLevel(cfg.Linters.Icon.Rules.Ext.Impact, lint.Error.Ptr())
	r.Icon.RulesSettings.Size.SetLevel(cfg.Linters.Icon.Rules.Size.Impact, lint.Error.Ptr())
	r.Icon.RulesSettings.Shape.SetLevel(cfg.Linters.Icon.Rules.Shape.Impact, lint.Error.Ptr())

	r.OSS.SetLevel(cfg.Linters.OSS.Impact)
	r.OSS.RulesSettings.Parse.SetLevel(cfg.Linters.OSS.Rules.Parse.Impact, lint.Error.Ptr())
	r.OSS.RulesSettings.Fields.SetLevel(cfg.Linters.OSS.Rules.Fields.Impact, lint.Error.Ptr())
	r.OSS.RulesSettings.Version.SetLevel(cfg.Linters.OSS.Rules.Version.Impact, lint.Error.Ptr())

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
