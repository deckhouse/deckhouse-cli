// Package settings maps the on-disk .pkglint.yaml document onto typed configuration
// and converts it into runtime-ready linter severities.
//
// The document is grouped by verification scope. `static` configures linting of a
// package directory on disk; `remote.bundle` and `remote.release` configure linting
// of the two images a published package consists of. Every scope carries a full,
// independent LintersSettings tree because the three sources hold different files
// and therefore warrant different severities.
package settings

// Config is the decoded .pkglint.yaml document.
type Config struct {
	// Version identifies the .pkglint.yaml schema version.
	Version string `mapstructure:"version"`

	// Static contains settings applied when verifying a package directory on disk.
	Static ScopeSettings `mapstructure:"static"`

	// Remote contains settings applied when verifying a package published to a registry.
	Remote RemoteSettings `mapstructure:"remote"`
}

// RemoteSettings groups the per-image settings used by remote verification.
// A published package is two images with different contents, so each gets its own scope.
type RemoteSettings struct {
	// Bundle contains settings for the bundle image, published as <repository>:<version>.
	// It carries the renderable package: charts, templates, openapi, docs and metadata.
	Bundle ScopeSettings `mapstructure:"bundle"`

	// Release contains settings for the release image, published as <repository>/release:<release>.
	// It carries release metadata only: version.json, changelog, openapi, docs and the icon.
	Release ScopeSettings `mapstructure:"release"`
}

// ScopeSettings holds the linter configuration for one verification scope.
type ScopeSettings struct {
	// Linters contains settings grouped by linter.
	Linters LintersSettings `mapstructure:"linters"`
}

// LintersSettings groups configuration for all supported linters.
// A zero value is meaningful: every field decodes to its documented default severity,
// so a scope omitted from .pkglint.yaml behaves exactly like a scope left at defaults.
type LintersSettings struct {
	// Templates contains settings for application template checks.
	Templates TemplatesSettings `mapstructure:"templates"`

	// Documentation contains settings for package documentation checks.
	Documentation DocumentationSettings `mapstructure:"docs"`

	// Images contains settings for Docker image checks (patches, etc.).
	Images ImagesSettings `mapstructure:"images"`

	// Icon contains settings for package-icon content checks.
	Icon IconSettings `mapstructure:"icon"`

	// OSS contains settings for optional oss.yaml metadata checks.
	OSS OSSSettings `mapstructure:"oss"`
}

// TemplatesSettings configures the templates linter and its rules.
type TemplatesSettings struct {
	// Impact sets the maximum severity emitted by the templates linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule templates linter settings.
	Rules TemplatesRulesSettings `mapstructure:"rules"`
}

// TemplatesRulesSettings configures individual templates linter rules.
// The instance-prefix, instance-namespace and job-name rules are intentionally not exposed
// here; they encode hard contracts and are not user-tunable.
type TemplatesRulesSettings struct {
	// PDB configures checks that every pod controller is covered by a PodDisruptionBudget.
	PDB RuleSettings `mapstructure:"pdb"`
	// ServicePort configures checks that Service ports use named (non-numeric) target ports.
	ServicePort RuleSettings `mapstructure:"service-port"`
	// VPA configures checks that every pod controller has a matching VerticalPodAutoscaler.
	VPA RuleSettings `mapstructure:"vpa"`
}

// DocumentationSettings configures the documentation linter and its rules.
type DocumentationSettings struct {
	// Impact sets the maximum severity emitted by the documentation linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule documentation linter settings.
	Rules DocumentationRulesSettings `mapstructure:"rules"`
}

// DocumentationRulesSettings configures individual documentation linter rules.
type DocumentationRulesSettings struct {
	// Readme configures checks that require docs/README.md.
	Readme RuleSettings `mapstructure:"readme"`
	// Bilingual configures checks that require Russian translations.
	Bilingual RuleSettings `mapstructure:"bilingual"`
	// CyrillicInEnglish configures checks that reject cyrillic text in English docs.
	CyrillicInEnglish RuleSettings `mapstructure:"cyrillic-in-english"`
}

// ImagesSettings configures the images linter and its rules.
type ImagesSettings struct {
	// Impact sets the maximum severity emitted by the images linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule images linter settings.
	Rules ImagesRulesSettings `mapstructure:"rules"`
}

// ImagesRulesSettings configures individual images linter rules.
type ImagesRulesSettings struct {
	// Patches configures checks that validate image patch layout and documentation.
	Patches RuleSettings `mapstructure:"patches"`
	// ImageName configures checks that reject underscores in image names.
	ImageName RuleSettings `mapstructure:"image-name"`
}

// IconSettings configures the icon linter and its rules.
type IconSettings struct {
	// Impact sets the maximum severity emitted by the icon linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule icon linter settings.
	Rules IconRulesSettings `mapstructure:"rules"`
}

// IconRulesSettings configures individual icon linter rules.
type IconRulesSettings struct {
	// Ext configures checks that validate the icon content matches its extension.
	Ext RuleSettings `mapstructure:"ext"`
	// Size configures checks that cap the icon file size on disk.
	Size RuleSettings `mapstructure:"size"`
	// Shape configures checks that cap the rendered icon dimensions.
	Shape RuleSettings `mapstructure:"shape"`
}

// OSSSettings configures the oss linter and its rules.
type OSSSettings struct {
	// Impact sets the maximum severity emitted by the oss linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule oss linter settings.
	Rules OSSRulesSettings `mapstructure:"rules"`
}

// OSSRulesSettings configures individual oss linter rules.
type OSSRulesSettings struct {
	// Parse configures checks that oss.yaml is valid YAML.
	Parse RuleSettings `mapstructure:"parse"`
	// Fields configures checks that required component fields are not empty.
	Fields RuleSettings `mapstructure:"fields"`
	// Version configures checks for version and versions field usage.
	Version RuleSettings `mapstructure:"version"`
}

// RuleSettings configures a single rule.
type RuleSettings struct {
	// Impact sets the maximum severity emitted by the rule.
	Impact string `mapstructure:"impact"`
}
