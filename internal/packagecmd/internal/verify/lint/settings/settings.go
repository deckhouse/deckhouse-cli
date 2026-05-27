package settings

// Config describes lint settings loaded from a package .pkglint.yaml file.
type Config struct {
	// Version identifies the .pkglint.yaml schema version.
	Version string `mapstructure:"version"`

	// Linters contains settings grouped by linter.
	Linters LintersSettings `mapstructure:"linters"`
}

// LintersSettings groups configuration for all supported linters.
type LintersSettings struct {
	// Layout contains settings for application layout checks.
	Layout LayoutSettings `mapstructure:"layout"`

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

// LayoutSettings configures the layout linter and its rules.
type LayoutSettings struct {
	// Impact sets the maximum severity emitted by the layout linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule layout linter settings.
	Rules LayoutRulesSettings `mapstructure:"rules"`
}

// LayoutRulesSettings configures individual layout linter rules.
type LayoutRulesSettings struct {
	// NoWerf configures checks that reject Werf files in application packages.
	NoWerf RuleSettings `mapstructure:"no-werf"`
	// NoChart configures checks that report Helm chart metadata in the package root.
	NoChart RuleSettings `mapstructure:"no-chart"`
	// NoHelmignore configures checks that reject committed .helmignore files.
	NoHelmignore RuleSettings `mapstructure:"no-helmignore"`
	// Gitignore configures checks that require .gitignore in the package root.
	Gitignore RuleSettings `mapstructure:"gitignore"`
	// Changelog configures checks that require changelog.yaml in the package root.
	Changelog RuleSettings `mapstructure:"changelog"`
	// Docs configures checks that require docs/ in the package root.
	Docs RuleSettings `mapstructure:"docs"`
	// Icon configures checks that require an icon.<ext> in the package root.
	Icon RuleSettings `mapstructure:"icon"`
}

// TemplatesSettings configures the templates linter and its rules.
type TemplatesSettings struct {
	// Impact sets the maximum severity emitted by the templates linter.
	Impact string `mapstructure:"impact"`
	// Rules contains per-rule templates linter settings.
	Rules TemplatesRulesSettings `mapstructure:"rules"`
}

// TemplatesRulesSettings configures individual templates linter rules.
// The instance-prefix and instance-namespace rules are intentionally not exposed here;
// they encode hard multi-instance contracts and are not user-tunable.
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
