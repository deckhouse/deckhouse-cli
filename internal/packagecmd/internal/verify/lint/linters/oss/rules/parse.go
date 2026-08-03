package rules

// Rule purpose: require oss.yaml to be valid YAML, since every other oss rule inspects the parsed document.

// ParseRuleID is the stable identifier used to reference this rule in configuration.
// The check itself lives in the linter: parsing is what gates the other rules, so it
// cannot be expressed as a rule over an already-parsed document.
const ParseRuleID = "parse"
