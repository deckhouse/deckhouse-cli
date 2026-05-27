package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss/model"
)

// VersionRuleID is the stable identifier used to reference this rule in configuration.
const VersionRuleID = "version"

// VersionRule validates version and versions usage in oss.yaml components.
type VersionRule struct {
	collector  *diag.Collector
	components []model.Component
}

// NewVersionRule constructs a VersionRule over parsed oss components.
func NewVersionRule(components []model.Component, collector *diag.Collector) *VersionRule {
	return &VersionRule{
		collector:  collector.With(diag.RuleID(VersionRuleID)),
		components: components,
	}
}

// Check validates version fields for each component.
func (r *VersionRule) Check(_ context.Context) {
	for idx, component := range r.components {
		componentID := model.ComponentObjectID(idx, component)
		collector := r.collector.With(diag.ObjectID(componentID))

		hasVersion := strings.TrimSpace(component.Version) != ""
		hasVersions := len(component.Versions) > 0

		if hasVersion && hasVersions {
			collector.Error("version and versions fields cannot be set at the same time")
			return
		}

		if !hasVersion && !hasVersions {
			collector.Error("either version or versions must be set")
			return
		}

		for versionIdx, version := range component.Versions {
			versionCollector := collector.With(diag.ObjectID(fmt.Sprintf("%s.versions[%d]", componentID, versionIdx)))

			if strings.TrimSpace(version.Name) == "" {
				versionCollector.Error("versions[%d].name must not be empty", versionIdx)
			}

			if strings.TrimSpace(version.Version) == "" {
				versionCollector.Error("versions[%d].version must not be empty", versionIdx)
			}
		}
	}
}
