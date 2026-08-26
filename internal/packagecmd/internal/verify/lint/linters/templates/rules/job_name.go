package rules

import (
	"context"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: enforce the package convention that Job names do not exceed 52 characters.

const (
	// JobNameRuleID is the stable identifier used to reference this rule in diagnostics.
	JobNameRuleID = "job-name"
	// maxJobNameLength is the maximum allowed length of a Job name.
	maxJobNameLength = 23

	cronJobKind = "CronJob"
	jobKind     = "Job"
)

// JobNameRule asserts every Job name is at most 52 characters long.
type JobNameRule struct {
	collector *diag.Collector
	objects   []render.Object
}

// NewJobNameRule constructs a JobNameRule scoped to its rule identifier.
func NewJobNameRule(objects []render.Object, collector *diag.Collector) *JobNameRule {
	return &JobNameRule{
		collector: collector.With(diag.RuleID(JobNameRuleID)),
		objects:   objects,
	}
}

// Check verifies every Job name is at most 52 characters long.
func (r *JobNameRule) Check(_ context.Context) {
	for _, obj := range r.objects {
		if obj.GetKind() != cronJobKind && obj.GetKind() != jobKind {
			continue
		}

		name, ok := strings.CutPrefix(obj.GetName(), d8aPrefix)
		if !ok {
			continue
		}

		name, ok = strings.CutPrefix(name, instancePrefix)
		if !ok {
			continue
		}

		if len(name) <= maxJobNameLength {
			continue
		}

		r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
			diag.Value(name),
		).Error("job name must not exceed %d characters; got %d", maxJobNameLength, len(name))
	}

	r.collector.Commit()
}
