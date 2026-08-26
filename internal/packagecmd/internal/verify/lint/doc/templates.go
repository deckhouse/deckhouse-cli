package doc

import (
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates/rules"
)

// templatesDoc documents the templates linter.
var templatesDoc = Linter{
	ID:      templates.LinterID,
	Summary: "Kubernetes objects rendered from the package templates",
	Description: []string{
		"Renders the package chart with generated example values and checks the resulting Kubernetes objects, so findings point at the manifests a cluster would actually receive.",
		"The linter is skipped when the source carries no templates/ directory, which is the normal state of the release image.",
	},
	Rules: []Rule{
		{
			ID:      rules.InstancePrefixRuleID,
			Summary: "Requires every object name to start with the instance prefix",
			Description: []string{
				"Several instances of one application can run in a single cluster, so every rendered object must be named after its instance. Templates take the name from the Helm runtime context rather than hardcoding it.",
			},
			Reports: []string{
				"a rendered object name does not start with the verify-time instance prefix",
			},
			Example: Example{
				Reported: []string{
					"metadata:",
					"  name: server",
				},
				Accepted: []string{
					"metadata:",
					"  name: {{ .Application.Instance.Name }}-server",
				},
			},
			Fix:     "Prefix object names with {{ .Application.Instance.Name }} instead of writing a literal name.",
			Tunable: false,
		},
		{
			ID:      rules.InstanceNamespaceRuleID,
			Summary: "Forbids hardcoding metadata.namespace",
			Description: []string{
				"The runtime places rendered resources in the namespace of the instance. A namespace written into the template pins every instance to one namespace and breaks that placement.",
			},
			Reports: []string{
				"a rendered object sets metadata.namespace",
			},
			Example: Example{
				Reported: []string{
					"metadata:",
					"  name: {{ .Application.Instance.Name }}-server",
					"  namespace: my-app",
				},
				Accepted: []string{
					"metadata:",
					"  name: {{ .Application.Instance.Name }}-server",
				},
			},
			Fix:     "Remove metadata.namespace from the template and let the runtime inject it.",
			Tunable: false,
		},
		{
			ID:      rules.JobNameRuleID,
			Summary: "Restricts Job names to 52 characters",
			Description: []string{
				"Job names are limited to 52 characters by the package naming convention. The rendered name is checked after all template values and prefixes have been applied.",
			},
			Reports: []string{
				"a rendered Job has a metadata.name longer than 52 characters",
			},
			Example: Example{
				Reported: []string{
					"kind: Job",
					"metadata:",
					"  name: {{ .Application.Instance.Name }}-a-very-long-maintenance-job-name",
				},
				Accepted: []string{
					"kind: Job",
					"metadata:",
					"  name: {{ .Application.Instance.Name }}-maintenance",
				},
			},
			Fix:     "Shorten metadata.name to 52 characters or fewer, including values added during template rendering.",
			Tunable: false,
		},
		{
			ID:      rules.PDBRuleID,
			Summary: "Requires a PodDisruptionBudget for every pod controller",
			Description: []string{
				"Without a budget, voluntary disruptions such as a node drain can take every replica down at once.",
				"Deployments and StatefulSets are covered; DaemonSets are exempt because their pods follow the nodes.",
			},
			Reports: []string{
				"a Deployment or StatefulSet has no PodDisruptionBudget in its namespace",
				"no PodDisruptionBudget selector matches the pod template labels of the controller",
			},
			Example: Example{
				Reported: []string{
					"kind: Deployment",
					"spec:",
					"  template:",
					"    metadata:",
					"      labels:",
					"        app: {{ .Application.Instance.Name }}-server",
					"# no PodDisruptionBudget selects app: <instance>-server",
				},
				Accepted: []string{
					"kind: PodDisruptionBudget",
					"spec:",
					"  minAvailable: 1",
					"  selector:",
					"    matchLabels:",
					"      app: {{ .Application.Instance.Name }}-server",
				},
			},
			Fix:     "Add a PodDisruptionBudget whose selector matches the controller's pod template labels.",
			Tunable: true,
		},
		{
			ID:      rules.ServicePortRuleID,
			Summary: "Requires Service ports to target a named port",
			Description: []string{
				"A numeric targetPort duplicates the container port number in a second place. Naming the port lets a container change its port without silently breaking the Service.",
			},
			Reports: []string{
				"a Service declares a numeric spec.ports[*].targetPort",
			},
			Example: Example{
				Reported: []string{
					"kind: Service",
					"spec:",
					"  ports:",
					"    - port: 5678",
					"      targetPort: 5678",
				},
				Accepted: []string{
					"kind: Service",
					"spec:",
					"  ports:",
					"    - port: 5678",
					"      targetPort: http    # matches the container port name",
					"---",
					"ports:",
					"  - name: http",
					"    containerPort: 5678",
				},
			},
			Fix:     "Name the container port and reference that name from targetPort.",
			Tunable: true,
		},
		{
			ID:      rules.VPARuleID,
			Summary: "Requires a valid VerticalPodAutoscaler for every pod controller",
			Description: []string{
				"Resource requests are managed by VPA rather than pinned in the manifests, so every pod controller needs one with bounds for each of its containers.",
				"Deployments, StatefulSets and DaemonSets are covered. A VPA with updateMode Off is accepted as an explicit opt-out and its container policies are not matched against the controller.",
			},
			Reports: []string{
				"a pod controller has no VerticalPodAutoscaler targeting it",
				"a VerticalPodAutoscaler has no spec.targetRef, or one without a kind or name",
				"a VerticalPodAutoscaler has no spec.resourcePolicy.containerPolicies",
				"updateMode is Auto, which is deprecated, or is not one of Off, Initial, Recreate, InPlaceOrRecreate",
				"a container policy is missing or contradicts its CPU or memory bounds",
				"the container policies do not cover every container of the controller",
			},
			Example: Example{
				Reported: []string{
					"kind: VerticalPodAutoscaler",
					"spec:",
					"  targetRef:",
					"    kind: Deployment",
					"    name: {{ .Application.Instance.Name }}-server",
					"  updatePolicy:",
					"    updateMode: Auto        # deprecated",
					"  # no resourcePolicy.containerPolicies",
				},
				Accepted: []string{
					"kind: VerticalPodAutoscaler",
					"spec:",
					"  targetRef:",
					"    apiVersion: apps/v1",
					"    kind: Deployment",
					"    name: {{ .Application.Instance.Name }}-server",
					"  updatePolicy:",
					"    updateMode: InPlaceOrRecreate",
					"  resourcePolicy:",
					"    containerPolicies:",
					"      - containerName: server",
					"        minAllowed:",
					"          cpu: 10m",
					"          memory: 32Mi",
					"        maxAllowed:",
					"          cpu: 500m",
					"          memory: 512Mi",
				},
			},
			Fix:     "Add a VerticalPodAutoscaler with updateMode InPlaceOrRecreate and minAllowed/maxAllowed CPU and memory bounds for every container.",
			Tunable: true,
		},
	},
	Notes: []string{
		"The instance-prefix, instance-namespace and job-name rules encode hard contracts: they have no per-rule severity and always run at the linter severity.",
		"All three are checked for application packages only, which deploy once per instance. A module deploys once per cluster and is not subject to them.",
	},
}
