/*
Copyright 2026 Flant JSC

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

package apiclient

import "encoding/json"

// The types below mirror the JSON the Deckhouse controller's runtime API answers with
// (deckhouse-controller/internal/packages/api in deckhouse/deckhouse, Module v2).
// Semver constraints are serialized as strings and decode to "" when the controller
// sends null, which it does for "no constraint".

// QueuesDump is the answer of GET /api/v1/queues/dump, optionally narrowed to one
// package with ?name=.
type QueuesDump struct {
	// Queues maps a queue name to its snapshot. A package queue is named after the
	// package; its hook queues are "<package>/<queue>" and "<package>/<queue>/sync",
	// and a module also has "<package>/crd" and "<package>/webhooks".
	Queues map[string]Queue `json:"queues"`
}

// Queue is a snapshot of one task queue.
type Queue struct {
	Length int    `json:"length"`
	Tasks  []Task `json:"tasks,omitempty"`
}

// Task is a task waiting in a queue.
type Task struct {
	// Index is the 1-based position in the queue.
	Index int    `json:"index"`
	Name  string `json:"name"`
	// Enqueued is how long ago the task was queued, as a Go duration.
	Enqueued string `json:"enqueued"`
	// NextRetry is how long until the next attempt, as a Go duration; it is
	// negative once the attempt is due.
	NextRetry string `json:"next_retry"`
	// Error is the last failure of the task, nil while it has not failed.
	Error *string `json:"error,omitempty"`
}

// SchedulerDump is the answer of GET /api/v1/scheduler/dump.
type SchedulerDump struct {
	Nodes map[string]SchedulerNode `json:"nodes"`
}

// SchedulerNode is the scheduling state of one package. It is also the answer of
// GET /api/v1/scheduler/dump?name=, which is null for an unknown package.
type SchedulerNode struct {
	Version string `json:"version"`
	// Order is the scheduling priority of the package: lower runs first.
	Order uint `json:"order"`
	// EffectiveOrder is Order raised to the highest order among the dependencies.
	EffectiveOrder uint      `json:"effectiveOrder"`
	State          NodeState `json:"state"`
	ScheduleReason string    `json:"scheduleReason"`
	// Decision is what the scheduler's rules resolved to.
	Decision     Decision              `json:"decision"`
	Dependencies map[string]Dependency `json:"dependencies,omitempty"`
	// Subscriptions are the packages this one follows (modules follow "global"),
	// Subscribers the packages that follow this one.
	Subscriptions []string `json:"subscriptions,omitempty"`
	Subscribers   []string `json:"subscribers,omitempty"`
}

// NodeState is the lifecycle phase of a scheduler node: idle → scheduled → active.
type NodeState string

const (
	// NodeStateIdle waits for eligibility and may be (re)scheduled.
	NodeStateIdle NodeState = "idle"
	// NodeStateScheduled passed all checks.
	NodeStateScheduled NodeState = "scheduled"
	// NodeStateActive finished processing; dependents may proceed.
	NodeStateActive NodeState = "active"
)

// Decision is a rule verdict with the reason to surface when the package ends up
// disabled.
type Decision struct {
	Kind    DecisionKind `json:"kind"`
	Reason  string       `json:"reason,omitempty"`
	Message string       `json:"message,omitempty"`
}

// DecisionKind is a rule's opinion about running a package.
type DecisionKind string

const (
	// DecisionUndefined has no opinion.
	DecisionUndefined DecisionKind = "Undefined"
	// DecisionEnable is a soft vote to run the package.
	DecisionEnable DecisionKind = "Enable"
	// DecisionDisable is a soft vote to stop the package.
	DecisionDisable DecisionKind = "Disable"
	// DecisionForbid is a hard veto no other rule overrides.
	DecisionForbid DecisionKind = "Forbid"
)

// Dependency is a requirement on another package.
type Dependency struct {
	// Constraint is the semver constraint; empty when any version will do.
	Constraint string `json:"constraint"`
	// Optional skips the check while the dependency is absent.
	Optional bool `json:"optional"`
}

// RequirementsDump is the answer of GET /api/v1/requirements/dump: the values hooks
// store for the release requirement checks, keyed by requirement.
type RequirementsDump map[string]any

// PackagesDump is the answer of GET /api/v1/packages/dump.
type PackagesDump struct {
	// Apps is keyed by "<namespace>.<name>".
	Apps    map[string]Application `json:"apps"`
	Modules map[string]Module      `json:"modules"`
}

// Application is the state of one application package.
type Application struct {
	Status PackageStatus `json:"status"`
	// Name is the instance name.
	Name       string                `json:"name"`
	Namespace  string                `json:"namespace"`
	Path       string                `json:"path"`
	Running    bool                  `json:"running"`
	Definition ApplicationDefinition `json:"definition"`
	Repository Repository            `json:"repository"`
	Digests    map[string]string     `json:"digests"`
	Values     map[string]any        `json:"values,omitempty"`
	Hooks      []string              `json:"hooks,omitempty"`
}

// Module is the state of one module package.
type Module struct {
	Status  PackageStatus `json:"status"`
	Name    string        `json:"name"`
	Running bool          `json:"running"`
	// Embedded marks a module that ships inside the Deckhouse image.
	Embedded   bool              `json:"embedded"`
	Path       string            `json:"path"`
	Definition ModuleDefinition  `json:"definition"`
	Repository Repository        `json:"repository"`
	Digests    map[string]string `json:"digests"`
	Values     map[string]any    `json:"values,omitempty"`
	Hooks      []string          `json:"hooks,omitempty"`
}

// NamedPackage is the answer of GET /api/v1/packages/dump?name=: the application
// or, failing that, the module with that name. The controller answers null when
// there is neither.
type NamedPackage struct {
	Application *Application
	Module      *Module
}

// UnmarshalJSON tells the two shapes apart by the namespace field, which only an
// application carries.
func (p *NamedPackage) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	if _, ok := fields["namespace"]; ok {
		p.Application = new(Application)

		return json.Unmarshal(data, p.Application)
	}

	p.Module = new(Module)

	return json.Unmarshal(data, p.Module)
}

// GlobalModule is the answer of GET /api/v1/packages/global/dump, null until the
// runtime has initialized the global module.
type GlobalModule struct {
	Status  PackageStatus  `json:"status"`
	Name    string         `json:"name"`
	Running bool           `json:"running"`
	Path    string         `json:"path"`
	Values  map[string]any `json:"values,omitempty"`
	Hooks   []string       `json:"hooks,omitempty"`
	// Dynamic is the enabled state of modules as global hooks and module config set it.
	Dynamic map[string]bool `json:"dynamic,omitempty"`
}

// ApplicationDefinition is the metadata an application declares.
type ApplicationDefinition struct {
	Name           string              `json:"name"`
	Version        string              `json:"version"`
	Stage          string              `json:"stage"`
	Requirements   PackageRequirements `json:"requirements"`
	Licensing      Licensing           `json:"licensing"`
	DisableOptions DisableOptions      `json:"disableOptions"`
}

// ModuleDefinition is the metadata a module declares.
type ModuleDefinition struct {
	Name           string              `json:"name"`
	Version        string              `json:"version"`
	Stage          string              `json:"stage"`
	Critical       bool                `json:"critical,omitempty"`
	Weight         uint32              `json:"weight,omitempty"`
	ExclusiveGroup string              `json:"exclusiveGroup,omitempty"`
	Requirements   PackageRequirements `json:"requirements"`
	Licensing      Licensing           `json:"licensing"`
	DisableOptions DisableOptions      `json:"disableOptions"`
}

// PackageRequirements is what a package needs from the cluster and from other modules.
type PackageRequirements struct {
	// Kubernetes and Deckhouse are semver constraints; empty when unset.
	Kubernetes string              `json:"kubernetes"`
	Deckhouse  string              `json:"deckhouse"`
	Modules    ModulesRequirements `json:"modules"`
}

// ModulesRequirements groups module dependencies by how they gate the package. The
// map values are semver constraints, empty when any version is acceptable.
type ModulesRequirements struct {
	// Mandatory modules must be present.
	Mandatory map[string]string `json:"mandatory"`
	// Conditional modules may be absent but must satisfy the constraint when present.
	Conditional map[string]string `json:"conditional"`
	// AnyOf groups each need at least one present member.
	AnyOf []ModuleGroup `json:"anyOf,omitempty"`
	// NoneOf groups forbid their members.
	NoneOf []ModuleGroup `json:"noneOf,omitempty"`
}

// ModuleGroup is a named group of module constraints.
type ModuleGroup struct {
	Name    string            `json:"name"`
	Members map[string]string `json:"members"`
}

// Licensing maps an edition to the license of the package in it.
type Licensing struct {
	Editions map[string]EditionLicense `json:"editions"`
}

// EditionLicense tells whether a package ships in an edition and which bundles enable it.
type EditionLicense struct {
	Available        bool     `json:"available"`
	EnabledInBundles []string `json:"enabledInBundles"`
}

// DisableOptions configures how the package may be disabled.
type DisableOptions struct {
	Confirmation bool            `json:"confirmation"`
	Messages     DisableMessages `json:"messages"`
}

// DisableMessages are the localized confirmation messages.
type DisableMessages struct {
	Ru string `json:"ru,omitempty"`
	En string `json:"en,omitempty"`
}

// Repository is the registry a package comes from. The dump carries the credentials
// as they are.
type Repository struct {
	Name         string `json:"name"`
	Repository   string `json:"repository"`
	DockerConfig string `json:"dockercfg"`
	Login        string `json:"login"`
	Password     string `json:"password"`
	Scheme       string `json:"scheme"`
	CA           string `json:"ca"`
}

// PackageStatus is the runtime status of a package.
type PackageStatus struct {
	Version    string         `json:"version"`
	Conditions []Condition    `json:"conditions"`
	Tracking   Tracking       `json:"tracking"`
	Settings   map[string]any `json:"settings,omitempty"`
	// URLs are the application endpoints found in the rendered manifests.
	URLs []URL `json:"urls,omitempty"`
}

// Condition is one status condition of a package.
type Condition struct {
	Type ConditionType `json:"type"`
	// Status is "True", "False" or "Unknown".
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ConditionType names a package status condition.
type ConditionType string

const (
	ConditionRequirementsMet        ConditionType = "RequirementsMet"
	ConditionReadyOnFilesystem      ConditionType = "ReadyOnFilesystem"
	ConditionLoaded                 ConditionType = "Loaded"
	ConditionHooksProcessed         ConditionType = "HooksProcessed"
	ConditionManifestsApplied       ConditionType = "ManifestsApplied"
	ConditionScaled                 ConditionType = "Scaled"
	ConditionConfigured             ConditionType = "Configured"
	ConditionPending                ConditionType = "Pending"
	ConditionCustomResourcesApplied ConditionType = "CustomResourcesApplied"
	ConditionWebhooksEnsured        ConditionType = "WebhooksEnsured"
	// ConditionMaintenanceMode has inverted polarity: True means the resources are
	// no longer reconciled.
	ConditionMaintenanceMode ConditionType = "MaintenanceMode"
)

// URL is an application endpoint.
type URL struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

// Tracking is the progress of the package's Helm release.
type Tracking struct {
	Completed int            `json:"completed"`
	Remaining int            `json:"remaining"`
	Report    ProgressReport `json:"report"`
}

// ProgressReport lists every operation of the release plans run so far, in order.
type ProgressReport struct {
	Operations []Operation `json:"operations"`
}

// Operation is one step of a release plan (nelm's progrep.Operation). Group, Version
// and Kind come from an embedded GroupVersionKind without JSON tags, hence the
// capitalized keys; meta and release operations leave the object fields empty.
type Operation struct {
	Group     string   `json:"Group"`
	Version   string   `json:"Version"`
	Kind      string   `json:"Kind"`
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Type      string   `json:"type"`
	Iteration int      `json:"iteration"`
	ID        string   `json:"id"`
	Category  string   `json:"category"`
	Status    string   `json:"status"`
	DependsOn []string `json:"dependsOn"`
}

// Snapshots is the answer of GET /api/v1/packages/snapshots/{name}: a hook name to
// its Kubernetes bindings. A hook without Kubernetes bindings maps to null.
type Snapshots map[string]map[string]BindingSnapshot

// BindingSnapshot is what a Kubernetes binding of a hook currently holds.
type BindingSnapshot struct {
	Snapshot   []SnapshotObject   `json:"snapshot"`
	Operations SnapshotOperations `json:"operations"`
}

// SnapshotObject is one object of a binding snapshot. Object is missing when the
// binding keeps only filter results in memory; FilterResult is missing when the
// binding has no jqFilter.
type SnapshotObject struct {
	Object       map[string]any `json:"object,omitempty"`
	FilterResult any            `json:"filterResult,omitempty"`
}

// SnapshotOperations counts the informer cache operations of a binding.
type SnapshotOperations struct {
	SinceStart         CachedObjectsInfo `json:"sinceStart"`
	SinceLastExecution CachedObjectsInfo `json:"sinceLastExecution"`
}

// CachedObjectsInfo is a set of informer cache counters.
type CachedObjectsInfo struct {
	Count    uint64 `json:"count"`
	Added    uint64 `json:"added"`
	Deleted  uint64 `json:"deleted"`
	Modified uint64 `json:"modified"`
	Cleaned  uint64 `json:"cleaned"`
}

// Endpoint is one line of GET /endpoints. The pprof subtree is listed as a single
// "GET /debug/pprof/*" line.
type Endpoint struct {
	Method string
	Path   string
}

// Readiness is the answer of GET /readyz. A leader is ready once its startup
// converge is done; a standby replica is ready while the leader is.
type Readiness struct {
	Ready   bool
	Message string
}
