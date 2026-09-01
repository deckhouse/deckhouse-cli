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

package dataapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	authv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// discoveryTimeout bounds one discovery request.
//
// It exists because the discovery client cannot be handed a context: client-go's
// ServerResourcesForGroupVersion takes none and issues its request with context.TODO(). So the
// calling command's deadline does not reach this request, and neither does Ctrl-C — and every
// `d8 data` subcommand now starts with it, before doing any work of its own.
//
// Leaving the configuration's timeout at zero does NOT mean the request is unbounded: client-go's
// setDiscoveryDefaults substitutes 32s. It means the bound is someone else's, fixed, and unrelated
// to how long the caller was willing to wait — `d8 data export delete` gives itself 25s and would
// still sit here for 32.
const discoveryTimeout = 30 * time.Second

// discriminatorVerb is the verb resolution asks about. Every `d8 data` subcommand reads the CR it
// operates on, so "get" is the one permission all of them need; resolving on the per-subcommand
// verb instead would let two subcommands pick different groups within the same cluster and give
// the user an inconsistent view.
const discriminatorVerb = "get"

// ErrNoBackend reports that no candidate group serves the resource at all: neither producing
// module is installed, or neither has finished installing its CRDs.
var ErrNoBackend = errors.New("no module in this cluster serves this resource")

// ErrForbidden reports that the cluster does serve the resource, but the calling user is not
// authorized for any group that serves it. This is deliberately distinct from ErrNoBackend: the
// fix is an RBAC grant, not enabling a module.
var ErrForbidden = errors.New("not authorized to use this resource")

// ResourceLister reports which resources one group/version serves.
// *discovery.DiscoveryClient satisfies it.
type ResourceLister interface {
	ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error)
}

// AccessReviewer answers "may the current user do this", via SelfSubjectAccessReview.
// client-go's typed SelfSubjectAccessReviewInterface satisfies it.
//
// SelfSubjectAccessReview is used rather than a trial request because it is the only check every
// authenticated user may perform (ClusterRole system:basic-user is bound to system:authenticated)
// and because it does not create, read or mutate anything in the target namespace.
type AccessReviewer interface {
	Create(ctx context.Context, ssar *authv1.SelfSubjectAccessReview, opts metav1.CreateOptions) (*authv1.SelfSubjectAccessReview, error)
}

// access is the outcome of asking whether the user may use one candidate group. It is
// three-valued on purpose: a cluster whose role bindings deny SelfSubjectAccessReview itself
// leaves the question open, and an open question must not read as a denial.
type access int

const (
	accessUnknown access = iota
	accessAllowed
	accessDenied
)

// candidates lists the groups to consider, most preferred first. storage-foundation wins a tie
// because it is the module that supersedes the other: a cluster serving both is one that has
// storage-foundation enabled.
func candidates() []Backend {
	return []Backend{
		{GroupVersion: FoundationGroupVersion, Module: foundationModule},
		{GroupVersion: LegacyGroupVersion, Module: legacyModule},
	}
}

// Resolve picks the group to address resource through, for a user acting in namespace.
//
// It answers two independent questions per candidate and combines them, because each alone is
// ambiguous:
//
//   - discovery: does the API server serve this group at all? RBAC does not affect the answer —
//     a role naming a group that no CRD backs still parses, so permission alone cannot tell an
//     installed module from an uninstalled one.
//   - SelfSubjectAccessReview: may this user read the resource in this namespace? Discovery does
//     not affect the answer, so a served group alone cannot tell an authorized user from one
//     whose grants were left behind by a previous edition.
//
// The first candidate that is both served and not denied wins. When nothing qualifies, the
// returned error distinguishes "no module serves it" (ErrNoBackend) from "it is served but you
// may not use it" (ErrForbidden), naming what was found either way.
func Resolve(ctx context.Context, cfg *rest.Config, resource, namespace string, log *slog.Logger) (Backend, error) {
	if cfg == nil {
		return Backend{}, fmt.Errorf("resolve %s API group: no REST config", resource)
	}

	if err := ctx.Err(); err != nil {
		// Wrapped like every other failure of this function: unwrapped, the user sees a bare
		// "context canceled" with no hint that it was group resolution that ran out of time.
		// errors.Is still reaches context.Canceled through the %w.
		return Backend{}, fmt.Errorf("resolve %s API group: %w", resource, err)
	}

	// The discovery client gets its own bounded copy of the configuration; see discoveryTimeout
	// for why the caller's context cannot bound it instead.
	discoveryConfig := rest.CopyConfig(cfg)
	discoveryConfig.Timeout = discoveryBudget(ctx, discoveryConfig.Timeout)

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(discoveryConfig)
	if err != nil {
		return Backend{}, fmt.Errorf("resolve %s API group: build discovery client: %w", resource, err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return Backend{}, fmt.Errorf("resolve %s API group: build Kubernetes client: %w", resource, err)
	}

	return resolve(ctx, discoveryClient, clientset.AuthorizationV1().SelfSubjectAccessReviews(), resource, namespace, log)
}

// resolve is Resolve with its two clients injected, so the decision table can be tested without
// a cluster.
func resolve(
	ctx context.Context,
	lister ResourceLister,
	reviewer AccessReviewer,
	resource, namespace string,
	log *slog.Logger,
) (Backend, error) {
	if log == nil {
		log = slog.Default()
	}

	// Candidates are consulted one at a time, in preference order, and the first that qualifies
	// ends the search. Probing all of them up front would make the command depend on the health
	// of a group it was never going to use: on a cluster that runs storage-foundation, a failing
	// discovery endpoint for the other group would break `d8 data` outright, even though nothing
	// in the run would have addressed it. It also spends two round trips where one settles the
	// question.
	var denied []Backend

	for _, backend := range candidates() {
		served, err := serves(lister, backend.GroupVersion, resource)
		if err != nil {
			return Backend{}, fmt.Errorf("resolve %s API group: discovering %s: %w", resource, backend, err)
		}

		if !served {
			continue
		}

		// accessUnknown counts as usable: the review machinery was unavailable, and refusing to
		// act on a question we could not ask would break clusters whose only fault is a
		// non-standard binding of system:basic-user. A real denial still surfaces, as a 403 on
		// the request that follows.
		if mayGet(ctx, reviewer, backend.GroupVersion, resource, namespace, log) == accessDenied {
			denied = append(denied, backend)
			continue
		}

		log.Debug("Resolved data API group",
			slog.String("resource", resource),
			slog.String("group_version", backend.String()),
			slog.String("module", backend.Module))

		return backend, nil
	}

	return Backend{}, resolutionError(denied, resource, namespace)
}

// discoveryBudget picks the timeout to put on the discovery configuration: the caller's remaining
// time when it is shorter than discoveryTimeout, and discoveryTimeout otherwise. A timeout already
// configured by the caller is never lengthened.
//
// Never returns zero. Zero is not "no limit" here — client-go would substitute its own 32s — it is
// "a limit nobody in this call chain chose", which is the whole thing this function replaces.
func discoveryBudget(ctx context.Context, configured time.Duration) time.Duration {
	budget := discoveryTimeout

	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < budget {
			budget = remaining
		}
	}

	if configured > 0 && configured < budget {
		budget = configured
	}

	if budget < time.Millisecond {
		budget = time.Millisecond
	}

	return budget
}

// resolutionError renders the failure that fits what the probes actually found, so the message
// names the one thing that has to change.
func resolutionError(denied []Backend, resource, namespace string) error {
	if len(denied) > 0 {
		served := make([]string, 0, len(denied))
		for _, b := range denied {
			served = append(served, b.String())
		}

		return fmt.Errorf(
			"%w: this cluster serves %s as %s, but your account may not %s it in namespace %q; "+
				"check with: d8 k auth can-i %s %s.%s -n %s",
			ErrForbidden,
			resource, strings.Join(served, " and "),
			discriminatorVerb, namespace,
			discriminatorVerb, resource, denied[0].GroupVersion.Group, namespace,
		)
	}

	return fmt.Errorf(
		"%w: %s is served by the %s module (%s) and by the %s module (%s), and this cluster serves neither; "+
			"enable one of them, or check that its CRDs finished installing",
		ErrNoBackend,
		resource,
		foundationModule, FoundationGroupVersion,
		legacyModule, LegacyGroupVersion,
	)
}

// serves reports whether the API server serves resource under gv. A group the server does not
// know is a 404, which is an answer ("not served") rather than a failure; anything else is a
// failure, because guessing past a broken discovery endpoint would silently pick the wrong group.
func serves(lister ResourceLister, gv schema.GroupVersion, resource string) (bool, error) {
	list, err := lister.ServerResourcesForGroupVersion(gv.String())
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	if list == nil {
		return false, nil
	}

	for _, r := range list.APIResources {
		// Exact match only: the same listing carries subresources such as "dataexports/status",
		// whose presence says nothing about the resource itself being servable.
		if r.Name == resource {
			return true, nil
		}
	}

	return false, nil
}

// mayGet asks the API server whether the current user may read resource in namespace. A failed
// review is reported as accessUnknown rather than a denial — see the caller for why.
func mayGet(
	ctx context.Context,
	reviewer AccessReviewer,
	gv schema.GroupVersion,
	resource, namespace string,
	log *slog.Logger,
) access {
	if reviewer == nil {
		return accessUnknown
	}

	review := &authv1.SelfSubjectAccessReview{
		Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authv1.ResourceAttributes{
				Namespace: namespace,
				Verb:      discriminatorVerb,
				Group:     gv.Group,
				Version:   gv.Version,
				Resource:  resource,
			},
		},
	}

	result, err := reviewer.Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		log.Debug("Access review unavailable, falling back to discovery order",
			slog.String("group_version", gv.String()),
			slog.String("resource", resource),
			slog.String("error", err.Error()))

		return accessUnknown
	}

	if result == nil {
		return accessUnknown
	}

	if result.Status.Allowed {
		return accessAllowed
	}

	// An evaluation error means the authorizer could not decide, which is not the same as a
	// decision to deny; treating it as denial would skip a group the user can in fact use.
	if result.Status.EvaluationError != "" {
		log.Debug("Access review could not evaluate, falling back to discovery order",
			slog.String("group_version", gv.String()),
			slog.String("resource", resource),
			slog.String("evaluation_error", result.Status.EvaluationError))

		return accessUnknown
	}

	return accessDenied
}
