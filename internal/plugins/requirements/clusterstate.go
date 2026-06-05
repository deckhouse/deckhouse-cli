/*
Copyright 2025 Flant JSC

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

package requirements

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Masterminds/semver/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	modulecond "github.com/deckhouse/deckhouse-cli/internal/status/tools/constants"
)

const (
	deckhouseNamespace         = "d8-system"
	deckhouseDeploymentName    = "deckhouse"
	deckhouseVersionAnnotation = "core.deckhouse.io/version"

	conditionStatusTrue = "True"
)

// moduleGVR identifies the Deckhouse Module custom resource.
var moduleGVR = schema.GroupVersionResource{Group: "deckhouse.io", Version: "v1alpha1", Resource: "modules"}

// ModuleState is the cluster-side fact about a single Deckhouse module.
type ModuleState struct {
	Enabled bool
	// Version is the installed module version, or nil when the module reports none
	// or a non-parseable value (some modules omit it).
	Version *semver.Version
}

// ClusterState is a one-shot snapshot of the cluster facts needed to enforce a
// plugin's requirements. The caller decides when to (re)build it - typically
// lazily and once per command run, only when a plugin actually declares a
// cluster-side requirement.
type ClusterState struct {
	// Kubernetes is the API server version, or nil if the cluster returned a version
	// string that is not parseable as semver (a declared k8s requirement then fails).
	Kubernetes *semver.Version
	// Deckhouse is the platform version, or nil for a non-release build (e.g. "dev")
	// or an absent annotation: Deckhouse version requirements are then skipped with
	// a warning. A failure to READ the deployment is a hard error, not nil.
	Deckhouse *semver.Version
	// Modules maps module name -> its state; absence from the map means "not in cluster".
	Modules map[string]ModuleState
}

// LoadClusterState builds the snapshot from the cluster. A failure is fatal for
// the caller: if a plugin declares a requirement we cannot verify, it must not
// be installed blindly.
func LoadClusterState(ctx context.Context, kubeCl kubernetes.Interface, dynamicCl dynamic.Interface, logger *dkplog.Logger) (*ClusterState, error) {
	kubeVersion, err := clusterKubernetesVersion(kubeCl, logger)
	if err != nil {
		return nil, err
	}

	deckhouseVersion, err := clusterDeckhouseVersion(ctx, kubeCl, logger)
	if err != nil {
		return nil, err
	}

	modules, err := clusterModules(ctx, dynamicCl)
	if err != nil {
		return nil, err
	}

	return &ClusterState{
		Kubernetes: kubeVersion,
		Deckhouse:  deckhouseVersion,
		Modules:    modules,
	}, nil
}

// clusterKubernetesVersion returns the API server version. A failure to REACH the
// API server is a hard error (the whole snapshot is unusable); a version string
// that cannot be parsed yields nil (so a module/Deckhouse-only plugin is not
// blocked - a declared Kubernetes requirement fails later in its validator).
func clusterKubernetesVersion(kubeCl kubernetes.Interface, logger *dkplog.Logger) (*semver.Version, error) {
	info, err := kubeCl.Discovery().ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("get kubernetes version: %w", err)
	}

	version, err := semver.NewVersion(info.GitVersion)
	if err != nil {
		logger.Warn("could not parse cluster Kubernetes version", slog.String("version", info.GitVersion))

		return nil, nil
	}

	return version, nil
}

// clusterDeckhouseVersion reads the platform version from the deckhouse deployment
// annotation. A read failure (missing deployment, RBAC denied, transient API error)
// is a hard error - we must not silently skip a declared requirement. An absent or
// non-release-semver annotation (e.g. "dev") returns nil to skip with a warning.
func clusterDeckhouseVersion(ctx context.Context, kubeCl kubernetes.Interface, logger *dkplog.Logger) (*semver.Version, error) {
	deployment, err := kubeCl.AppsV1().Deployments(deckhouseNamespace).Get(ctx, deckhouseDeploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read deckhouse deployment to determine version: %w", err)
	}

	raw, found := deployment.Annotations[deckhouseVersionAnnotation]
	if !found || raw == "" {
		logger.Debug("deckhouse version annotation absent; its requirements will be skipped")

		return nil, nil
	}

	version, err := semver.NewVersion(raw)
	if err != nil {
		logger.Debug("deckhouse version is not a release semver; its requirements will be skipped", slog.String("version", raw))

		return nil, nil
	}

	return version, nil
}

func clusterModules(ctx context.Context, dynamicCl dynamic.Interface) (map[string]ModuleState, error) {
	list, err := dynamicCl.Resource(moduleGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list deckhouse modules: %w", err)
	}

	modules := make(map[string]ModuleState, len(list.Items))

	for i := range list.Items {
		item := list.Items[i].Object
		modules[list.Items[i].GetName()] = ModuleState{
			Enabled: moduleEnabled(item),
			Version: moduleVersion(item),
		}
	}

	return modules, nil
}

// moduleEnabled reports whether a module is on. It mirrors the repo convention
// (internal/status/objects/cni_modules): enabled when either the module-config or
// the module-manager condition is True, so a module the operator has enabled is not
// reported off during a reconcile window where only one condition has flipped.
func moduleEnabled(obj map[string]any) bool {
	conditions, found, err := unstructured.NestedSlice(obj, "status", "conditions")
	if !found || err != nil {
		return false
	}

	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok || condition["status"] != conditionStatusTrue {
			continue
		}

		switch condition["type"] {
		case modulecond.ModuleConditionEnabledByModuleManager, modulecond.ModuleConditionEnabledByModuleConfig:
			return true
		}
	}

	return false
}

func moduleVersion(obj map[string]any) *semver.Version {
	raw, found, err := unstructured.NestedString(obj, "properties", "version")
	if !found || err != nil || raw == "" {
		return nil
	}

	version, err := semver.NewVersion(raw)
	if err != nil {
		return nil
	}

	return version
}
