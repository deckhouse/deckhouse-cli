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

package flags

import (
	"os"

	"github.com/spf13/pflag"

	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
)

const (
	defaultDeckhousePluginsDir = "/opt/deckhouse/lib/deckhouse-cli"

	// Env* constants are the single source of truth for the environment variable
	// names: functional reads and help texts must reference them, never literals.
	EnvSkipClusterChecks = "D8_PLUGINS_SKIP_CLUSTER_CHECKS"

	// EnvPluginsDir overrides the plugins directory (same as --plugins-dir);
	// applied at command registration in cmd/d8/root.go.
	EnvPluginsDir = "DECKHOUSE_CLI_PATH"
)

// CLI Parameters
var (
	DeckhousePluginsDir = defaultDeckhousePluginsDir

	// Kubeconfig and KubeContext locate the cluster; used to reach the
	// registry-packages-proxy and to enforce cluster-side plugin requirements
	// (Kubernetes/Deckhouse/module versions).
	Kubeconfig  = defaultKubeconfigPath()
	KubeContext string

	// SkipClusterChecks downgrades cluster-side requirement enforcement to a warning,
	// so a plugin that declares such a requirement can be installed without reaching
	// the cluster.
	SkipClusterChecks = skipClusterChecksDefault()
)

func skipClusterChecksDefault() bool {
	switch os.Getenv(EnvSkipClusterChecks) {
	case "1", "true", "TRUE", "True":
		return true
	default:
		return false
	}
}

func defaultKubeconfigPath() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}

	return os.ExpandEnv("$HOME/.kube/config")
}

func AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&DeckhousePluginsDir,
		"plugins-dir",
		DeckhousePluginsDir,
		"Path to the d8 plugins directory. Defaults to $"+EnvPluginsDir+".",
	)
	flagSet.StringVarP(
		&Kubeconfig,
		"kubeconfig",
		"k",
		Kubeconfig,
		"Path to the kubeconfig file. Used to reach registry-packages-proxy and to enforce cluster-side plugin requirements. Defaults to $KUBECONFIG.",
	)
	flagSet.StringVar(
		&KubeContext,
		"context",
		KubeContext,
		"Kubeconfig context to use. Used to reach registry-packages-proxy and to enforce cluster-side plugin requirements.",
	)
	flagSet.BoolVar(
		&SkipClusterChecks,
		"skip-cluster-checks",
		SkipClusterChecks,
		"Skip enforcement of cluster-side plugin requirements (Kubernetes/Deckhouse/module versions) when the cluster is unreachable. Defaults to $"+EnvSkipClusterChecks+".",
	)

	rppflags.AddFlags(flagSet)
}
