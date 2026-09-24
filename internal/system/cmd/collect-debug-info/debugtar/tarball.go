package debugtar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func Tarball(config *rest.Config, kubeCl kubernetes.Interface, excludeFiles []string, commandTimeout time.Duration, requestInterval time.Duration) error {
	const (
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return fmt.Errorf("failed to get Deckhouse pod: %w", err)
	}

	activeModules, modulesErr := fetchActiveModules(config, kubeCl, podName, namespace, containerName, commandTimeout)
	if modulesErr != nil {
		fmt.Fprintf(os.Stderr, "ERROR: could not fetch the list of active modules: %v\n", modulesErr)
		fmt.Fprintf(os.Stderr, "  collection continues without module filtering: module-gated commands run anyway and may produce empty files; per-module commands are skipped because their file names cannot be resolved (%s)\n",
			strings.Join(moduleScopedFiles(debugCommands), ", "))
	}

	commands, acceptedNames := filterAndExpandCommands(debugCommands, activeModules, modulesErr == nil, newExcludeSet(excludeFiles))

	if err := validateExcludeNames(excludeFiles, acceptedNames); err != nil {
		return err
	}

	return writeArchive(
		config, kubeCl, podName, namespace, containerName,
		commands, commandTimeout, requestInterval,
		"Collecting debug info from Deckhouse...",
		"Debug archive collection completed.",
	)
}

type moduleList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

// fetchActiveModules returns a map with the names of modules that are in the Ready phase.
func fetchActiveModules(
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	timeout time.Duration,
) (map[string]bool, error) {
	cmdLine := []string{"kubectl", "get", "module", "-o", "json"}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stdout, stderr, err := utilk8s.ExecCommandInPod(ctx, config, kubeCl, cmdLine, podName, namespace, containerName)
	if err != nil {
		return nil, fmt.Errorf("stream kubectl get module: %w (stderr: %s)", err, stderr)
	}

	if len(stdout) == 0 {
		return nil, fmt.Errorf("kubectl get module returned no output (stderr: %s)", stderr)
	}

	var list moduleList
	if err = json.Unmarshal(stdout, &list); err != nil {
		return nil, fmt.Errorf("parse module list: %w", err)
	}

	active := make(map[string]bool, len(list.Items))
	for _, item := range list.Items {
		if item.Status.Phase == "Ready" {
			active[item.Metadata.Name] = true
		}
	}

	return active, nil
}
