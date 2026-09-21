package debugtar

import (
	"sort"
	"strings"
)

// filterAndExpandCommands selects the commands to run: it resolves the
// {module-name} placeholder against the active modules and drops the entries
// excluded on the command line. It also returns every name --exclude accepts
// for this run, including the names of entries these very excludes dropped, so
// a valid name is never reported as unknown.
//
// Exclusion happens here, and not further down, because this is the only place
// where both spellings of an entry are known at once: the resolved archive name
// (d8-cloud-provider-aws-ccm-logs.txt) and the module-independent token printed
// by --list-exclude (ccm-logs).
//
// modulesKnown reports whether activeModules actually describes the cluster. It
// is false when the module list could not be fetched: module-gated commands are
// then collected anyway (an empty file beats a silently missing one), except
// those whose File carries the {module-name} placeholder — their archive entry
// name cannot be resolved, so they are skipped rather than stored under a
// literal placeholder name.
func filterAndExpandCommands(commands []command, activeModules map[string]bool, modulesKnown bool, excludeSet map[string]bool) (selected []command, acceptedNames []string) {
	selected = make([]command, 0, len(commands))
	acceptedNames = make([]string, 0, len(commands))

	for _, cmd := range commands {
		// The token stays accepted even when the command is gated out below:
		// --exclude ccm-logs must not fail on a cluster without a cloud provider.
		token := excludeBaseName(cmd)
		acceptedNames = append(acceptedNames, token)

		if excludedByName(excludeSet, cmd.File, token) {
			continue
		}

		if cmd.RequiredModule == "" {
			selected = append(selected, cmd)
			continue
		}

		if !modulesKnown {
			if !needsModuleExpansion(cmd) {
				selected = append(selected, cmd)
			}

			continue
		}

		if needsModuleExpansion(cmd) {
			matchedModules := matchingModules(activeModules, cmd.RequiredModule)
			for _, moduleName := range matchedModules {
				expanded := cmd
				expanded.File = strings.ReplaceAll(cmd.File, "{module-name}", moduleName)
				expanded.Args = replaceModuleName(cmd.Args, moduleName)

				acceptedNames = append(acceptedNames, expanded.File)

				if excludedByName(excludeSet, expanded.File) {
					continue
				}

				selected = append(selected, expanded)
			}
		} else {
			for moduleName := range activeModules {
				if isModuleMatch(moduleName, cmd.RequiredModule) {
					selected = append(selected, cmd)
					break
				}
			}
		}
	}

	return selected, acceptedNames
}

func matchingModules(activeModules map[string]bool, required string) []string {
	var matched []string

	for name := range activeModules {
		if isModuleMatch(name, required) {
			matched = append(matched, name)
		}
	}

	sort.Strings(matched)

	return matched
}

func isModuleMatch(moduleName, required string) bool {
	return moduleName == required || strings.HasPrefix(moduleName, required)
}

// moduleScopedFiles lists the File templates that can only be resolved with a
// known module list, for the warning printed when that list is unavailable.
func moduleScopedFiles(commands []command) []string {
	var files []string

	for _, cmd := range commands {
		if cmd.RequiredModule != "" && needsModuleExpansion(cmd) {
			files = append(files, cmd.File)
		}
	}

	return files
}
