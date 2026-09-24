package debugtar

import (
	"fmt"
	"sort"
	"strings"
)

// trimArchiveExt drops the archive entry extension, so --exclude accepts a name
// with or without it.
func trimArchiveExt(name string) string {
	return strings.TrimSuffix(strings.TrimSuffix(name, ".json"), ".txt")
}

// excludeBaseName returns the --exclude token printed by --list-exclude for a
// command template: the archive entry name as written in debugCommands, or —
// when that name is per-module and therefore cluster-specific — the
// module-independent remainder (d8-{module-name}-ccm-logs.txt -> ccm-logs).
//
// The token is always derived from File, so a new per-module command needs no
// extra per-command data and cannot disagree with its own file name.
func excludeBaseName(cmd command) string {
	if !strings.Contains(cmd.File, "{module-name}") {
		return cmd.File
	}

	name := strings.ReplaceAll(cmd.File, "d8-{module-name}-", "")
	name = strings.ReplaceAll(name, "-{module-name}-", "-")
	name = strings.ReplaceAll(name, "-{module-name}", "")
	name = strings.ReplaceAll(name, "{module-name}-", "")
	name = strings.ReplaceAll(name, "{module-name}", "")

	return trimArchiveExt(name)
}

// newExcludeSet normalizes the raw --exclude values into the form matched
// against command names: surrounding spaces and the extension are irrelevant.
func newExcludeSet(excludeFiles []string) map[string]bool {
	set := make(map[string]bool, len(excludeFiles))

	for _, name := range excludeFiles {
		name = trimArchiveExt(strings.TrimSpace(name))
		if name != "" {
			set[name] = true
		}
	}

	return set
}

// excludedByName reports whether any of the spellings of one archive entry was
// excluded on the command line. A name matches only that entry: there is no
// prefix or group matching.
// P.S. --exclude d8 cannot silently drop every d8-* file.
func excludedByName(excludeSet map[string]bool, names ...string) bool {
	if len(excludeSet) == 0 {
		return false
	}

	for _, name := range names {
		if name != "" && excludeSet[trimArchiveExt(name)] {
			return true
		}
	}

	return false
}

// validateExcludeNames rejects --exclude values that cannot match any archive
// entry, so a typo is reported instead of quietly collecting the full archive.
// acceptedNames comes from filterAndExpandCommands and already covers both the
// resolved entry names of this run and the module-independent tokens.
func validateExcludeNames(excludeFiles, acceptedNames []string) error {
	known := make(map[string]bool, len(acceptedNames))
	accepted := make([]string, 0, len(acceptedNames))

	for _, name := range acceptedNames {
		key := trimArchiveExt(name)
		if key == "" || known[key] {
			continue
		}

		known[key] = true

		accepted = append(accepted, name)
	}

	var unknown []string

	for _, name := range excludeFiles {
		name = trimArchiveExt(strings.TrimSpace(name))
		if name != "" && !known[name] {
			unknown = append(unknown, name)
		}
	}

	if len(unknown) == 0 {
		return nil
	}

	return fmt.Errorf("unknown --exclude name(s): %s%s\nrun \"d8 system collect-debug-info --list-exclude\" to see the accepted names",
		strings.Join(unknown, ", "), suggestExcludeNames(unknown, accepted))
}

// suggestExcludeNames offers the accepted names that contain (or are contained
// in) an unknown one, which covers both typos and the group prefixes that used
// to match implicitly.
func suggestExcludeNames(unknown, accepted []string) string {
	const maxSuggestions = 5

	seen := make(map[string]bool, maxSuggestions)

	var matches []string

	for _, name := range unknown {
		for _, candidate := range accepted {
			key := trimArchiveExt(candidate)
			if seen[key] || !strings.Contains(key, name) && !strings.Contains(name, key) {
				continue
			}

			seen[key] = true

			matches = append(matches, candidate)
		}
	}

	if len(matches) == 0 {
		return ""
	}

	sort.Strings(matches)

	if len(matches) > maxSuggestions {
		return fmt.Sprintf("; did you mean one of: %s, ... (%d more)", strings.Join(matches[:maxSuggestions], ", "), len(matches)-maxSuggestions)
	}

	return fmt.Sprintf("; did you mean: %s", strings.Join(matches, ", "))
}

// GetExcludableFiles returns the tokens accepted by --exclude, one per archive
// entry, as printed by --list-exclude.
func GetExcludableFiles() []string {
	seen := make(map[string]bool, len(debugCommands))

	files := make([]string, 0, len(debugCommands))
	for _, cmd := range debugCommands {
		name := excludeBaseName(cmd)
		if !seen[name] {
			seen[name] = true
			files = append(files, name)
		}
	}

	sort.Strings(files)

	return files
}
