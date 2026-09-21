package debugtar

import (
	"archive/tar"
	"fmt"
	"slices"
	"strings"
)

// command is one entry of a debug archive: the shell command executed inside
// the Deckhouse pod and the archive file its output lands in. The tables of
// such entries live in debugcommands.go and virtualization.go.
type command struct {
	Cmd  string
	Args []string
	File string

	// RequiredModule gates the command on a module being Ready (status.phase ==
	// "Ready"): the command runs only when the name of some Ready module starts
	// with this string, so "cloud-provider" matches cloud-provider-aws. An empty
	// string means always run.
	//
	// Together with the {module-name} placeholder it also means "once per
	// matching module": when RequiredModule is set and File or any Args element
	// contains the placeholder (see needsModuleExpansion), the command is
	// duplicated for every matching Ready module, with the placeholder
	// substituted in both File and Args. Put the placeholder in File whenever it
	// appears in Args — copies that differ only in Args all end up under the same
	// archive entry name, and only the last one survives extraction.
	//
	// Leaving RequiredModule empty while the placeholder is present means it is
	// never resolved and stays literal in the output.
	//
	// When the module list cannot be fetched at all, gating is impossible and
	// the fallback differs by shape: commands without the placeholder run anyway
	// (they either produce data or an empty file), commands with it are skipped,
	// since their archive entry name cannot be resolved.
	RequiredModule string
}

// needsModuleExpansion reports whether cmd must be duplicated once per active
// module matching RequiredModule (with {module-name} substituted into File
// and Args), rather than run once as-is.
func needsModuleExpansion(cmd command) bool {
	if strings.Contains(cmd.File, "{module-name}") {
		return true
	}

	return slices.ContainsFunc(cmd.Args, func(arg string) bool {
		return strings.Contains(arg, "{module-name}")
	})
}

func replaceModuleName(args []string, moduleName string) []string {
	expanded := make([]string, len(args))
	for i, arg := range args {
		expanded[i] = strings.ReplaceAll(arg, "{module-name}", moduleName)
	}

	return expanded
}

func (c *command) writeToTar(tarWriter *tar.Writer, fileContent []byte) error {
	header := &tar.Header{
		Name: c.File,
		Mode: 0o600,
		Size: int64(len(fileContent)),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("write tar header: %v", err)
	}

	if _, err := tarWriter.Write(fileContent); err != nil {
		return fmt.Errorf("copy content: %v", err)
	}

	return nil
}
