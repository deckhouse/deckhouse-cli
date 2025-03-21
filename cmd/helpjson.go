/*
Copyright 2024 Flant JSC

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

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type CommandInfo struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Version     string            `json:"version,omitempty"`
	Aliases     []string          `json:"aliases"`
	Flags       map[string]string `json:"flags"`
	Subcommands []CommandInfo     `json:"subcommands"`
}

func init() {
	helpJsonCmd := &cobra.Command{
		Use:    "help-json",
		Short:  "Get all d8 command options and flags in json.",
		Hidden: true,
		RunE:   helpJson,
	}
	rootCmd.AddCommand(helpJsonCmd)
}

func helpJson(cmd *cobra.Command, _ []string) error {
	commandsData := extractCommands(rootCmd)
	jsonData, err := json.MarshalIndent(commandsData, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(jsonData))
	return nil
}

func extractCommands(cmd *cobra.Command) CommandInfo {
	flags := make(map[string]string)
	collectFlags(cmd.Flags(), flags)
	collectFlags(cmd.PersistentFlags(), flags)
	helpFlag := cmd.Flags().Lookup("help")
	if helpFlag == nil {
		rootCmd.Flags().BoolP("help", "h", false, "Show help for the command")
	}

	var subcommands []CommandInfo
	for _, subCmd := range cmd.Commands() {
		subcommands = append(subcommands, extractCommands(subCmd))
	}

	return CommandInfo{
		Name:        cmd.Use,
		Description: cmd.Short,
		Version:     cmd.Version,
		Flags:       flags,
		Aliases:     cmd.Aliases,
		Subcommands: subcommands,
	}
}

func collectFlags(flagSet *pflag.FlagSet, flags map[string]string) {
	if flagSet != nil {
		flagSet.VisitAll(func(f *pflag.Flag) {
			flags[f.Name] = f.Usage
		})
	}
}
