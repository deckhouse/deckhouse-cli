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
	"k8s.io/kubectl/pkg/util/templates"
)

type CommandInfo struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Aliases     []string          `json:"aliases"`
	Flags       map[string]string `json:"flags"`
	Subcommands []CommandInfo     `json:"subcommands"`
}

var (
	helpLong = templates.LongDesc(`
		providing the respective flag.`)
)

func init() {
	helpJsonCmd := &cobra.Command{
		Use:   "help-json",
		Short: "Get all commands in json",
		Long:  helpLong,
		//Hidden: true,
		RunE: helpJson,
	}

	//flags.AddPersistentFlags(helpCmd)
	//helpJsonCmd.AddCommand(platform.NewCommand())

	rootCmd.AddCommand(helpJsonCmd)

}

func helpJson(cmd *cobra.Command, _ []string) error {

	commandsData := extractCommands(rootCmd)

	jsonData, err := json.MarshalIndent(commandsData, "", "  ")
	if err != nil {
		return err
	}

	// Print JSON output
	fmt.Println(string(jsonData))

	return nil
}

func extractCommands(cmd *cobra.Command) CommandInfo {
	flags := make(map[string]string)
	collectFlags(cmd.Flags(), flags)
	collectFlags(cmd.PersistentFlags(), flags)

	// Recursive extraction of subcommands
	var subcommands []CommandInfo
	for _, subCmd := range cmd.Commands() {
		subcommands = append(subcommands, extractCommands(subCmd)) // 🔥 Recursion happens here
	}

	return CommandInfo{
		Name:        cmd.Use,
		Description: cmd.Short,
		Flags:       flags,
		Aliases:     cmd.Aliases,
		Subcommands: subcommands,
	}
}

// Helper function to collect flags
func collectFlags(flagSet *pflag.FlagSet, flags map[string]string) {
	if flagSet != nil {
		flagSet.VisitAll(func(f *pflag.Flag) {
			flags[f.Name] = f.Usage
		})
	}
}

//// Extract subcommands and flags recursively
//func extractCommands(cmd *cobra.Command, _ []string) error {
//
//	// Extract flags
//	flags := make(map[string]string)
//	var flagSet *pflag.FlagSet
//	if flagSet != nil {
//		flagSet.VisitAll(func(f *pflag.Flag) {
//			flags[f.Name] = f.Usage
//		})
//	}
//
//	// Extract subcommands
//	var subcommands []CommandInfo
//	for _, subCmd := range cmd.Commands() {
//		subcommands = append(subcommands, CommandInfo{
//			Name:        subCmd.Use,
//			Description: subCmd.Short,
//			Flags:       flags,
//			Aliases:     subCmd.Aliases,
//			Subcommands: nil,
//		})
//	}
//
//	helpInfo := CommandInfo{
//		Name:        cmd.Use,
//		Description: cmd.Short,
//		Flags:       flags,
//		Aliases:     cmd.Aliases,
//		Subcommands: subcommands,
//	}
//
//	// Convert to JSON
//	jsonData, err := json.MarshalIndent(helpInfo, "", "  ")
//	if err != nil {
//		fmt.Println("Error generating JSON:", err)
//		os.Exit(1)
//	}
//
//	// Print JSON
//	fmt.Println(string(jsonData))
//
//	//return CommandInfo{
//	//	Name:        cmd.Use,
//	//	Description: cmd.Short,
//	//	Flags:       flags,
//	//	Aliases:     cmd.Aliases,
//	//	Subcommands: subcommands,
//	//}
//	return err
//}
//
//// Helper function to collect flags
////func collectFlags(flagSet *pflag.FlagSet, flags map[string]string) {
////	if flagSet != nil {
////		flagSet.VisitAll(func(f *pflag.Flag) {
////			flags[f.Name] = f.Usage
////		})
////	}
////}
//
////func extractCommands(cmd *cobra.Command) CommandInfo {
////	flags := make(map[string]string)
////	cmd.Flags().VisitAll(func(f *pflag.Flag) {
////		flags[f.Name] = f.Usage
////	})
////
////	var subcommands []CommandInfo
////	for _, subCmd := range cmd.Commands() {
////		subcommands = append(subcommands, extractCommands(subCmd))
////	}
////
////	return CommandInfo{
////		Name:        cmd.Use,
////		Description: cmd.Short,
////		Aliases:     cmd.Aliases,
////		Flags:       flags,
////		Subcommands: subcommands,
////	}
////}
////
////func extractFlags(cmd *cobra.Command) map[string][]FlagInfo {
////	flagsMap := make(map[string][]FlagInfo)
////
////	// Helper function to extract flags from a flag set
////	getFlags := func(flagSet *pflag.FlagSet) []FlagInfo {
////		var flags []FlagInfo
////		if flagSet != nil {
////			flagSet.VisitAll(func(f *pflag.Flag) {
////				flags = append(flags, FlagInfo{
////					Name:        f.Name,
////					Description: f.Usage,
////				})
////			})
////		}
////		return flags
////	}
////
////	// Get flags for the current command
////	flagsMap[cmd.Use] = append(getFlags(cmd.Flags()), getFlags(cmd.PersistentFlags())...)
////
////	// Recursively collect flags from subcommands
////	for _, subCmd := range cmd.Commands() {
////		for key, value := range extractFlags(subCmd) {
////			flagsMap[key] = value
////		}
////	}
////
////	return flagsMap
////}
//
////func AddPersistentFlags(cmd *cobra.Command) {
////	defaultKubeconfigPath := os.ExpandEnv("$HOME/.kube/config")
////	if p := os.Getenv("KUBECONFIG"); p != "" {
////		defaultKubeconfigPath = p
////	}
////	cmd.PersistentFlags().StringP(
////		"kubeconfig",
////		"k",
////		defaultKubeconfigPath,
////		"KubeConfig of the cluster. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)",
////	)
////}
//
////var platformLong = templates.LongDesc(`
////Operate platform options in DKP.
////
////© Flant JSC 2025`)
////
////func NewCommand() *cobra.Command {
////	platformCmd := &cobra.Command{
////		Use:     "platform <command>",
////		Short:   "Operate platform options.",
////		Aliases: []string{"p"},
////		Long:    platformLong,
////		PreRunE: flags.ValidateParameters,
////	}
////
////	platformCmd.AddCommand(
////		edit.NewCommand(),
////		module.NewCommand(),
////		collect_debug_info.NewCommand(),
////		queue.NewCommand(),
////	)
////
////	flags.AddPersistentFlags(platformCmd)
////
////	return platformCmd
////}
