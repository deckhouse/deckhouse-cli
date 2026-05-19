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

package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/pki/certs"
)

const defaultCertificatesDir = "/etc/kubernetes/pki"

var checkLong = templates.LongDesc(`
Check expiration of control-plane certificates and kubeconfig client certificates.

Without arguments, all known control-plane certificates under --path and the
kubeconfig files under --kubeconfig-dir are checked.  Output is split into two
sections – leaf certificates and certificate authorities – similar to
"kubeadm certs check-expiration".

--kubeconfig-dir defaults to the parent directory of --path, which matches the
standard Kubernetes layout (/etc/kubernetes/pki → /etc/kubernetes).  Set it
explicitly when using a non-default directory layout.

With a single PATH argument, only that file is inspected.  The command
auto-detects whether the file is a kubeconfig or a PEM certificate.

© Flant JSC 2026`)

// NewCheckCommand returns the "certs check" leaf command.
func NewCheckCommand() *cobra.Command {
	var certsDir string
	var kubeconfigDir string

	checkCmd := &cobra.Command{
		Use:     "check [PATH]",
		Short:   "Check expiration of control-plane certificates",
		Long:    checkLong,
		Args:    cobra.MaximumNArgs(1),
		Example: "  d8 tools pki certs check\n  d8 tools pki certs check /etc/kubernetes/pki/apiserver.crt\n  d8 tools pki certs check /etc/kubernetes/admin.conf\n  d8 tools pki certs check --path /opt/k8s/pki --kubeconfig-dir /opt/k8s",
		RunE: func(cmd *cobra.Command, args []string) error {
			var report *certs.Report
			var err error

			if len(args) == 1 {
				report, err = certs.BuildSingleFileReport(args[0])
				if err != nil {
					return fmt.Errorf("checking certificate %q: %w", args[0], err)
				}
			} else {
				effectiveKubeconfigDir := kubeconfigDir
				if effectiveKubeconfigDir == "" {
					effectiveKubeconfigDir = filepath.Dir(certsDir)
				}
				report, err = certs.BuildFullScanReport(certsDir, effectiveKubeconfigDir)
				if err != nil {
					return fmt.Errorf("checking certificates in %q: %w", certsDir, err)
				}
			}

			certs.RenderReport(cmd.OutOrStdout(), report)
			return nil
		},
	}

	checkCmd.Flags().StringVar(&certsDir, "path", defaultCertificatesDir,
		"Directory containing the PKI certificates (used in full-scan mode only)")
	checkCmd.Flags().StringVar(&kubeconfigDir, "kubeconfig-dir", "",
		"Directory containing kubeconfig files (full-scan mode only; defaults to the parent of --path)")

	return checkCmd
}
