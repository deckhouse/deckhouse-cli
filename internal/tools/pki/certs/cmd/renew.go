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
	"net"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/pki/certs"
)

var renewLong = templates.LongDesc(`
Renew control-plane certificates and kubeconfig client certificates.

Renewal is unconditional — certificates are re-signed regardless of their current
expiration date.

All Subject/SAN/Usage fields are read from the existing certificate on disk.
No cluster configuration is required — this command is designed for emergency
recovery when the Kubernetes API is unavailable.

The signing CA must be present on disk and must not be expired. If the CA private
key is absent (external CA), or the CA certificate has expired, the certificate
is skipped; renewing leaf certificates against an expired CA is pointless because
chain validation will fail. The command exits non-zero if any certificate was skipped.

After renewal, restart kube-apiserver, kube-controller-manager, kube-scheduler
and etcd so that the new certificates take effect or reboot the node/kubelet.

© Flant JSC 2026`)

func NewRenewCommand() *cobra.Command {
	var (
		certsDir      string
		kubeconfigDir string
		dryRun        bool
		san           string
	)

	renewCmd := &cobra.Command{
		Use:   "renew (all | PATH)",
		Short: "Renew control-plane certificates and kubeconfig client certificates",
		Long:  renewLong,
		Args:  cobra.ArbitraryArgs,
		Example: "  d8 tools pki certs renew all\n" +
			"  d8 tools pki certs renew all --dry-run\n" +
			"  d8 tools pki certs renew all --san 192.168.0.5\n" +
			"  d8 tools pki certs renew /etc/kubernetes/pki/apiserver.crt\n" +
			"  d8 tools pki certs renew /etc/kubernetes/admin.conf\n" +
			"  d8 tools pki certs renew all --path /opt/k8s/pki --kubeconfig-dir /opt/k8s",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Usage()
			}

			if len(args) > 1 {
				return fmt.Errorf("accepts exactly one argument (PATH), received %d", len(args))
			}

			return certs.RunRenewSingle(cmd.OutOrStdout(), args[0], certsDir, kubeconfigDir, dryRun)
		},
	}

	allCmd := &cobra.Command{
		Use:   "all",
		Short: "Renew all control-plane certificates and kubeconfig client certificates",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var extraIP net.IP
			if san != "" {
				extraIP = net.ParseIP(san)
				if extraIP == nil {
					return fmt.Errorf("--san accepts only an IP address, got %q", san)
				}
			}

			return certs.RunRenewAll(cmd.OutOrStdout(), certsDir, kubeconfigDir, dryRun, extraIP)
		},
	}

	addPathFlags := func(c *cobra.Command) {
		c.Flags().StringVar(&certsDir, "path", defaultCertificatesDir,
			"Directory containing the PKI certificates and CA files")
		c.Flags().StringVar(&kubeconfigDir, "kubeconfig-dir", "",
			"Directory containing kubeconfig files (defaults to the parent of --path)")
		c.Flags().BoolVar(&dryRun, "dry-run", false,
			"Run all without writing any files")
	}
	addPathFlags(renewCmd)
	addPathFlags(allCmd)

	// --san is only for a full renewal
	allCmd.Flags().StringVar(&san, "san", "",
		"New IP SAN to add to serving certificates (e.g. the new master IP)")

	renewCmd.AddCommand(allCmd)

	return renewCmd
}
