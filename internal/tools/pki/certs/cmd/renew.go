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
	"errors"
	"fmt"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/tools/pki/certs"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/kubeconfig"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/pki"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var renewLong = templates.LongDesc(`
Renew control-plane certificates and kubeconfig client certificates.

Renewal is unconditional — certificates are re-signed regardless of their current
expiration date.

All Subject/SAN/Usage fields are read from the existing certificate on disk.
No cluster configuration is required — this command is designed for emergency
recovery when the Kubernetes API is unavailable.

The signing CA must be present on disk and must not be expired. If the CA private
key is absent (external CA), the certificate is skipped. If the CA certificate has
expired, the command stops with an error — renewing leaf certificates against an
expired CA is pointless because chain validation will fail.

After renewal, restart kube-apiserver, kube-controller-manager, kube-scheduler
and etcd so that the new certificates take effect or reboot the node/kubelet.

© Flant JSC 2026`)

type leafEntry struct {
	name     pki.LeafCertName
	longName string
}

type kcEntry struct {
	file     kubeconfig.File
	longName string
}

var knownLeafCerts = []leafEntry{
	{pki.ApiserverCertName, "certificate for serving the Kubernetes API"},
	{pki.ApiserverKubeletClientCertName, "certificate for the API server to connect to kubelet"},
	{pki.ApiserverEtcdClientCertName, "certificate the apiserver uses to access etcd"},
	{pki.FrontProxyClientCertName, "certificate for the front proxy client"},
	{pki.EtcdServerCertName, "certificate for serving etcd"},
	{pki.EtcdPeerCertName, "certificate for etcd nodes to communicate with each other"},
	{pki.EtcdHealthcheckClientCertName, "certificate for liveness probes to healthcheck etcd"},
}

var knownKubeconfigFiles = []kcEntry{
	{kubeconfig.Admin, "certificate embedded in the kubeconfig file for the admin to use"},
	{kubeconfig.SuperAdmin, "certificate embedded in the kubeconfig file for the super-admin"},
	{kubeconfig.ControllerManager, "certificate embedded in the kubeconfig file for the controller manager to use"},
	{kubeconfig.Scheduler, "certificate embedded in the kubeconfig file for the scheduler to use"},
}

// NewRenewCommand returns the "certs renew" group command with all subcommands.
func NewRenewCommand() *cobra.Command {
	var (
		certsDir      string
		kubeconfigDir string
	)

	renewCmd := &cobra.Command{
		Use:   "renew",
		Short: "Renew control-plane certificates",
		Long:  renewLong,
	}

	addRenewFlags := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&certsDir, "path", defaultCertificatesDir,
			"Directory containing the PKI certificates and CA files")
		cmd.Flags().StringVar(&kubeconfigDir, "kubeconfig-dir", "",
			"Directory containing kubeconfig files (defaults to the parent of --path)")
	}

	effectiveKubeconfigDir := func() string {
		if kubeconfigDir != "" {
			return kubeconfigDir
		}
		return filepath.Dir(certsDir)
	}

	// -- "all" subcommand --
	allCmd := &cobra.Command{
		Use:   "all",
		Short: "Renew all control-plane leaf certificates and kubeconfig client certificates",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			w := cmd.OutOrStdout()
			kcDir := effectiveKubeconfigDir()

			for _, entry := range knownLeafCerts {
				msg, fatal := renewLeafCert(certsDir, entry)
				if fatal != nil {
					return fatal
				}
				fmt.Fprintln(w, msg)
			}

			fmt.Fprintln(w)
			report, err := certs.BuildFullScanReport(certsDir, kcDir)
			if err != nil {
				return fmt.Errorf("building post-renewal report: %w", err)
			}
			certs.RenderReport(w, report)

			fmt.Fprintln(w)
			fmt.Fprintln(w, "Done. Restart kube-apiserver, kube-controller-manager, kube-scheduler and etcd.")
			return nil
		},
	}
	addRenewFlags(allCmd)
	renewCmd.AddCommand(allCmd)
	return renewCmd
}

func renewLeafCert(certsDir string, entry leafEntry) (string, error) {
	err := pki.RenewLeafCert(certsDir, entry.name)
	if err == nil {
		return fmt.Sprintf("%s renewed", entry.longName), nil
	}

	var missingCert *pki.CertMissingError
	var expiredCA *pki.CAExpiredError
	var externalCA *pki.CAExternalError

	switch {
	case errors.As(err, &missingCert):
		return fmt.Sprintf("MISSING! %s", entry.longName), nil
	case errors.As(err, &externalCA):
		return fmt.Sprintf("Detected external %s, %s can't be renewed", externalCA.CAName, entry.longName), nil
	case errors.As(err, &expiredCA):
		return "", fmt.Errorf(
			"CA %q expired at %s — rotate the CA before renewing kubeconfig certificates",
			expiredCA.CAName, expiredCA.ExpiredAt.UTC().Format("Jan 02, 2006 15:04 MST"),
		)
	default:
		return "", fmt.Errorf("renew %q: %w", entry.name, err)
	}
}
