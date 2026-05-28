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
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse/go_lib/controlplane/constants"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/kubeconfig"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/pki"
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

func NewRenewCommand() *cobra.Command {
	var (
		certsDir      string
		kubeconfigDir string
		dryRun        bool
	)

	renewCmd := &cobra.Command{
		Use:   "renew (all | PATH)",
		Short: "Renew control-plane certificates and kubeconfig client certificates",
		Long:  renewLong,
		Args: cobra.ArbitraryArgs,
		Example: "  d8 tools pki certs renew all\n" +
			"  d8 tools pki certs renew all --dry-run\n" +
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
			w := cmd.OutOrStdout()
			kcDir := effectiveKubeconfigDir(certsDir, kubeconfigDir)
			return runRenewSingle(w, args[0], certsDir, kcDir, dryRun)
		},
	}

	allCmd := &cobra.Command{
		Use:   "all",
		Short: "Renew all control-plane certificates and kubeconfig client certificates",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			w := cmd.OutOrStdout()
			kcDir := effectiveKubeconfigDir(certsDir, kubeconfigDir)
			return runRenewAll(w, certsDir, kcDir, dryRun)
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

	renewCmd.AddCommand(allCmd)

	return renewCmd
}

func effectiveKubeconfigDir(certsDir, override string) string {
	if override != "" {
		return override
	}
	return filepath.Dir(certsDir)
}

func runRenewAll(w io.Writer, certsDir, kcDir string, dryRun bool) error {
	printDryRunBanner(w, dryRun)

	var warnings []string
	usedCAs := map[pki.RootCertName]struct{}{}

	leafOpts := []pki.RenewOption{pki.WithRenewDir(certsDir)}
	if dryRun {
		leafOpts = append(leafOpts, pki.WithDryRun())
	}

	leafReport := pki.RenewCertificates(leafOpts...)
	for _, e := range leafReport.Entries {
		line := formatLeafEntry(pki.LeafDescription(e.Name), e)
		fmt.Fprintln(w, line)
		if e.Err != nil {
			warnings = append(warnings, line)
			continue
		}
		usedCAs[e.Authority] = struct{}{}
	}

	kcOpts := []kubeconfig.RenewOption{
		kubeconfig.WithRenewKubeconfigDir(kcDir),
		kubeconfig.WithRenewPKIDir(certsDir),
	}
	if dryRun {
		kcOpts = append(kcOpts, kubeconfig.WithDryRun())
	}

	kcReport := kubeconfig.RenewClientCerts(kcOpts...)
	for _, e := range kcReport.Entries {
		line := formatKubeconfigEntry(kubeconfig.FileDescription(e.File), e)
		fmt.Fprintln(w, line)
		if e.Err != nil {
			warnings = append(warnings, line)
			continue
		}
		usedCAs[pki.CACertName] = struct{}{} // kubeconfig client certs are signed by ca
	}

	warnings = append(warnings, checkCAsOutliveRenewed(w, certsDir, usedCAs)...)

	printDryRunFooter(w, dryRun)

	return warningsError(warnings)
}

func printDryRunBanner(w io.Writer, dryRun bool) {
	if dryRun {
		fmt.Fprintln(w, "DRY RUN — no files will be modified")
	}
}

func printDryRunFooter(w io.Writer, dryRun bool) {
	if dryRun {
		fmt.Fprintln(w, "(dry-run) nothing was written")
	}
}

// checkCAsOutliveRenewed warns when a CA that signed a freshly renewed cert expires sooner than the new (1-year) cert.
func checkCAsOutliveRenewed(w io.Writer, certsDir string, usedCAs map[pki.RootCertName]struct{}) []string {
	if len(usedCAs) == 0 {
		return nil
	}

	cas := make([]pki.RootCertName, 0, len(usedCAs))
	for ca := range usedCAs {
		cas = append(cas, ca)
	}

	report := pki.ListCertificateExpirations(
		pki.WithCertificatesDir(certsDir),
		pki.WithRootCertificates(cas...),
	)

	threshold := time.Now().Add(constants.CertificateValidityPeriod)
	var warnings []string
	for _, e := range report.Entries {
		if e.Err != nil {
			// CA read failure here is already surfaced via leaf renew skips.
			continue
		}
		if e.NotAfter.Before(threshold) {
			line := fmt.Sprintf("WARNING: CA %q expires %s, sooner than the renewed certificates — rotate the CA: %s",
				e.Name, e.NotAfter.UTC().Format("Jan 02, 2006 15:04 MST"), e.Path)
			fmt.Fprintln(w, line)
			warnings = append(warnings, line)
		}
	}
	return warnings
}

func warningsError(warnings []string) error {
	if len(warnings) == 0 {
		return nil
	}
	return fmt.Errorf("%d warning(s) during renewal:\n%s", len(warnings), strings.Join(warnings, "\n"))
}

func runRenewSingle(w io.Writer, path, certsDir, kcDirOverride string, dryRun bool) error {
	if kcExp, err := kubeconfig.GetClientCertificateExpiration(path); err == nil {
		return renewSingleKubeconfig(w, path, kcExp.File, certsDir, kcDirOverride, dryRun)
	}

	certExp, certErr := pki.GetCertificateExpiration(path)
	if certErr != nil {
		return fmt.Errorf("cannot determine file type for %q: not a recognizable kubeconfig or PEM certificate: %w", path, certErr)
	}
	if certExp.IsCA {
		return fmt.Errorf("CA certificate renewal is not supported: %q", path)
	}
	return renewSingleLeaf(w, path, pki.LeafCertName(certExp.Name), certsDir, dryRun)
}

func renewSingleLeaf(w io.Writer, path string, name pki.LeafCertName, pkiDir string, dryRun bool) error {
	printDryRunBanner(w, dryRun)

	if !strings.HasPrefix(filepath.Clean(path), filepath.Clean(pkiDir)+string(filepath.Separator)) {
		return fmt.Errorf("certificate %q is not under PKI directory %q; use --path to specify the correct directory", path, pkiDir)
	}

	opts := []pki.RenewOption{pki.WithRenewDir(pkiDir), pki.WithRenewLeafs(name)}
	if dryRun {
		opts = append(opts, pki.WithDryRun())
	}

	var warnings []string
	usedCAs := map[pki.RootCertName]struct{}{}

	report := pki.RenewCertificates(opts...)
	for _, e := range report.Entries {
		line := formatLeafEntry(pki.LeafDescription(e.Name), e)
		fmt.Fprintln(w, line)
		if e.Err != nil {
			warnings = append(warnings, line)
			continue
		}
		usedCAs[e.Authority] = struct{}{}
	}

	warnings = append(warnings, checkCAsOutliveRenewed(w, pkiDir, usedCAs)...)

	printDryRunFooter(w, dryRun)

	return warningsError(warnings)
}

func renewSingleKubeconfig(w io.Writer, path string, file kubeconfig.File, certsDirOverride, kcDirOverride string, dryRun bool) error {
	printDryRunBanner(w, dryRun)

	kcDir := kcDirOverride
	if kcDir == "" {
		kcDir = filepath.Dir(filepath.Clean(path))
	}

	opts := []kubeconfig.RenewOption{
		kubeconfig.WithRenewKubeconfigDir(kcDir),
		kubeconfig.WithRenewPKIDir(certsDirOverride),
	}
	if dryRun {
		opts = append(opts, kubeconfig.WithDryRun())
	}

	var warnings []string
	usedCAs := map[pki.RootCertName]struct{}{}

	entry := kubeconfig.KubeconfigRenewEntry{
		File: file,
		Path: path,
		Err:  kubeconfig.RenewClientCert(file, opts...),
	}
	line := formatKubeconfigEntry(kubeconfig.FileDescription(file), entry)
	fmt.Fprintln(w, line)
	if entry.Err != nil {
		warnings = append(warnings, line)
	} else {
		usedCAs[pki.CACertName] = struct{}{} // kubeconfig client certs are signed by ca
	}

	warnings = append(warnings, checkCAsOutliveRenewed(w, certsDirOverride, usedCAs)...)

	printDryRunFooter(w, dryRun)

	return warningsError(warnings)
}

func formatLeafEntry(desc string, e pki.PKIRenewEntry) string {
	var (
		missing   *pki.MissingError
		caMissing *pki.CAMissingError
		external  *pki.CAExternalError
		expired   *pki.CAExpiredError
	)
	switch {
	case e.Err == nil:
		return fmt.Sprintf("%s renewed", desc)
	case errors.As(e.Err, &missing):
		return fmt.Sprintf("MISSING! %s: %s", desc, e.Path)
	case errors.As(e.Err, &caMissing):
		return fmt.Sprintf("CA %q missing, %s skipped: %s", caMissing.CAName, desc, e.Path)
	case errors.As(e.Err, &external):
		return fmt.Sprintf("Detected external %s, %s can't be renewed: %s", external.CAName, desc, e.Path)
	case errors.As(e.Err, &expired):
		return fmt.Sprintf("CA %q expired, %s skipped: %s", expired.CAName, desc, e.Path)
	default:
		return fmt.Sprintf("%s — error: %s", desc, e.Err.Error())
	}
}

func formatKubeconfigEntry(desc string, e kubeconfig.KubeconfigRenewEntry) string {
	var (
		missing   *kubeconfig.MissingError
		caMissing *kubeconfig.CAMissingError
		external  *kubeconfig.CAExternalError
		expired   *kubeconfig.CAExpiredError
	)
	switch {
	case e.Err == nil:
		return fmt.Sprintf("%s renewed", desc)
	case errors.As(e.Err, &missing):
		return fmt.Sprintf("MISSING! %s: %s", desc, e.Path)
	case errors.As(e.Err, &caMissing):
		return fmt.Sprintf("CA %q missing, %s skipped: %s", caMissing.CAName, desc, e.Path)
	case errors.As(e.Err, &external):
		return fmt.Sprintf("Detected external %s, %s can't be renewed: %s", external.CAName, desc, e.Path)
	case errors.As(e.Err, &expired):
		return fmt.Sprintf("CA %q expired, %s skipped: %s", expired.CAName, desc, e.Path)
	default:
		return fmt.Sprintf("%s — error: %s", desc, e.Err.Error())
	}
}
