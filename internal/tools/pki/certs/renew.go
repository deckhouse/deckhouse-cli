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

package certs

import (
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse/go_lib/controlplane/constants"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/kubeconfig"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/pki"
)

// RunRenewAll renews every known control-plane leaf certificate and kubeconfig client certificate.
func RunRenewAll(w io.Writer, certsDir, kubeconfigDirOverride string, dryRun bool, extraIP net.IP) error {
	kcDir := effectiveKubeconfigDir(certsDir, kubeconfigDirOverride)

	printDryRunBanner(w, dryRun)

	var warnings []string
	usedCAs := map[pki.RootCertName]struct{}{}

	leafOpts := []pki.RenewOption{pki.WithRenewDir(certsDir)}
	if dryRun {
		leafOpts = append(leafOpts, pki.WithDryRun())
	}
	if extraIP != nil {
		leafOpts = append(leafOpts, pki.WithRenewExtraIP(extraIP))
	}

	warnings = append(warnings, collectLeafWarnings(w, pki.RenewCertificates(leafOpts...), usedCAs)...)

	kcOpts := []kubeconfig.RenewOption{
		kubeconfig.WithRenewKubeconfigDir(kcDir),
		kubeconfig.WithRenewPKIDir(certsDir),
	}
	if dryRun {
		kcOpts = append(kcOpts, kubeconfig.WithDryRun())
	}

	warnings = append(warnings, collectKubeconfigWarnings(w, kubeconfig.RenewClientCerts(kcOpts...), usedCAs)...)

	warnings = append(warnings, checkCAsOutliveRenewed(w, certsDir, usedCAs)...)

	printDryRunFooter(w, dryRun)

	return warningsError(warnings)
}

// RunRenewSingle renews a single artifact identified by path: it auto-detects whether path is a kubeconfig file or a PEM leaf certificate.
func RunRenewSingle(w io.Writer, path, certsDir, kubeconfigDirOverride string, dryRun bool) error {
	if kcExp, err := kubeconfig.GetClientCertificateExpiration(path); err == nil {
		return renewSingleKubeconfig(w, path, kcExp.File, certsDir, kubeconfigDirOverride, dryRun)
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

func effectiveKubeconfigDir(certsDir, override string) string {
	if override != "" {
		return override
	}
	return filepath.Dir(certsDir)
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

	usedCAs := map[pki.RootCertName]struct{}{}
	warnings := collectLeafWarnings(w, pki.RenewCertificates(opts...), usedCAs)
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
		kubeconfig.WithRenewFiles(file),
	}
	if dryRun {
		opts = append(opts, kubeconfig.WithDryRun())
	}

	usedCAs := map[pki.RootCertName]struct{}{}
	warnings := collectKubeconfigWarnings(w, kubeconfig.RenewClientCerts(opts...), usedCAs)
	warnings = append(warnings, checkCAsOutliveRenewed(w, certsDirOverride, usedCAs)...)

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

	report, err := pki.ListCertificateExpirations(
		pki.WithCertificatesDir(certsDir),
		pki.WithRootCertificates(cas...),
	)
	if err != nil {
		// It shouldn't happen, but return error through as a warning.
		line := fmt.Sprintf("WARNING: cannot check CA expiration: %v", err)
		fmt.Fprintln(w, line)
		return []string{line}
	}

	threshold := time.Now().Add(constants.CertificateValidityPeriod)
	var warnings []string
	for _, e := range report.Entries {
		if e.Err != nil {
			// A CA read failure here is already surfaced via the renew entry above.
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
	return fmt.Errorf("%d certificate(s) not renewed; see output above", len(warnings))
}

// collectLeafWarnings prints a progress line for every leaf renewal entry
func collectLeafWarnings(w io.Writer, report pki.PKIRenewReport, usedCAs map[pki.RootCertName]struct{}) []string {
	var warnings []string
	for _, e := range report.Entries {
		line := formatLeafEntry(pki.LeafDescription(e.Name), e)
		fmt.Fprintln(w, line)
		if e.Err != nil {
			warnings = append(warnings, line)
			continue
		}
		usedCAs[e.Authority] = struct{}{}
	}
	return warnings
}

// collectKubeconfigWarnings mirrors collectLeafWarnings for kubeconfigs
func collectKubeconfigWarnings(w io.Writer, report kubeconfig.KubeconfigRenewReport, usedCAs map[pki.RootCertName]struct{}) []string {
	var warnings []string
	for _, e := range report.Entries {
		line := formatKubeconfigEntry(kubeconfig.FileDescription(e.File), e)
		fmt.Fprintln(w, line)
		if e.Err != nil {
			warnings = append(warnings, line)
			continue
		}
		usedCAs[pki.CACertName] = struct{}{}
	}
	return warnings
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
