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
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/deckhouse/deckhouse/go_lib/controlplane/kubeconfig"
	"github.com/deckhouse/deckhouse/go_lib/controlplane/pki"
)

// CertEntry represents a non-CA certificate in the report.
type CertEntry struct {
	Name      string
	Expires   time.Time
	Authority string
}

// CAEntry represents a CA certificate in the report.
type CAEntry struct {
	Name    string
	Expires time.Time
}

// Report holds the result of a certificate expiration check.
type Report struct {
	Certs []CertEntry
	CAs   []CAEntry
}

// BuildFullScanReport enumerates all known control-plane certificates and kubeconfig
// client certificates, returning a report split into CAs and leaf certs.
// certsDir is the PKI directory (e.g. /etc/kubernetes/pki).
// kubeconfigDir is the directory containing kubeconfig files (e.g. /etc/kubernetes).
// Callers that want the standard layout can pass filepath.Dir(certsDir).
func BuildFullScanReport(certsDir, kubeconfigDir string) (*Report, error) {
	pkiExpirations, err := pki.ListCertificateExpirations(pki.WithCertificatesDir(certsDir))
	if err != nil {
		return nil, fmt.Errorf("listing PKI certificates in %q: %w", certsDir, err)
	}

	kcExpirations, err := kubeconfig.ListClientCertificateExpirations(kubeconfig.WithKubeconfigDir(kubeconfigDir))
	if err != nil {
		return nil, fmt.Errorf("listing kubeconfig client certificates in %q: %w", kubeconfigDir, err)
	}

	report := &Report{}

	for _, exp := range pkiExpirations {
		if exp.IsCA {
			report.CAs = append(report.CAs, CAEntry{
				Name:    pkiDisplayName(exp.Name),
				Expires: exp.NotAfter,
			})
		} else {
			report.Certs = append(report.Certs, CertEntry{
				Name:      pkiDisplayName(exp.Name),
				Expires:   exp.NotAfter,
				Authority: pkiDisplayName(string(exp.Authority)),
			})
		}
	}

	for _, exp := range kcExpirations {
		report.Certs = append(report.Certs, CertEntry{
			Name:      kubeconfigDisplayName(exp.File),
			Expires:   exp.NotAfter,
			Authority: string(pki.CACertName),
		})
	}

	sort.Slice(report.Certs, func(i, j int) bool {
		return report.Certs[i].Name < report.Certs[j].Name
	})
	sort.Slice(report.CAs, func(i, j int) bool {
		return report.CAs[i].Name < report.CAs[j].Name
	})

	return report, nil
}

// BuildSingleFileReport inspects a single file at path.
// It tries kubeconfig parsing first; if that fails it falls back to PEM certificate
// parsing. If both parsers fail, the combined error is returned.
func BuildSingleFileReport(path string) (*Report, error) {
	kcExp, kcErr := kubeconfig.GetClientCertificateExpiration(path)
	if kcErr == nil {
		return &Report{
			Certs: []CertEntry{{
				Name:      kubeconfigDisplayName(kcExp.File),
				Expires:   kcExp.NotAfter,
				Authority: string(pki.CACertName),
			}},
		}, nil
	}

	certExp, certErr := pki.GetCertificateExpiration(path)
	if certErr == nil {
		report := &Report{}
		if certExp.IsCA {
			report.CAs = []CAEntry{{Name: pkiDisplayName(certExp.Name), Expires: certExp.NotAfter}}
		} else {
			report.Certs = []CertEntry{{
				Name:      pkiDisplayName(certExp.Name),
				Expires:   certExp.NotAfter,
				Authority: pkiDisplayName(string(certExp.Authority)),
			}}
		}
		return report, nil
	}

	return nil, errors.Join(
		fmt.Errorf("kubeconfig: %w", kcErr),
		fmt.Errorf("certificate: %w", certErr),
	)
}

// RenderReport writes the certificate expiration report to w in two sections:
// leaf certificates followed by certificate authorities.
func RenderReport(w io.Writer, report *Report) {
	now := time.Now().UTC()

	if len(report.Certs) > 0 {
		tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		fmt.Fprintln(tw, "CERTIFICATE\tEXPIRES\tRESIDUAL TIME\tCERTIFICATE AUTHORITY")
		for _, c := range report.Certs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				c.Name,
				c.Expires.UTC().Format("Jan 02, 2006 15:04 MST"),
				residualTime(c.Expires, now),
				c.Authority,
			)
		}
		tw.Flush()
	}

	if len(report.CAs) > 0 {
		if len(report.Certs) > 0 {
			fmt.Fprintln(w)
		}
		tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		fmt.Fprintln(tw, "CERTIFICATE AUTHORITY\tEXPIRES\tRESIDUAL TIME")
		for _, ca := range report.CAs {
			fmt.Fprintf(tw, "%s\t%s\t%s\n",
				ca.Name,
				ca.Expires.UTC().Format("Jan 02, 2006 15:04 MST"),
				residualTime(ca.Expires, now),
			)
		}
		tw.Flush()
	}
}

// residualTime formats the duration between notAfter and now in a compact human-readable form.
// Years are computed as totalDays/365 (integer division), matching the kubeadm
// "certs check-expiration" approximation.  A leap year therefore still reads as
// "1y" at 366 days, and 730 days reads as "2y" regardless of calendar years.
func residualTime(notAfter, now time.Time) string {
	if !notAfter.After(now) {
		return "expired"
	}
	d := notAfter.Sub(now)
	totalDays := int(d.Hours() / 24)
	if totalDays < 1 {
		return "< 1 day"
	}
	if totalDays < 365 {
		return fmt.Sprintf("%dd", totalDays)
	}
	years := totalDays / 365
	return fmt.Sprintf("%dy", years)
}

// pkiDisplayName returns a display-friendly certificate name for CLI output.
// PKI inventory uses slash-separated names for nested etcd paths; the CLI renders
// them with dashes to match kubeadm-style output better.
func pkiDisplayName(name string) string {
	return strings.ReplaceAll(name, "/", "-")
}

// kubeconfigDisplayName returns a display-friendly name for a kubeconfig file:
// it strips the directory component and preserves the .conf suffix.
func kubeconfigDisplayName(file kubeconfig.File) string {
	return filepath.Base(string(file))
}
