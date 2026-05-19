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
	"bytes"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse/go_lib/controlplane/kubeconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResidualTime(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		notAfter time.Time
		want     string
	}{
		{
			name:     "expired",
			notAfter: now.Add(-time.Hour),
			want:     "expired",
		},
		{
			name:     "less than a day",
			notAfter: now.Add(23 * time.Hour),
			want:     "< 1 day",
		},
		{
			name:     "one day",
			notAfter: now.Add(24 * time.Hour),
			want:     "1d",
		},
		{
			name:     "30 days",
			notAfter: now.Add(30 * 24 * time.Hour),
			want:     "30d",
		},
		{
			name:     "364 days",
			notAfter: now.Add(364 * 24 * time.Hour),
			want:     "364d",
		},
		{
			name:     "365 days (1 year)",
			notAfter: now.Add(365 * 24 * time.Hour),
			want:     "1y",
		},
		{
			name:     "9 years",
			notAfter: now.Add(9 * 365 * 24 * time.Hour),
			want:     "9y",
		},
		// The year boundary uses integer division by 365 (kubeadm-style approximation).
		// A leap year (366 days) therefore still reports as "1y", and 730 days
		// reports as "2y" regardless of whether those days span actual calendar years.
		{
			name:     "leap year boundary: 366 days rounds as 1y",
			notAfter: now.Add(366 * 24 * time.Hour),
			want:     "1y",
		},
		{
			name:     "730 days reports as 2y (kubeadm approximation)",
			notAfter: now.Add(730 * 24 * time.Hour),
			want:     "2y",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := residualTime(tt.notAfter, now)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRenderReport_CertsSection(t *testing.T) {
	expires := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	report := &Report{
		Certs: []CertEntry{
			{Name: "apiserver", Expires: expires, Authority: "ca"},
		},
	}

	var buf bytes.Buffer
	RenderReport(&buf, report)
	out := buf.String()

	require.Contains(t, out, "CERTIFICATE")
	require.Contains(t, out, "EXPIRES")
	require.Contains(t, out, "RESIDUAL TIME")
	require.Contains(t, out, "CERTIFICATE AUTHORITY")
	require.Contains(t, out, "EXTERNALLY MANAGED")
	require.Contains(t, out, "apiserver")
	require.Contains(t, out, "ca")
	require.Contains(t, out, "no")
}

func TestRenderReport_CAsSection(t *testing.T) {
	expires := time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC)

	report := &Report{
		CAs: []CAEntry{
			{Name: "ca", Expires: expires},
		},
	}

	var buf bytes.Buffer
	RenderReport(&buf, report)
	out := buf.String()

	require.Contains(t, out, "CERTIFICATE AUTHORITY")
	require.Contains(t, out, "EXPIRES")
	require.Contains(t, out, "ca")
	require.Contains(t, out, "no")
}

func TestRenderReport_BothSections(t *testing.T) {
	expires := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	report := &Report{
		Certs: []CertEntry{
			{Name: "apiserver", Expires: expires, Authority: "ca"},
		},
		CAs: []CAEntry{
			{Name: "ca", Expires: expires},
		},
	}

	var buf bytes.Buffer
	RenderReport(&buf, report)
	out := buf.String()

	require.Contains(t, out, "apiserver")
	require.Contains(t, out, "CERTIFICATE AUTHORITY")
	require.Contains(t, out, "ca")

	// Should have a blank separator line between sections
	require.Contains(t, out, "\n\n")
}

func TestKubeconfigDisplayName(t *testing.T) {
	tests := []struct {
		input kubeconfig.File
		want  string
	}{
		{kubeconfig.Admin, "admin.conf"},
		{kubeconfig.ControllerManager, "controller-manager.conf"},
		{kubeconfig.Scheduler, "scheduler.conf"},
		{kubeconfig.SuperAdmin, "super-admin.conf"},
		{kubeconfig.Kubelet, "kubelet.conf"},
		{"/etc/kubernetes/admin.conf", "admin.conf"},
	}
	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			got := kubeconfigDisplayName(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPkiDisplayName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"apiserver", "apiserver"},
		{"front-proxy-ca", "front-proxy-ca"},
		{"etcd/server", "etcd-server"},
		{"etcd/healthcheck-client", "etcd-healthcheck-client"},
		{"etcd/ca", "etcd-ca"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, pkiDisplayName(tt.input))
		})
	}
}
