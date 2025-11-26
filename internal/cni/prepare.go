/*
Copyright 2025 Flant JSC

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

package cni

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	cniSwitchNamespace = "cni-switch"
)

var (
	moduleConfigGVK = schema.GroupVersionKind{
		Group:   "deckhouse.io",
		Version: "v1alpha1",
		Kind:    "ModuleConfig",
	}

	CNIModuleConfigs = []string{"cni-cilium", "cni-flannel", "cni-simple-bridge"}
)

// RunPrepare executes the logic for the 'cni-switch prepare' command.
func RunPrepare(targetCNI string, timeout time.Duration) error {
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("🚀 Starting CNI switch preparation for target '%s' (global timeout: %s)\n", targetCNI, timeout)

	// 1. Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}
	fmt.Printf("✅ Kubernetes client created (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 2. Find an existing migration or create a new one
	activeMigration, err := getOrCreateMigrationForPrepare(ctx, rtClient, targetCNI)
	if err != nil {
		return err
	}
	if activeMigration == nil {
		// This means preparation is already complete, and the user has been notified.
		return nil
	}
	fmt.Printf(
		"✅ Working with CNIMigration '%s' (total elapsed: %s)\n\n",
		activeMigration.Name,
		time.Since(startTime).Round(time.Millisecond),
	)

	// 3. Detect current CNI and update migration status
	if activeMigration.Status.CurrentCNI == "" {
		var currentCNI string
		currentCNI, err = detectCurrentCNI(rtClient)
		if err != nil {
			return fmt.Errorf("detecting current CNI: %w", err)
		}
		fmt.Printf("Detected current CNI: '%s'\n", currentCNI)

		if currentCNI == targetCNI {
			return fmt.Errorf("target CNI '%s' is the same as the current CNI. Nothing to do", targetCNI)
		}

		activeMigration.Status.CurrentCNI = currentCNI
		err = rtClient.Status().Update(ctx, activeMigration)
		if err != nil {
			return fmt.Errorf("updating migration status with current CNI: %w", err)
		}
		fmt.Printf(
			"✅ Added current CNI to migration status (total elapsed: %s)\n\n",
			time.Since(startTime).Round(time.Millisecond),
		)
	}

	// 4. Create the dedicated namespace
	fmt.Printf("Creating dedicated namespace '%s'...\n", cniSwitchNamespace)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cniSwitchNamespace}}
	if err = rtClient.Create(ctx, ns); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace %s: %w", cniSwitchNamespace, err)
	}
	fmt.Printf("✅ Namespace ensured (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 5. Get the helper image name from the d8-cli-data configmap
	cm := &corev1.ConfigMap{}
	if err = rtClient.Get(
		ctx,
		client.ObjectKey{Name: "d8-cli-data", Namespace: "d8-system"},
		cm,
	); err != nil {
		return fmt.Errorf("getting d8-cli-data configmap: %w", err)
	}
	imageName, ok := cm.Data["cni-switch-helper-image"]
	if !ok || imageName == "" {
		return fmt.Errorf("cni-switch-helper-image not found or empty in d8-cli-data configmap")
	}

	// 6. Apply RBAC
	fmt.Println("Applying RBAC...")
	// Agent RBAC
	agentSA := getSwitchHelperServiceAccount(cniSwitchNamespace)
	if err = rtClient.Create(ctx, agentSA); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating agent service account: %w", err)
	}
	agentRole := getSwitchHelperClusterRole()
	if err = rtClient.Create(ctx, agentRole); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating cluster role: %w", err)
	}
	agentBinding := getSwitchHelperClusterRoleBinding(cniSwitchNamespace)
	if err = rtClient.Create(ctx, agentBinding); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating cluster role binding: %w", err)
	}
	// Webhook RBAC
	webhookSA := getWebhookServiceAccount(cniSwitchNamespace)
	if err = rtClient.Create(ctx, webhookSA); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating webhook service account: %w", err)
	}
	webhookRole := getWebhookClusterRole()
	if err = rtClient.Create(ctx, webhookRole); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating webhook cluster role: %w", err)
	}
	webhookBinding := getWebhookClusterRoleBinding(cniSwitchNamespace)
	if err = rtClient.Create(ctx, webhookBinding); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating webhook cluster role binding: %w", err)
	}
	fmt.Printf("✅ RBAC applied (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 7. Create and wait for the mutating webhook
	fmt.Println("Deploying Mutating Webhook for annotating new pods...")
	// Generate certificates
	caCert, serverCert, serverKey, err := generateWebhookCertificates(cniSwitchNamespace)
	if err != nil {
		return fmt.Errorf("generating webhook certificates: %w", err)
	}

	// Create TLS secret
	tlsSecret := getWebhookTLSSecret(cniSwitchNamespace, serverCert, serverKey)
	if err = rtClient.Create(ctx, tlsSecret); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating webhook tls secret: %w", err)
	}

	// Create Deployment
	webhookDeployment := getWebhookDeployment(cniSwitchNamespace, imageName, webhookServiceAccountName)
	if err = rtClient.Create(ctx, webhookDeployment); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating webhook deployment: %w", err)
	}

	// Wait for Deployment to be ready
	if err = waitForDeploymentReady(ctx, rtClient, webhookDeployment); err != nil {
		return fmt.Errorf("waiting for webhook deployment ready: %w", err)
	}

	// Create Service
	webhookService := getWebhookService(cniSwitchNamespace)
	if err = rtClient.Create(ctx, webhookService); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating webhook service: %w", err)
	}

	// Create MutatingWebhookConfiguration
	webhookConfig := getMutatingWebhookConfiguration(cniSwitchNamespace, caCert)
	if err = rtClient.Create(ctx, webhookConfig); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating mutating webhook configuration: %w", err)
	}
	fmt.Printf("✅ Mutating Webhook is active (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 8. Create and wait for the cni-switch-helper daemonset
	dsKey := client.ObjectKey{Name: "cni-switch-helper", Namespace: cniSwitchNamespace}
	ds := &appsv1.DaemonSet{}
	if err = rtClient.Get(ctx, dsKey, ds); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("getting helper daemonset: %w", err)
		}
		fmt.Println("Creating helper DaemonSet 'cni-switch-helper'...")
		dsToCreate := getSwitchHelperDaemonSet(cniSwitchNamespace, imageName)
		if err = rtClient.Create(ctx, dsToCreate); err != nil {
			return fmt.Errorf("creating helper daemonset: %w", err)
		}
		ds = dsToCreate
	} else {
		fmt.Println("Helper DaemonSet 'cni-switch-helper' already exists.")
	}

	if err = waitForDaemonSetReady(ctx, rtClient, ds); err != nil {
		return fmt.Errorf("waiting for daemonset ready: %w", err)
	}
	fmt.Printf("✅ Helper DaemonSet is ready (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 9. Wait for all nodes to be prepared
	fmt.Println("Waiting for all nodes to complete the preparation step...")
	err = waitForNodesPrepared(ctx, rtClient)
	if err != nil {
		return fmt.Errorf("waiting for nodes to be prepared: %w", err)
	}
	fmt.Printf("✅ All nodes are prepared (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 7. Update overall status
	activeMigration.Status.Conditions = append(activeMigration.Status.Conditions, metav1.Condition{
		Type:               "PreparationSucceeded",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "AllNodesPrepared",
		Message:            "All nodes successfully completed the preparation step.",
	})
	err = rtClient.Status().Update(ctx, activeMigration)
	if err != nil {
		return fmt.Errorf("updating CNIMigration status to prepared: %w", err)
	}

	fmt.Printf(
		"🎉 Cluster successfully prepared for CNI switch (total time: %s)\n",
		time.Since(startTime).Round(time.Second),
	)
	fmt.Println("\nYou can now run 'd8 cni-switch switch' to proceed")

	return nil
}

// generateWebhookCertificates creates a self-signed CA and a server certificate for the webhook.
func generateWebhookCertificates(namespace string) (caCert, serverCert, serverKey []byte, err error) {
	// CA configuration
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2025),
		Subject: pkix.Name{
			Organization: []string{"deckhouse.io"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating CA private key: %w", err)
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating CA certificate: %w", err)
	}

	caPEM := new(bytes.Buffer)
	_ = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	// Server certificate configuration
	commonName := fmt.Sprintf("%s.%s.svc", webhookServiceName, namespace)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2026),
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"deckhouse.io"},
		},
		DNSNames:    []string{commonName},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}

	serverPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating server private key: %w", err)
	}

	serverCertBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &serverPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating server certificate: %w", err)
	}

	serverCertPEM := new(bytes.Buffer)
	_ = pem.Encode(serverCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCertBytes,
	})

	serverPrivKeyPEM := new(bytes.Buffer)
	_ = pem.Encode(serverPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(serverPrivKey),
	})

	return caPEM.Bytes(), serverCertPEM.Bytes(), serverPrivKeyPEM.Bytes(), nil
}

func getOrCreateMigrationForPrepare(
	ctx context.Context,
	rtClient client.Client,
	targetCNI string,
) (*v1alpha1.CNIMigration, error) {
	activeMigration, err := FindActiveMigration(ctx, rtClient)
	if err != nil {
		return nil, fmt.Errorf("failed to find active migration: %w", err)
	}

	if activeMigration != nil {
		fmt.Printf("Found active CNIMigration '%s'\n", activeMigration.Name)

		// Check if preparation is already done
		for _, cond := range activeMigration.Status.Conditions {
			if cond.Type == "PreparationSucceeded" && cond.Status == metav1.ConditionTrue {
				fmt.Println("🎉 Cluster has already been prepared for CNI switch.")
				fmt.Println("\nYou can now run 'd8 cni-switch switch' to proceed.")
				return nil, nil // Signal to the caller that we can exit gracefully
			}
		}

		// Check if migration is in an unexpected phase
		if activeMigration.Status.ObservedPhase != "" && activeMigration.Status.ObservedPhase != "Preparing" {
			return nil, fmt.Errorf(
				"an active migration is already in the '%s' phase. "+
					"Cannot run 'prepare' again. To proceed, run 'd8 cni-switch switch'. "+
					"To start over, run 'd8 cni-switch cleanup'",
				activeMigration.Status.ObservedPhase,
			)
		}

		return activeMigration, nil
	}

	migrationName := fmt.Sprintf("cni-migration-%s", time.Now().Format("20060102-150405"))
	fmt.Printf("No active migration found. Creating a new one...\n")

	newMigration := &v1alpha1.CNIMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name: migrationName,
		},
		Spec: v1alpha1.CNIMigrationSpec{
			TargetCNI: targetCNI,
			Phase:     "Prepare",
		},
	}

	err = rtClient.Create(ctx, newMigration)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			fmt.Println("Migration object was created by another process. Getting it.")
			err = rtClient.Get(ctx, client.ObjectKey{Name: migrationName}, newMigration)
			if err != nil {
				return nil, fmt.Errorf("getting existing CNIMigration object: %w", err)
			}
			return newMigration, nil
		}
		return nil, fmt.Errorf("creating new CNIMigration object: %w", err)
	}

	newMigration.Status.ObservedPhase = "Preparing"
	err = rtClient.Status().Update(ctx, newMigration)
	if err != nil {
		return nil, fmt.Errorf("updating status for new CNIMigration object: %w", err)
	}

	fmt.Printf("Successfully created CNIMigration object '%s'\n", newMigration.Name)
	return newMigration, nil
}

func waitForDaemonSetReady(ctx context.Context, rtClient client.Client, ds *appsv1.DaemonSet) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			key := client.ObjectKey{Name: ds.Name, Namespace: ds.Namespace}
			err := rtClient.Get(ctx, key, ds)
			if err != nil {
				fmt.Printf("\n⚠️ Warning: could not get DaemonSet status: %v\n", err)
				continue
			}

			if ds.Status.DesiredNumberScheduled == ds.Status.NumberReady && ds.Status.NumberUnavailable == 0 {
				fmt.Printf(
					"\rWaiting for DaemonSet... %d/%d pods ready\n",
					ds.Status.NumberReady,
					ds.Status.DesiredNumberScheduled,
				)
				return nil
			}

			fmt.Printf(
				"\rWaiting for DaemonSet... %d/%d pods ready",
				ds.Status.NumberReady,
				ds.Status.DesiredNumberScheduled,
			)
		}
	}
}

func waitForDeploymentReady(ctx context.Context, rtClient client.Client, dep *appsv1.Deployment) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			key := client.ObjectKey{Name: dep.Name, Namespace: dep.Namespace}
			err := rtClient.Get(ctx, key, dep)
			if err != nil {
				fmt.Printf("\n⚠️ Warning: could not get Deployment status: %v\n", err)
				continue
			}

			// This is the exit condition for the loop.
			if dep.Spec.Replicas != nil && dep.Status.ReadyReplicas >=
				*dep.Spec.Replicas && dep.Status.UnavailableReplicas == 0 {
				fmt.Printf(
					"\rWaiting for Deployment... %d/%d replicas ready\n",
					dep.Status.ReadyReplicas,
					*dep.Spec.Replicas,
				)
				return nil
			}

			// This is the progress update.
			if dep.Spec.Replicas != nil {
				fmt.Printf(
					"\rWaiting for Deployment... %d/%d replicas ready",
					dep.Status.ReadyReplicas,
					*dep.Spec.Replicas,
				)
			}
		}
	}
}

func waitForNodesPrepared(ctx context.Context, rtClient client.Client) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes := &corev1.NodeList{}
			if err := rtClient.List(ctx, nodes); err != nil {
				fmt.Printf("⚠️ Warning: could not list nodes: %v\n", err)
				continue
			}
			totalNodes := len(nodes.Items)

			migrations := &v1alpha1.CNINodeMigrationList{}
			if err := rtClient.List(ctx, migrations); err != nil {
				fmt.Printf("⚠️ Warning: could not list node migrations: %v\n", err)
				continue
			}

			readyNodes := 0
			for _, migration := range migrations.Items {
				for _, cond := range migration.Status.Conditions {
					if cond.Type == "PreparationSucceeded" && cond.Status == metav1.ConditionTrue {
						readyNodes++
						break
					}
				}
			}

			fmt.Printf("\rProgress: %d/%d nodes prepared...", readyNodes, totalNodes)

			if readyNodes >= totalNodes && totalNodes > 0 {
				fmt.Printf("\rProgress: %d/%d nodes prepared...\n", readyNodes, totalNodes)
				return nil
			}
		}
	}
}

func detectCurrentCNI(rtClient client.Client) (string, error) {
	var enabledCNIs []string
	for _, cniModule := range CNIModuleConfigs {
		mc := &unstructured.Unstructured{}
		mc.SetGroupVersionKind(moduleConfigGVK)

		err := rtClient.Get(context.Background(), client.ObjectKey{Name: cniModule}, mc)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return "", fmt.Errorf("getting module config %s: %w", cniModule, err)
		}

		enabled, found, err := unstructured.NestedBool(mc.Object, "spec", "enabled")
		if err != nil {
			return "", fmt.Errorf("parsing 'spec.enabled' for module config %s: %w", cniModule, err)
		}

		if found && enabled {
			cniName := strings.TrimPrefix(cniModule, "cni-")
			enabledCNIs = append(enabledCNIs, cniName)
		}
	}

	if len(enabledCNIs) == 0 {
		return "", fmt.Errorf("no enabled CNI module found. Looked for: %s", strings.Join(CNIModuleConfigs, ", "))
	}

	if len(enabledCNIs) > 1 {
		return "", fmt.Errorf(
			"found multiple enabled CNI modules: %s. Please disable all but one",
			strings.Join(enabledCNIs, ", "),
		)
	}

	return enabledCNIs[0], nil
}
