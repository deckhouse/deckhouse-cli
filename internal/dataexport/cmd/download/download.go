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

package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/util"
)

const (
	cmdName = "download"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf("  # Download file from Filesystem type volume"),
		fmt.Sprintf("    ... %s [flags] file_data_name path/file.ext [-o out_file.ext] [--publish]", cmdName),
		fmt.Sprintf(`    ... %s -n target-namespace my-file-volume mydir/testdir/largeimage.iso -o largeimage.iso`, cmdName),
		fmt.Sprintf("  # Download raw data from Block type volume"),
		fmt.Sprintf("    ... %s [flags] block_data_name [-o out_file.ext] [--publish]", cmdName),
		fmt.Sprintf(`    ... %s -n target-namespace my-block-volume -o largeimage.raw --publish`, cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(clientCmdConfig clientcmd.ClientConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_export_name [path/file.ext]",
		Short:   "Download exported data",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd, args, clientCmdConfig)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			_, _, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().StringP("output", "o", "", "file to save data (default: same as resource)")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")

	return cmd
}

func parseArgs(args []string) (deName, srcPath string, err error) {
	switch len(args) {
	case 1:
		deName = args[0]
	case 2:
		deName = args[0]
		srcPath = args[1]
	default:
		return "", "", fmt.Errorf("invalid arguments")
	}

	if len(srcPath) > 0 && srcPath[:1] != "/" {
		srcPath = "/" + srcPath
	}

	return
}

func Run(cmd *cobra.Command, args []string, clientCmdConfig clientcmd.ClientConfig) error {
	ctx := context.Background()
	namespace, _ := cmd.Flags().GetString("namespace")
	dstPath, _ := cmd.Flags().GetString("output")
	publish, _ := cmd.Flags().GetBool("publish")
	pvName := ""

	deName, srcPath, err := parseArgs(args)
	if err != nil {
		return fmt.Errorf("arguments parsing error: %s", err.Error())
	}

	rtClient, err := util.NewKubeRTClient(clientCmdConfig)
	if err != nil {
		return err
	}

	deObj, err := util.GetDataExportWithRestart(deName, namespace, rtClient)
	if err != nil {
		return err
	}

	podUrl := deObj.Status.Url
	if podUrl == "" {
		return fmt.Errorf("DataExport %s/%s has no URL", deObj.ObjectMeta.Namespace, deObj.ObjectMeta.Name)
	}

	volumeKind := deObj.Spec.TargetRef.Kind
	if !slices.Contains([]string{"PersistentVolumeClaim", "VolumeSnapshot"}, volumeKind) {
		return fmt.Errorf("invalid volume kind: %s", volumeKind)
	}
	volumeName := deObj.Spec.TargetRef.Name
	fmt.Printf("volumeKind %s, deName %s, srcPath %s, dstPath %s, publish %v\n", volumeKind, deName, srcPath, dstPath, publish)

	// Get volumeMode from k8s PVC/VS
	volumeMode := ""
	switch volumeKind {
	case "PersistentVolumeClaim":
		pvc := corev1.PersistentVolumeClaim{}
		err := rtClient.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: volumeName}, &pvc)
		if err != nil {
			return fmt.Errorf("kube Get pvc: %s", err.Error())
		}

		volumeMode = string(*pvc.Spec.VolumeMode)
		pvName = pvc.Spec.VolumeName
	case "VolumeSnapshot":
		return fmt.Errorf("%s VolumeSnapshot is not implemented yet", cmdName)
	}
	fmt.Printf("volumeMode %s, pvName %s\n", volumeMode, pvName)

	// Validate srcPath, dstPath params
	switch volumeMode {
	case "Filesystem":
		if srcPath == "" || srcPath[len(srcPath)-1:] == "/" {
			return fmt.Errorf("invalid source path: '%s'", srcPath)
		}
		if dstPath == "" {
			pathList := strings.Split(srcPath, "/")
			dstPath = pathList[len(pathList)-1]
		}
	case "Block":
		if dstPath == "" {
			dstPath = deName
		}
	default:
		return fmt.Errorf("invalid volume mode: %s", volumeMode)
	}

	// Authorization token
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "d8-data-exporter", Name: "data-exporter"}}

	token := &authenticationv1.TokenRequest{}
	err = rtClient.SubResource("token").Create(ctx, sa, token)
	if err != nil {
		return fmt.Errorf("kube Create token: %s", err.Error())
	}
	fmt.Printf("Token %#v\n\n", token.Status.Token)

	// Authentication access
	crBinding := &rbacv1.ClusterRoleBinding{}
	err = rtClient.Get(ctx, client.ObjectKey{Name: "data-exporter-binding"}, crBinding)
	if err != nil && apierrors.IsNotFound(err) {
		crBinding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data-exporter-binding",
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "cluster-admin", //TODO create user with role by controller
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      "data-exporter",
					Namespace: "d8-data-exporter",
				},
			},
		}
		err = rtClient.Create(ctx, crBinding)
	}
	if err != nil {
		return err
	}

	// Download file from backend
	req, _ := http.NewRequest("GET", podUrl+"/api/v1/files"+srcPath, nil)
	req.Header.Add("Authorization", "Bearer "+token.Status.Token)
	httpClient := &http.Client{}
	resp, _ := httpClient.Do(req)
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		if resp.ContentLength > 0 && resp.ContentLength < 1000 {
			msg, err := io.ReadAll(resp.Body)
			if err == nil {
				return fmt.Errorf("Backend response \"%s\" Msg: %s", resp.Status, string(msg))
			}
		}

		return fmt.Errorf("Backend response \"%s\"", resp.Status)
	}

	// Create out file
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	fmt.Printf("Downloaded in %s\n", dstPath)
	return nil
}
