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

package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"
	sigsyaml "sigs.k8s.io/yaml"
)

const (
	FormatHuman = "human"
	FormatJSON  = "json"
	FormatYAML  = "yaml"

	snapshotGroup    = "storage.deckhouse.io"
	snapshotVersion  = "v1alpha1"
	snapshotListKind = "SnapshotList"
)

// SnapshotInfo holds the display fields for one Snapshot CR.
type SnapshotInfo struct {
	Namespace   string    `json:"namespace"`
	Name        string    `json:"name"`
	Ready       bool      `json:"ready"`
	ReadyReason string    `json:"readyReason,omitempty"`
	Content     string    `json:"content,omitempty"`
	Children    int       `json:"children"`
	Created     time.Time `json:"created"`
}

// List returns SnapshotInfo for all Snapshot CRs visible via rtClient.
// When namespace is non-empty only that namespace is queried.
func List(ctx context.Context, rtClient ctrlrtclient.Client, namespace string) ([]SnapshotInfo, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   snapshotGroup,
		Version: snapshotVersion,
		Kind:    snapshotListKind,
	})

	var listOpts []ctrlrtclient.ListOption
	if namespace != "" {
		listOpts = append(listOpts, ctrlrtclient.InNamespace(namespace))
	}

	if err := rtClient.List(ctx, list, listOpts...); err != nil {
		return nil, fmt.Errorf("list Snapshot CRs: %w", err)
	}

	infos := make([]SnapshotInfo, 0, len(list.Items))

	for i := range list.Items {
		infos = append(infos, toInfo(&list.Items[i]))
	}

	return infos, nil
}

func toInfo(obj *unstructured.Unstructured) SnapshotInfo {
	info := SnapshotInfo{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

	if ts := obj.GetCreationTimestamp(); !ts.IsZero() {
		info.Created = ts.Time
	}

	info.Content, _, _ = unstructured.NestedString(obj.Object, "status", "boundSnapshotContentName")

	info.Ready, info.ReadyReason = readyStatus(obj)

	children, _, _ := unstructured.NestedSlice(obj.Object, "status", "childrenSnapshotRefs")
	info.Children = len(children)

	return info
}

func readyStatus(obj *unstructured.Unstructured) (bool, string) {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")

	for _, raw := range conditions {
		c, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		condType, _ := c["type"].(string)
		if condType != "Ready" {
			continue
		}

		status, _ := c["status"].(string)
		if status == "True" {
			return true, ""
		}

		msg, _ := c["message"].(string)
		if msg != "" {
			return false, msg
		}

		reason, _ := c["reason"].(string)
		return false, reason
	}

	return false, "Ready condition not found"
}

// Render writes infos to w in the requested format.
// showNamespace controls whether the NAMESPACE column appears in human output.
func Render(w io.Writer, infos []SnapshotInfo, format string, showNamespace bool) error {
	switch format {
	case FormatJSON:
		return renderJSON(w, infos)
	case FormatYAML:
		return renderYAML(w, infos)
	default:
		return renderHuman(w, infos, showNamespace)
	}
}

func renderJSON(w io.Writer, infos []SnapshotInfo) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(infos)
}

func renderYAML(w io.Writer, infos []SnapshotInfo) error {
	data, err := json.Marshal(infos)
	if err != nil {
		return fmt.Errorf("marshal for yaml: %w", err)
	}

	out, err := sigsyaml.JSONToYAML(data)
	if err != nil {
		return fmt.Errorf("convert to yaml: %w", err)
	}

	_, err = w.Write(out)

	return err
}

func renderHuman(w io.Writer, infos []SnapshotInfo, showNamespace bool) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	if showNamespace {
		fmt.Fprintln(tw, "NAMESPACE\tNAME\tREADY\tCONTENT\tCHILDREN\tAGE")
	} else {
		fmt.Fprintln(tw, "NAME\tREADY\tCONTENT\tCHILDREN\tAGE")
	}

	for i := range infos {
		info := &infos[i]

		readyStr := "False"
		if info.Ready {
			readyStr = "True"
		}

		contentStr := info.Content
		if contentStr == "" {
			contentStr = "<none>"
		}

		age := formatAge(time.Since(info.Created))

		if showNamespace {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
				info.Namespace, info.Name, readyStr, contentStr, info.Children, age)
		} else {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
				info.Name, readyStr, contentStr, info.Children, age)
		}
	}

	return tw.Flush()
}

func formatAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		days := int(d.Hours()) / 24
		return fmt.Sprintf("%dd", days)
	}
}
