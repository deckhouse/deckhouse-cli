package whitelist

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestMatches(t *testing.T) {
	type args struct {
		obj runtime.Object
	}
	tests := []struct {
		name string
		want bool
		args args
	}{
		{
			name: "plain unstructured resource in whitelist",
			want: true,
			args: args{
				obj: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":      "deckhouse-registry",
						"namespace": "d8-system",
					},
				}},
			},
		},
		{
			name: "Secret in whitelist",
			want: true,
			args: args{
				obj: &corev1.Secret{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Secret",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "deckhouse-registry",
						Namespace: "d8-system",
					},
				},
			},
		},
		{
			name: "plain unstructured resource not in whitelist",
			want: false,
			args: args{
				obj: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":      "kube-root-ca.crt",
						"namespace": "kube-system",
					},
				}},
			},
		},
		{
			name: "Secret not in whitelist",
			want: false,
			args: args{
				obj: &corev1.Secret{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Secret",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "kube-root-ca.crt",
						Namespace: "kube-system",
					},
				},
			},
		},
		{
			name: "matching regexp of resource in whitelist",
			want: true,
			args: args{
				obj: &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Secret",
					"metadata": map[string]interface{}{
						"name":      "d8-node-terraform-state-dev-master-0",
						"namespace": "d8-system",
					},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := BakedInFilter{}
			if got := f.Matches(tt.args.obj); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}
