package operatemodule

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
)

func TestOperateModule(t *testing.T) {
	type args struct {
		dynamicClient dynamic.Interface
		name          string
		moduleState   ModuleState
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "check enabled test",
			args: args{
				dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme(), &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleConfig",
					"metadata":   map[string]any{"name": "stronghold", "namespace": ""},
					"spec": map[string]interface{}{
						"enabled": false,
					},
				}}),
				name:        "stronghold",
				moduleState: ModuleEnabled,
			},
		},
		{
			name:    "check not exist module",
			wantErr: false,
			args: args{
				dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme(), &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleConfig",
					"metadata":   map[string]any{"name": "deckhouse", "namespace": ""},
					"spec": map[string]interface{}{
						"enabled": false,
					},
				}}),
				name:        "stronghold",
				moduleState: ModuleEnabled,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := OperateModule(tt.args.dynamicClient, tt.args.name, tt.args.moduleState); (err != nil) != tt.wantErr {
				t.Errorf("OperateModule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
