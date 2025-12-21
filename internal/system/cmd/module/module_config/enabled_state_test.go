package module_config

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
)

func TestSetEnabledState(t *testing.T) {
	type args struct {
		dynamicClient dynamic.Interface
		name          string
		state         EnabledState
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantStatus ResultStatus
	}{
		{
			name: "enable disabled module",
			args: args{
				dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme(), &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleConfig",
					"metadata":   map[string]any{"name": "stronghold", "namespace": ""},
					"spec": map[string]interface{}{
						"enabled": false,
					},
				}}),
				name:  "stronghold",
				state: Enabled,
			},
			wantStatus: Changed,
		},
		{
			name: "enable already enabled module",
			args: args{
				dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme(), &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleConfig",
					"metadata":   map[string]any{"name": "stronghold", "namespace": ""},
					"spec": map[string]interface{}{
						"enabled": true,
					},
				}}),
				name:  "stronghold",
				state: Enabled,
			},
			wantStatus: AlreadyInState,
		},
		{
			name: "disable already disabled module",
			args: args{
				dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme(), &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleConfig",
					"metadata":   map[string]any{"name": "stronghold", "namespace": ""},
					"spec": map[string]interface{}{
						"enabled": false,
					},
				}}),
				name:  "stronghold",
				state: Disabled,
			},
			wantStatus: AlreadyInState,
		},
		{
			name:    "create module config if not exists",
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
				name:  "stronghold",
				state: Enabled,
			},
			wantStatus: Changed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SetEnabledState(tt.args.dynamicClient, tt.args.name, tt.args.state)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetEnabledState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result.Status != tt.wantStatus {
				t.Errorf("SetEnabledState() status = %v, want %v", result.Status, tt.wantStatus)
			}
		})
	}
}

func TestIsInState(t *testing.T) {
	tests := []struct {
		name         string
		obj          *unstructured.Unstructured
		desiredState EnabledState
		want         bool
	}{
		{
			name: "module enabled, want enabled",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"spec": map[string]interface{}{
					"enabled": true,
				},
			}},
			desiredState: Enabled,
			want:         true,
		},
		{
			name: "module enabled, want disabled",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"spec": map[string]interface{}{
					"enabled": true,
				},
			}},
			desiredState: Disabled,
			want:         false,
		},
		{
			name: "module disabled, want disabled",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"spec": map[string]interface{}{
					"enabled": false,
				},
			}},
			desiredState: Disabled,
			want:         true,
		},
		{
			name: "module disabled, want enabled",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"spec": map[string]interface{}{
					"enabled": false,
				},
			}},
			desiredState: Enabled,
			want:         false,
		},
		{
			name: "no enabled field",
			obj: &unstructured.Unstructured{Object: map[string]interface{}{
				"spec": map[string]interface{}{},
			}},
			desiredState: Enabled,
			want:         false,
		},
		{
			name:         "no spec field",
			obj:          &unstructured.Unstructured{Object: map[string]interface{}{}},
			desiredState: Enabled,
			want:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isInState(tt.obj, tt.desiredState); got != tt.want {
				t.Errorf("isInState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseOperationError(t *testing.T) {
	tests := []struct {
		name       string
		moduleName string
		err        error
		wantExp    bool
		wantModule string
	}{
		{
			name:       "experimental module error",
			moduleName: "neuvector",
			err:        errors.New("admission webhook denied: the 'neuvector' module is experimental"),
			wantExp:    true,
			wantModule: "neuvector",
		},
		{
			name:       "other error",
			moduleName: "test",
			err:        errors.New("some other error"),
			wantExp:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseOperationError(tt.moduleName, tt.err)
			var expErr *ExperimentalModuleError
			isExp := errors.As(result, &expErr)
			if isExp != tt.wantExp {
				t.Errorf("parseOperationError() isExperimental = %v, want %v", isExp, tt.wantExp)
			}
			if isExp && expErr.ModuleName != tt.wantModule {
				t.Errorf("parseOperationError() moduleName = %v, want %v", expErr.ModuleName, tt.wantModule)
			}
		})
	}
}
