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

package user

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

func userOpListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		iamtypes.UserOperationGVR: "UserOperationList",
	}
}

// TestUserOpDefs_Registered locks down the entries the factory generates so
// a future refactor can't quietly drop one of the supported verbs. The full
// command tree wiring is covered separately in iam_test.go; here we only
// care that the factory itself sees lock / unlock / reset2fa.
func TestUserOpDefs_Registered(t *testing.T) {
	uses := make([]string, 0, len(userOpDefs))
	for _, d := range userOpDefs {
		uses = append(uses, strings.SplitN(d.Use, " ", 2)[0])
	}
	assert.ElementsMatch(t, []string{"lock", "unlock", "reset2fa"}, uses,
		"userOpDefs must continue to drive lock / unlock / reset2fa")
}

// TestRunUserOperation_NoWait_BuildsObjectShape covers the wire shape of the
// UserOperation CR that the factory submits. All ops share apiVersion, kind,
// initiatorType="admin", spec.type and spec.user; per-op fields land under
// ExtraSpec. We use --wait=false to avoid simulating the controller's
// status.phase progression.
func TestRunUserOperation_NoWait_BuildsObjectShape(t *testing.T) {
	cases := []struct {
		name      string
		req       userOpRequest
		expectKey string // present under spec for op-specific extras
	}{
		{
			name: "Lock carries spec.lock.for",
			req: userOpRequest{
				NamePrefix: "op-lock-",
				OpType:     "Lock",
				User:       "anton",
				ExtraSpec:  map[string]any{"lock": map[string]any{"for": "30m"}},
			},
			expectKey: "lock",
		},
		{
			name: "Unlock has no extra spec field",
			req: userOpRequest{
				NamePrefix: "op-unlock-",
				OpType:     "Unlock",
				User:       "anton",
			},
		},
		{
			name: "Reset2FA has no extra spec field",
			req: userOpRequest{
				NamePrefix: "op-reset2fa-",
				OpType:     "Reset2FA",
				User:       "anton",
			},
		},
		{
			name: "ResetPassword carries spec.resetPassword.newPasswordHash",
			req: userOpRequest{
				NamePrefix: "op-resetpw-",
				OpType:     "ResetPassword",
				User:       "anton",
				ExtraSpec: map[string]any{
					"resetPassword": map[string]any{
						"newPasswordHash": "$2a$10$abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghi",
					},
				},
			},
			expectKey: "resetPassword",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, userOpListKinds())

			cmd := &cobra.Command{}
			out := &bytes.Buffer{}
			cmd.SetOut(out)
			cmd.SetErr(out)
			cmd.SetContext(t.Context())
			addWaitFlags(cmd)
			require.NoError(t, cmd.Flags().Set("wait", "false"))

			require.NoError(t, runUserOperation(cmd, dyn, tc.req))

			// Exactly one UserOperation must have been created.
			list, err := dyn.Resource(iamtypes.UserOperationGVR).List(t.Context(), metav1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, list.Items, 1)

			obj := list.Items[0]
			assert.Equal(t, iamtypes.APIVersionDeckhouseV1, obj.GetAPIVersion())
			assert.Equal(t, iamtypes.KindUserOperation, obj.GetKind())
			assert.True(t, strings.HasPrefix(obj.GetName(), tc.req.NamePrefix),
				"generated name must start with NamePrefix %q, got %q",
				tc.req.NamePrefix, obj.GetName())

			user, _, _ := unstructured.NestedString(obj.Object, "spec", "user")
			opType, _, _ := unstructured.NestedString(obj.Object, "spec", "type")
			initiator, _, _ := unstructured.NestedString(obj.Object, "spec", "initiatorType")
			assert.Equal(t, tc.req.User, user)
			assert.Equal(t, tc.req.OpType, opType)
			assert.Equal(t, "admin", initiator,
				"all CLI-issued UserOperations must be initiatorType=admin")

			if tc.expectKey != "" {
				_, found, _ := unstructured.NestedMap(obj.Object, "spec", tc.expectKey)
				assert.Truef(t, found, "spec.%s must be present", tc.expectKey)
			}

			// --wait=false prints the operation name on stdout and returns;
			// the success line ("Succeeded: ...") is only printed in the
			// wait branch.
			assert.Contains(t, out.String(), obj.GetName())
			assert.NotContains(t, out.String(), "Succeeded")
		})
	}
}

// TestRunUserOperation_NameIsUnique guards against the regression where two
// rapid invocations generate the same metadata.name and the second Create
// fails AlreadyExists. The factory uses time.Now().UnixNano() suffix; this
// test rapidly issues two requests and asserts both succeed with different
// names.
func TestRunUserOperation_NameIsUnique(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, userOpListKinds())

	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetContext(t.Context())
	addWaitFlags(cmd)
	require.NoError(t, cmd.Flags().Set("wait", "false"))

	req := userOpRequest{NamePrefix: "op-unlock-", OpType: "Unlock", User: "anton"}
	require.NoError(t, runUserOperation(cmd, dyn, req))
	require.NoError(t, runUserOperation(cmd, dyn, req))

	list, err := dyn.Resource(iamtypes.UserOperationGVR).List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.NotEqual(t, list.Items[0].GetName(), list.Items[1].GetName(),
		"two consecutive UserOperations must get distinct names")
}

// TestLockDef_RejectsBadDuration locks down the input validation done in
// the table-driven factory: lock requires a Go duration string for the
// second positional arg, and a typo must surface before any RPC.
func TestLockDef_RejectsBadDuration(t *testing.T) {
	var lockDef *userOpDef
	for i := range userOpDefs {
		if strings.HasPrefix(userOpDefs[i].Use, "lock") {
			lockDef = &userOpDefs[i]
			break
		}
	}
	require.NotNil(t, lockDef, "lock def must be registered")
	require.NotNil(t, lockDef.BuildExtraSpec)

	_, err := lockDef.BuildExtraSpec([]string{"anton", "not-a-duration"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid lockDuration")

	got, err := lockDef.BuildExtraSpec([]string{"anton", "30m"})
	require.NoError(t, err)
	lockSpec, ok := got["lock"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "30m", lockSpec["for"])
}
