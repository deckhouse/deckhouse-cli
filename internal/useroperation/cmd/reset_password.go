package useroperation

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func newResetPasswordCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "reset-password <username> <bcryptHash>",
		Aliases:       []string{"resetpass"},
		Short:         "Reset local user's password in Dex (requires bcrypt hash)",
		Args:          cobra.ExactArgs(2),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			username := args[0]
			bcryptHash := args[1]
			wf, err := getWaitFlags(cmd)
			if err != nil {
				return err
			}

			dyn, err := newDynamicClient(cmd)
			if err != nil {
				return err
			}

			name := fmt.Sprintf("op-resetpass-%d", time.Now().Unix())
			obj := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "deckhouse.io/v1",
					"kind":       "UserOperation",
					"metadata": map[string]any{
						"name": name,
					},
					"spec": map[string]any{
						"user":          username,
						"type":          "ResetPassword",
						"initiatorType": "admin",
						"resetPassword": map[string]any{
							"newPasswordHash": bcryptHash,
						},
					},
				},
			}

			_, err = createUserOperation(cmd.Context(), dyn, obj)
			if err != nil {
				return fmt.Errorf("create UserOperation: %w", err)
			}

			if !wf.wait {
				cmd.Printf("%s\n", name)
				return nil
			}

			result, err := waitUserOperation(cmd.Context(), dyn, name, wf.timeout)
			if err != nil {
				return fmt.Errorf("wait UserOperation: %w", err)
			}

			phase, _, _ := unstructured.NestedString(result.Object, "status", "phase")
			message, _, _ := unstructured.NestedString(result.Object, "status", "message")
			if phase == "Failed" {
				return fmt.Errorf("ResetPassword failed: %s", message)
			}
			cmd.Printf("Succeeded: %s\n", name)
			return nil
		},
	}

	cmd.Long = "Reset local user's password in Dex.\n\nThe second argument must be a bcrypt hash (e.g. produced by `htpasswd -BinC 10`)."
	addWaitFlags(cmd)
	return cmd
}
