package useroperation

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func newLockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "lock <username> <lockDuration>",
		Short:         "Lock local user in Dex for a period of time",
		Args:          cobra.ExactArgs(2),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			username := args[0]
			lockDuration := args[1]
			// Validate duration format (must be parseable by time.ParseDuration; supports s/m/h).
			if _, err := time.ParseDuration(lockDuration); err != nil {
				return fmt.Errorf("invalid lockDuration %q: %w", lockDuration, err)
			}

			wf, err := getWaitFlags(cmd)
			if err != nil {
				return err
			}

			dyn, err := newDynamicClient(cmd)
			if err != nil {
				return err
			}

			name := fmt.Sprintf("op-lock-%d", time.Now().Unix())
			obj := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "deckhouse.io/v1",
					"kind":       "UserOperation",
					"metadata": map[string]any{
						"name": name,
					},
					"spec": map[string]any{
						"user":          username,
						"type":          "Lock",
						"initiatorType": "admin",
						"lock": map[string]any{
							"for": lockDuration,
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
				return fmt.Errorf("Lock failed: %s", message)
			}
			cmd.Printf("Succeeded: %s\n", name)
			return nil
		},
	}

	cmd.Long = "Lock local user in Dex for a period of time.\n\nThe lockDuration argument must be a duration string (e.g. 30s, 10m, 1h)."
	addWaitFlags(cmd)
	return cmd
}
