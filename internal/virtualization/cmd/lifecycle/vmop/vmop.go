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

package vmop

import (
	"context"
	"fmt"
	"strings"

	"github.com/deckhouse/virtualization/api/client/kubeclient"
	"github.com/deckhouse/virtualization/api/core/v1alpha2"
	"github.com/deckhouse/virtualization/api/core/v1alpha2/vmopcondition"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
)

type VirtualMachineOperation struct {
	client kubeclient.Client
}

func (v VirtualMachineOperation) Stop(ctx context.Context, vmName, vmNamespace string, wait, force bool) (msg string, err error) {
	vmop := v.newVMOP(vmName, vmNamespace, v1alpha2.VMOPTypeStop, force)
	return v.do(ctx, vmop, wait)
}

func (v VirtualMachineOperation) Start(ctx context.Context, vmName, vmNamespace string, wait bool) (msg string, err error) {
	vmop := v.newVMOP(vmName, vmNamespace, v1alpha2.VMOPTypeStart, false)
	return v.do(ctx, vmop, wait)
}

func (v VirtualMachineOperation) Restart(ctx context.Context, vmName, vmNamespace string, wait, force bool) (msg string, err error) {
	vmop := v.newVMOP(vmName, vmNamespace, v1alpha2.VMOPTypeRestart, force)
	return v.do(ctx, vmop, wait)
}

func (v VirtualMachineOperation) Migrate(ctx context.Context, vmName, vmNamespace string, wait bool) (msg string, err error) {
	vmop := v.newVMOP(vmName, vmNamespace, v1alpha2.VMOPTypeMigrate, false)
	return v.do(ctx, vmop, wait)
}

func (v VirtualMachineOperation) do(ctx context.Context, vmop *v1alpha2.VirtualMachineOperation, wait bool) (msg string, err error) {
	if wait {
		vmop, err = v.createAndWait(ctx, vmop)
	} else {
		vmop, err = v.create(ctx, vmop)
	}
	msg = v.generateMsg(vmop)
	return msg, err
}

func (v VirtualMachineOperation) generateMsg(vmop *v1alpha2.VirtualMachineOperation) string {
	if vmop == nil {
		return ""
	}
	key := types.NamespacedName{Namespace: vmop.GetNamespace(), Name: vmop.GetName()}
	vmKey := types.NamespacedName{Namespace: vmop.GetNamespace(), Name: vmop.Spec.VirtualMachine}
	phase := vmop.Status.Phase

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("VirtualMachine %q ", vmKey.String()))

	if v.isFinished(vmop) {
		if !v.isCompleted(vmop) {
			sb.WriteString("was not ")
		}
		switch vmop.Spec.Type {
		case v1alpha2.VMOPTypeStart:
			sb.WriteString("started. ")
		case v1alpha2.VMOPTypeStop:
			sb.WriteString("stopped. ")
		case v1alpha2.VMOPTypeRestart:
			sb.WriteString("restarted. ")
		case v1alpha2.VMOPTypeMigrate:
			sb.WriteString("migrated.")
		}
	} else {
		switch vmop.Spec.Type {
		case v1alpha2.VMOPTypeStart:
			sb.WriteString("starting. ")
		case v1alpha2.VMOPTypeStop:
			sb.WriteString("stopping. ")
		case v1alpha2.VMOPTypeRestart:
			sb.WriteString("restarting. ")
		case v1alpha2.VMOPTypeMigrate:
			sb.WriteString("migrating.")
		}
	}

	sb.WriteString(fmt.Sprintf("VirtualMachineOperation %q ", key.String()))
	switch phase {
	case v1alpha2.VMOPPhasePending:
		sb.WriteString("pending.")
	case v1alpha2.VMOPPhaseInProgress:
		sb.WriteString("in progress.")
	case v1alpha2.VMOPPhaseCompleted:
		sb.WriteString("completed.")
	case v1alpha2.VMOPPhaseFailed:
		cond, _ := getCondition(vmopcondition.TypeCompleted.String(), vmop.Status.Conditions)
		sb.WriteString(fmt.Sprintf("failed. reason=%q, message=%q.", cond.Reason, cond.Message))
	default:
		sb.WriteString(fmt.Sprintf(" phase=%q.", phase))
	}
	sb.WriteString("\n")
	return sb.String()
}

func (v VirtualMachineOperation) createAndWait(ctx context.Context, vmop *v1alpha2.VirtualMachineOperation) (*v1alpha2.VirtualMachineOperation, error) {
	vmop, err := v.create(ctx, vmop)
	if err != nil {
		return nil, err
	}
	if v.isFinished(vmop) {
		return vmop, nil
	}
	return v.wait(ctx, vmop.GetName(), vmop.GetNamespace())
}

func (v VirtualMachineOperation) create(ctx context.Context, vmop *v1alpha2.VirtualMachineOperation) (*v1alpha2.VirtualMachineOperation, error) {
	return v.client.VirtualMachineOperations(vmop.GetNamespace()).Create(ctx, vmop, metav1.CreateOptions{})
}

func (v VirtualMachineOperation) wait(ctx context.Context, name, namespace string) (*v1alpha2.VirtualMachineOperation, error) {
	var vmop *v1alpha2.VirtualMachineOperation
	selector, err := fields.ParseSelector(fmt.Sprintf("metadata.name=%s", name))
	if err != nil {
		return nil, err
	}
	watcher, err := v.client.VirtualMachineOperations(namespace).Watch(ctx, metav1.ListOptions{FieldSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()
	for event := range watcher.ResultChan() {
		op, ok := event.Object.(*v1alpha2.VirtualMachineOperation)
		if !ok {
			continue
		}
		if v.isFinished(op) {
			vmop = op
			break
		}
	}
	if !v.isFinished(vmop) {
		return nil, context.DeadlineExceeded
	}
	return vmop, nil
}

func (v VirtualMachineOperation) isCompleted(vmop *v1alpha2.VirtualMachineOperation) bool {
	if vmop == nil {
		return false
	}
	return vmop.Status.Phase == v1alpha2.VMOPPhaseCompleted
}

func (v VirtualMachineOperation) isFinished(vmop *v1alpha2.VirtualMachineOperation) bool {
	if vmop == nil {
		return false
	}
	return vmop.Status.Phase == v1alpha2.VMOPPhaseCompleted || vmop.Status.Phase == v1alpha2.VMOPPhaseFailed
}

func (v VirtualMachineOperation) newVMOP(vmName, vmNamespace string, t v1alpha2.VMOPType, force bool) *v1alpha2.VirtualMachineOperation {
	return &v1alpha2.VirtualMachineOperation{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha2.VMOPKind,
			APIVersion: v1alpha2.Version,
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: vmName + "-",
			Namespace:    vmNamespace,
		},
		Spec: v1alpha2.VirtualMachineOperationSpec{
			Type:           t,
			VirtualMachine: vmName,
			Force:          force,
		},
	}
}

func New(client kubeclient.Client) *VirtualMachineOperation {
	return &VirtualMachineOperation{
		client: client,
	}
}

func getCondition(condType string, conds []metav1.Condition) (metav1.Condition, bool) {
	for _, cond := range conds {
		if cond.Type == condType {
			return cond, true
		}
	}

	return metav1.Condition{}, false
}
