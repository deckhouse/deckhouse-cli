package rules

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require Service ports to declare a named (non-numeric) target port so deployments can change container ports without breaking services.

// ServicePortRuleID is the stable identifier used to reference this rule in configuration.
const ServicePortRuleID = "service-port"

// ServicePortRule asserts every Service.spec.ports[*].targetPort is a named port, not a numeric one.
type ServicePortRule struct {
	collector *diag.Collector
	objects   []render.Object
}

// NewServicePortRule constructs a ServicePortRule scoped to its rule identifier.
func NewServicePortRule(objects []render.Object, collector *diag.Collector) *ServicePortRule {
	return &ServicePortRule{
		collector: collector.With(diag.RuleID(ServicePortRuleID)),
		objects:   objects,
	}
}

// Check verifies every Service port targets a named (non-numeric) port.
func (r *ServicePortRule) Check(_ context.Context) {
	for _, obj := range r.objects {
		if obj.GetKind() != "Service" {
			continue
		}

		baseID := obj.ObjectID()

		collector := r.collector.With(
			diag.ObjectID(baseID),
			diag.Path(obj.FilePath),
		)

		var service corev1.Service
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &service); err != nil {
			collector.Error("cannot convert object to Service: %v", err)
			continue
		}

		checkServicePorts(service, baseID, collector)
	}
}

// checkServicePorts emits diagnostics for service ports that use a numeric target port.
func checkServicePorts(service corev1.Service, baseID string, collector *diag.Collector) {
	for _, port := range service.Spec.Ports {
		if port.TargetPort.Type != intstr.Int {
			continue
		}

		portCollector := collector.With(
			diag.ObjectID(fmt.Sprintf("%s ; port = %s", baseID, port.Name)),
		)

		if port.TargetPort.IntVal == 0 {
			portCollector.Error("service port must use an explicit named (non-numeric) target port")
			continue
		}

		portCollector.With(diag.Value(port.TargetPort.IntVal)).
			Error("service port must use a named (non-numeric) target port")
	}
}
