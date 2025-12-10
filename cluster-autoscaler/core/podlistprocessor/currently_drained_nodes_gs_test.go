package podlistprocessor

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

var _ scaledown.Actuator = (*mockActuator)(nil)

func (m *mockActuator) StartDeletionForGridscaleProvider(_, _, _ []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	return m.StartDeletion(nil, nil)
}
