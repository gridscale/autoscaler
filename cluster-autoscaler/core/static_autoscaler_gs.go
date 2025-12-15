package core

import (
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/klog/v2"
)

func (a *StaticAutoscaler) cleanUpTaintsForAllNodes() {
	if readyNodes, err := a.ReadyNodeLister().List(); err != nil {
		klog.Errorf("Failed to list ready nodes, not cleaning up taints: %v", err)
	} else {
		taints.CleanAllToBeDeleted(readyNodes,
			a.AutoscalingContext.ClientSet, a.Recorder, a.CordonNodeBeforeTerminate)
		if a.AutoscalingContext.AutoscalingOptions.MaxBulkSoftTaintCount == 0 {
			// Clean old taints if soft taints handling is disabled
			taints.CleanAllDeletionCandidates(readyNodes,
				a.AutoscalingContext.ClientSet, a.Recorder)
		}
	}
	a.initialized = true
}
