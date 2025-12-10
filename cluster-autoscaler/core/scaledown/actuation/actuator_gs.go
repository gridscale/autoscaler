package actuation

import (
	"fmt"
	"sort"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/budgets"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"

	"k8s.io/klog/v2"
)

// NodeGroupWithNodes is a custom return type for the grouping done by groupNodesByNodeGroup.
type NodeGroupWithNodes struct {
	Group cloudprovider.NodeGroup
	All   []*apiv1.Node
	Empty []*apiv1.Node
	Drain []*apiv1.Node
}

// groupNodesByNodeGroup groups empty and drain nodes by their node group.
// If sortByNodeName is true, the nodes in each group will be sorted alphabetically by node name.
func (a *Actuator) groupNodesByNodeGroup(empty, drain, all []*apiv1.Node, sortByNodeName bool) (map[string]NodeGroupWithNodes, errors.AutoscalerError) {
	grouped := map[string]NodeGroupWithNodes{}
	for _, node := range empty {
		nodeGroup, err := a.ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			return nil, errors.NewAutoscalerErrorf(errors.CloudProviderError, "failed to find node group for %s: %v", node.Name, err)
		}
		// FIXME: Prevent nil panic on nil nodeGroup
		if _, ok := grouped[nodeGroup.Id()]; !ok {
			grouped[nodeGroup.Id()] = NodeGroupWithNodes{
				Group: nodeGroup,
				All:   []*apiv1.Node{},
				Empty: []*apiv1.Node{},
				Drain: []*apiv1.Node{},
			}
		}
		currentNodeGroupWithNodes := grouped[nodeGroup.Id()]
		currentNodeGroupWithNodes.Empty = append(currentNodeGroupWithNodes.Empty, node)
		grouped[nodeGroup.Id()] = currentNodeGroupWithNodes
	}

	for _, node := range drain {
		nodeGroup, err := a.ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			return nil, errors.NewAutoscalerErrorf(errors.CloudProviderError, "failed to find node group for %s: %v", node.Name, err)
		}
		// FIXME: Prevent nil panic on nil nodeGroup
		if _, ok := grouped[nodeGroup.Id()]; !ok {
			grouped[nodeGroup.Id()] = NodeGroupWithNodes{
				Group: nodeGroup,
				All:   []*apiv1.Node{},
				Empty: []*apiv1.Node{},
				Drain: []*apiv1.Node{},
			}
		}
		currentNodeGroupWithNodes := grouped[nodeGroup.Id()]
		currentNodeGroupWithNodes.Drain = append(currentNodeGroupWithNodes.Drain, node)
		grouped[nodeGroup.Id()] = currentNodeGroupWithNodes
	}

	for _, node := range all {
		nodeGroup, err := a.ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			return nil, errors.NewAutoscalerErrorf(errors.CloudProviderError, "failed to find node group for %s: %v", node.Name, err)
		}
		// FIXME: Prevent nil panic on nil nodeGroup
		if _, ok := grouped[nodeGroup.Id()]; !ok {
			grouped[nodeGroup.Id()] = NodeGroupWithNodes{
				Group: nodeGroup,
				All:   []*apiv1.Node{},
				Empty: []*apiv1.Node{},
				Drain: []*apiv1.Node{},
			}
		}
		currentNodeGroupWithNodes := grouped[nodeGroup.Id()]
		currentNodeGroupWithNodes.All = append(currentNodeGroupWithNodes.All, node)
		grouped[nodeGroup.Id()] = currentNodeGroupWithNodes
	}
	// if sortByNodeName is true, sort the nodes alphabetically by node name in each group
	if sortByNodeName {
		for _, nodeGroupWithNodes := range grouped {
			sort.Slice(nodeGroupWithNodes.Empty, func(i, j int) bool {
				iNameLower := strings.ToLower(nodeGroupWithNodes.Empty[i].Name)
				jNameLower := strings.ToLower(nodeGroupWithNodes.Empty[j].Name)
				return iNameLower < jNameLower
			})
			sort.Slice(nodeGroupWithNodes.Drain, func(i, j int) bool {
				iNameLower := strings.ToLower(nodeGroupWithNodes.Drain[i].Name)
				jNameLower := strings.ToLower(nodeGroupWithNodes.Drain[j].Name)
				return iNameLower < jNameLower
			})
			sort.Slice(nodeGroupWithNodes.All, func(i, j int) bool {
				iNameLower := strings.ToLower(nodeGroupWithNodes.All[i].Name)
				jNameLower := strings.ToLower(nodeGroupWithNodes.All[j].Name)
				return iNameLower < jNameLower
			})
		}
	}
	return grouped, nil
}

// StartDeletionForGridscaleProvider triggers a new deletion process for gridscale provider.
// *NOTE* gridscale provider does not support deletion of specific nodes. Gridscale provider only supports
// scale up/down by changing the number of nodes in the cluster. For the case of scale down, the last n nodes are
// deleted automatically by the provider. Therefore, we need to follow theses steps:
// 1. Count the number of nodes to be deleted (including to-be-deleted empty and to-be-deleted non-empty nodes).
// 2. Replace the to-be-deleted nodes with the last n nodes in the cluster.
// 3. Taint & drain the to-be-deleted nodes.
// 4. Delete the last n nodes in the cluster.
func (a *Actuator) StartDeletionForGridscaleProvider(empty, drain, all []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	a.nodeDeletionScheduler.ResetAndReportMetrics()
	deletionStartTime := time.Now()
	defer func() { metrics.UpdateDuration(metrics.ScaleDownNodeDeletion, time.Since(deletionStartTime)) }()

	klog.V(4).Infof("Deletion requested. Node counts: empty=%d drain=%d all=%d", len(empty), len(drain), len(all))
	klog.V(4).Info("[**]Empty nodes:")
	logNodes(empty)
	klog.V(4).Info("[**]Drain nodes:")
	logNodes(drain)
	klog.V(4).Info("[**]All nodes:")
	logNodes(all)

	if len(empty)+len(drain) >= len(all) {
		// If the number of nodes to be deleted is greater than or equal to the number of nodes in the cluster,
		// we cannot delete the nodes. Return an error.
		return status.ScaleDownError, nil, errors.NewAutoscalerError(
			errors.InternalError,
			"cannot delete nodes because the number of nodes to be deleted is greater than or equal to the number of nodes in the cluster. There has to be at least one node left in the cluster.",
		)
	}

	// Group the empty/drain nodes by node group.
	nodesToDeleteByNodeGroup, err := a.groupNodesByNodeGroup(empty, drain, all, true)
	if err != nil {
		return status.ScaleDownError, nil, err
	}

	var scaledDownNodes []*status.ScaleDownNode
	// Scale down nodes for each node group. One node group at a time.
	for nodeGroupID, nodeGroupWithNodes := range nodesToDeleteByNodeGroup {
		klog.V(4).Infof(" ------ Start scaling down nodes for node group %s", nodeGroupID)
		emptyToDeleteByGroup := []*apiv1.Node{}
		drainToDeleteByGroup := []*apiv1.Node{}
		emptyToDeleteNodeGroupViews, drainToDeleteNodeGroupViews := a.budgetProcessor.CropNodes(
			a.nodeDeletionTracker,
			nodeGroupWithNodes.Empty,
			nodeGroupWithNodes.Drain,
		)
		for _, bucket := range emptyToDeleteNodeGroupViews {
			emptyToDeleteByGroup = append(emptyToDeleteByGroup, bucket.Nodes...)
		}
		for _, bucket := range drainToDeleteNodeGroupViews {
			drainToDeleteByGroup = append(drainToDeleteByGroup, bucket.Nodes...)
		}
		if len(emptyToDeleteByGroup) == 0 && len(drainToDeleteByGroup) == 0 {
			klog.V(4).Infof(" ------ Aborting scaling down nodes for node group %s because no empty or drain nodes are present to be deleted", nodeGroupID)
			return status.ScaleDownNoNodeDeleted, nil, nil
		}

		klog.V(4).Infof("[**]Original empty nodes in node group %s (count: %d):", nodeGroupID, len(emptyToDeleteByGroup))
		logNodes(emptyToDeleteByGroup)
		klog.V(4).Infof("[**]Original drain nodes in node group %s (count: %d):", nodeGroupID, len(drainToDeleteByGroup))
		logNodes(drainToDeleteByGroup)

		// copy the all nodes (for safety).
		copiedAllByGroup := make([]*apiv1.Node, len(nodeGroupWithNodes.All))
		copy(copiedAllByGroup, nodeGroupWithNodes.All)
		// Replace the to-be-deleted nodes with the last n nodes in the group.
		var nodesToDeleteByGroup []*apiv1.Node
		nodesToDeleteCountByGroup := len(emptyToDeleteByGroup) + len(drainToDeleteByGroup)
		if nodesToDeleteCountByGroup > 0 {
			if nodesToDeleteCountByGroup > len(copiedAllByGroup) {
				return status.ScaleDownError, nil, errors.NewAutoscalerError(
					errors.InternalError,
					fmt.Sprintf("cannot delete nodes because the number of nodes to be deleted is greater than the total node count in the node group %s.", nodeGroupID),
				)
			}
			nodesToDeleteByGroup = copiedAllByGroup[len(copiedAllByGroup)-nodesToDeleteCountByGroup:]
		}
		klog.V(4).Info("[**]New empty nodes to delete: ", len(nodesToDeleteByGroup))
		logNodes(nodesToDeleteByGroup)

		// Clean taint from OLD to-be-deleted nodes
		oldToBeDeletedNodes := append(emptyToDeleteByGroup, drainToDeleteByGroup...)
		for _, node := range oldToBeDeletedNodes {
			if _, err := taints.CleanDeletionCandidate(node, a.ctx.ClientSet); err != nil {
				klog.Warningf("failed to clean taint DeletionCandidateTaint from node %s: %v", node.Name, err)
			}
			if _, err := taints.CleanToBeDeleted(node, a.ctx.ClientSet, a.ctx.CordonNodeBeforeTerminate); err != nil {
				klog.Warningf("failed to clean taint ToBeDeletedTaint from node %s: %v", node.Name, err)
			}
		}

		// do some sanity check
		if len(nodesToDeleteByGroup) <= 0 {
			return status.ScaleDownError, nil, errors.NewAutoscalerError(
				errors.InternalError,
				"cannot delete nodes because there is no node to be deleted.",
			)
		}
		for i, node := range nodesToDeleteByGroup {
			if node == nil {
				return status.ScaleDownError, nil, errors.NewAutoscalerError(
					errors.InternalError,
					fmt.Sprintf("cannot delete nodes because the node at index %d of to-be-deleted nodes is nil.", i),
				)
			}
		}

		nodesToDeleteNodeGroupViews := []*budgets.NodeGroupView{
			{
				Nodes: nodesToDeleteByGroup,
			},
		}

		// Taint all nodes that need drain synchronously, but don't start any drain/deletion yet. Otherwise, pods evicted from one to-be-deleted node
		// could get recreated on another.
		klog.V(4).Infof("Tainting to-be-deleted nodes for node group %s", nodeGroupID)
		_, err := a.taintNodesSync(nodesToDeleteNodeGroupViews)
		if err != nil {
			return status.ScaleDownError, nil, err
		}
		// Clean taint from NEW to-be-deleted nodes after scale down. We don't care about the error here.
		defer func() {
			klog.V(4).Infof("Cleaning taint from to-be-deleted nodes for node group %s", nodeGroupID)
			for _, node := range nodesToDeleteByGroup {
				taints.CleanToBeDeleted(node, a.ctx.ClientSet, a.ctx.CordonNodeBeforeTerminate)
			}
		}()
		klog.V(4).Infof("Finish tainting to-be-deleted nodes for node group %s", nodeGroupID)

		for _, drainNode := range nodesToDeleteByGroup {
			if sdNode, err := a.scaleDownNodeToReport(drainNode, true); err == nil {
				klog.V(0).Infof("Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", drainNode.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
				a.ctx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDown", "Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", drainNode.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
				scaledDownNodes = append(scaledDownNodes, sdNode)
			} else {
				klog.Errorf("Scale-down: couldn't report scaled down node, err: %v", err)
			}
		}

		klog.V(4).Infof("Draining to-be-deleted nodes for node group %s", nodeGroupID)
		// Drain to-be-deleted nodes synchronously.
		finishFuncList, cpErr := a.drainNodesSyncForGridscaleProvider(nodeGroupID, nodesToDeleteByGroup)
		if cpErr != nil {
			return status.ScaleDownError, nil, errors.NewAutoscalerErrorf(errors.CloudProviderError, "failed to drain nodes: %v", cpErr)
		}
		klog.V(4).Infof("Finish draining to-be-deleted nodes for node group %s", nodeGroupID)

		klog.V(4).Infof("Start scaling down nodes for node group %s", nodeGroupID)
		// Delete the last n nodes in the cluster.
		dErr := nodeGroupWithNodes.Group.DeleteNodes(nodesToDeleteByGroup)
		if dErr != nil {
			for _, finishFunc := range finishFuncList {
				finishFunc(status.NodeDeleteErrorFailedToDelete, dErr)
			}
			return status.ScaleDownError, nil, errors.NewAutoscalerErrorf(errors.CloudProviderError, "failed to delete nodes: %v", dErr)
		}
		for _, finishFunc := range finishFuncList {
			finishFunc(status.NodeDeleteOk, nil)
		}
		klog.V(4).Infof(" ------ Finish scaling down nodes for node group %s", nodeGroupID)
	}
	klog.V(4).Infof("Finish scaling down nodes")
	return status.ScaleDownNodeDeleteStarted, scaledDownNodes, nil
}

func (a *Actuator) drainNodesSyncForGridscaleProvider(nodeGroupID string, nodes []*apiv1.Node) ([]func(resultType status.NodeDeleteResultType, err error), errors.AutoscalerError) {
	var finishFuncList []func(resultType status.NodeDeleteResultType, err error)
	clusterSnapshot, err := a.createSnapshot(nodes)
	if err != nil {
		klog.Errorf("Scale-down: couldn't create delete snapshot, err: %v", err)
		nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "createSnapshot returned error %v", err)}
		for _, node := range nodes {
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroupID, true, "failed to create delete snapshot", nodeDeleteResult)
		}
		return nil, errors.NewAutoscalerErrorf(errors.InternalError, "couldn't create delete snapshot, err: %v", err)
	}
	for _, node := range nodes {
		nodeInfo, err := clusterSnapshot.GetNodeInfo(node.Name)
		if err != nil {
			klog.Errorf("Scale-down: can't retrieve node %q from snapshot, err: %v", node.Name, err)
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerErrorf(errors.InternalError, "nodeInfos.Get for %q returned error: %v", node.Name, err)}
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroupID, true, "failed to get node info", nodeDeleteResult)
			continue
		}
		a.nodeDeletionTracker.StartDeletionWithDrain(nodeGroupID, node.Name)
		evictionResults, err := a.nodeDeletionScheduler.evictor.DrainNode(a.ctx, nodeInfo)
		klog.V(4).Infof("Scale-down: drain results for node %s: %v", node.Name, evictionResults)
		if err != nil {
			a.nodeDeletionTracker.EndDeletion(nodeGroupID, node.Name, status.NodeDeleteResult{
				Err:                err,
				ResultType:         status.NodeDeleteErrorFailedToEvictPods,
				PodEvictionResults: evictionResults,
			})
			a.ctx.Recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to drain the node: %v", err)
			return nil, errors.NewAutoscalerErrorf(errors.ApiCallError, "couldn't drain node %q", node)
		}
		nodeName := node.Name
		finishFunc := func(resultType status.NodeDeleteResultType, err error) {
			result := status.NodeDeleteResult{
				Err:                err,
				ResultType:         resultType,
				PodEvictionResults: evictionResults,
			}
			a.nodeDeletionTracker.EndDeletion(nodeGroupID, nodeName, result)
		}
		finishFuncList = append(finishFuncList, finishFunc)
	}
	return finishFuncList, nil
}

func logNodes(nodes []*apiv1.Node) {
	for _, node := range nodes {
		klog.V(4).Infof("-\t%s\n", node.Name)
	}
}
