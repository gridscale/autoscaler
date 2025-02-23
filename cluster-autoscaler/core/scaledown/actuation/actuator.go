/*
Copyright 2022 The Kubernetes Authors.

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

package actuation

import (
	"fmt"
	"sort"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/budgets"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/deletiontracker"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/core/utils"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/utilization"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/klog/v2"
)

// Actuator is responsible for draining and deleting nodes.
type Actuator struct {
	ctx                   *context.AutoscalingContext
	nodeDeletionTracker   *deletiontracker.NodeDeletionTracker
	nodeDeletionScheduler *GroupDeletionScheduler
	deleteOptions         options.NodeDeleteOptions
	drainabilityRules     rules.Rules
	// TODO: Move budget processor to scaledown planner, potentially merge into PostFilteringScaleDownNodeProcessor
	// This is a larger change to the code structure which impacts some existing actuator unit tests
	// as well as Cluster Autoscaler implementations that may override ScaleDownSetProcessor
	budgetProcessor           *budgets.ScaleDownBudgetProcessor
	configGetter              actuatorNodeGroupConfigGetter
	nodeDeleteDelayAfterTaint time.Duration
}

// actuatorNodeGroupConfigGetter is an interface to limit the functions that can be used
// from NodeGroupConfigProcessor interface
type actuatorNodeGroupConfigGetter interface {
	// GetIgnoreDaemonSetsUtilization returns IgnoreDaemonSetsUtilization value that should be used for a given NodeGroup.
	GetIgnoreDaemonSetsUtilization(nodeGroup cloudprovider.NodeGroup) (bool, error)
}

type NodeGroupWithNodes struct {
	Group cloudprovider.NodeGroup
	All   []*apiv1.Node
	Empty []*apiv1.Node
	Drain []*apiv1.Node
}

// NewActuator returns a new instance of Actuator.
func NewActuator(ctx *context.AutoscalingContext, scaleStateNotifier nodegroupchange.NodeGroupChangeObserver, ndt *deletiontracker.NodeDeletionTracker, deleteOptions options.NodeDeleteOptions, drainabilityRules rules.Rules, configGetter actuatorNodeGroupConfigGetter) *Actuator {
	ndb := NewNodeDeletionBatcher(ctx, scaleStateNotifier, ndt, ctx.NodeDeletionBatcherInterval)
	legacyFlagDrainConfig := SingleRuleDrainConfig(ctx.MaxGracefulTerminationSec)
	var evictor Evictor
	if len(ctx.DrainPriorityConfig) > 0 {
		evictor = NewEvictor(ndt, ctx.DrainPriorityConfig, true)
	} else {
		evictor = NewEvictor(ndt, legacyFlagDrainConfig, false)
	}
	return &Actuator{
		ctx:                       ctx,
		nodeDeletionTracker:       ndt,
		nodeDeletionScheduler:     NewGroupDeletionScheduler(ctx, ndt, ndb, evictor),
		budgetProcessor:           budgets.NewScaleDownBudgetProcessor(ctx),
		deleteOptions:             deleteOptions,
		drainabilityRules:         drainabilityRules,
		configGetter:              configGetter,
		nodeDeleteDelayAfterTaint: ctx.NodeDeleteDelayAfterTaint,
	}
}

// CheckStatus should returns an immutable snapshot of ongoing deletions.
func (a *Actuator) CheckStatus() scaledown.ActuationStatus {
	return a.nodeDeletionTracker.Snapshot()
}

// ClearResultsNotNewerThan removes information about deletions finished before or exactly at the provided timestamp.
func (a *Actuator) ClearResultsNotNewerThan(t time.Time) {
	a.nodeDeletionTracker.ClearResultsNotNewerThan(t)
}

// DeletionResults returns deletion results since the last ClearResultsNotNewerThan call
// in a map form, along with the timestamp of last result.
func (a *Actuator) DeletionResults() (map[string]status.NodeDeleteResult, time.Time) {
	return a.nodeDeletionTracker.DeletionResults()
}

// StartDeletion triggers a new deletion process.
func (a *Actuator) StartDeletion(empty, drain []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	a.nodeDeletionScheduler.ResetAndReportMetrics()
	deletionStartTime := time.Now()
	defer func() { metrics.UpdateDuration(metrics.ScaleDownNodeDeletion, time.Since(deletionStartTime)) }()

	scaledDownNodes := make([]*status.ScaleDownNode, 0)
	emptyToDelete, drainToDelete := a.budgetProcessor.CropNodes(a.nodeDeletionTracker, empty, drain)
	if len(emptyToDelete) == 0 && len(drainToDelete) == 0 {
		return status.ScaleDownNoNodeDeleted, nil, nil
	}

	if len(emptyToDelete) > 0 {
		// Taint all empty nodes synchronously
		if err := a.taintNodesSync(emptyToDelete); err != nil {
			return status.ScaleDownError, scaledDownNodes, err
		}

		emptyScaledDown := a.deleteAsyncEmpty(emptyToDelete)
		scaledDownNodes = append(scaledDownNodes, emptyScaledDown...)
	}

	if len(drainToDelete) > 0 {
		// Taint all nodes that need drain synchronously, but don't start any drain/deletion yet. Otherwise, pods evicted from one to-be-deleted node
		// could get recreated on another.
		if err := a.taintNodesSync(drainToDelete); err != nil {
			return status.ScaleDownError, scaledDownNodes, err
		}

		// All nodes involved in the scale-down should be tainted now - start draining and deleting nodes asynchronously.
		drainScaledDown := a.deleteAsyncDrain(drainToDelete)
		scaledDownNodes = append(scaledDownNodes, drainScaledDown...)
	}

	return status.ScaleDownNodeDeleteStarted, scaledDownNodes, nil
}

// groupNodesByNodeGroup groups empty and drain nodes by their node group.
// If sortByNodeName is true, the nodes in each group will be sorted alphabetically by node name.
func (a *Actuator) groupNodesByNodeGroup(empty, drain, all []*apiv1.Node, sortByNodeName bool) (map[string]NodeGroupWithNodes, errors.AutoscalerError) {
	grouped := map[string]NodeGroupWithNodes{}
	for _, node := range empty {
		nodeGroup, err := a.ctx.CloudProvider.NodeGroupForNode(node)
		if err != nil {
			return nil, errors.NewAutoscalerError(errors.CloudProviderError, "failed to find node group for %s: %v", node.Name, err)
		}
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
			return nil, errors.NewAutoscalerError(errors.CloudProviderError, "failed to find node group for %s: %v", node.Name, err)
		}
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
			return nil, errors.NewAutoscalerError(errors.CloudProviderError, "failed to find node group for %s: %v", node.Name, err)
		}
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

// NOTE: I drain the wrong nodes.
func (a *Actuator) StartDeletionForGridscaleProvider(empty, drain, all []*apiv1.Node) (status.ScaleDownResult, []*status.ScaleDownNode, errors.AutoscalerError) {
	a.nodeDeletionScheduler.ResetAndReportMetrics()
	deletionStartTime := time.Now()
	defer func() { metrics.UpdateDuration(metrics.ScaleDownNodeDeletion, time.Since(deletionStartTime)) }()
	if len(empty)+len(drain) >= len(all) {
		// If the number of nodes to be deleted is greater than or equal to the number of nodes in the cluster,
		// we cannot delete the nodes. Return an error.
		return status.ScaleDownError, nil, errors.NewAutoscalerError(
			errors.InternalError,
			"cannot delete nodes because the number of nodes to be deleted is greater than or equal to the number of nodes in the cluster. There has to be at least one node left in the cluster.",
		)
	}

	// Group the emtpy/drain nodes by node group.
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
			return status.ScaleDownNoNodeDeleted, nil, nil
		}

		klog.V(4).Infof("[**]Original empty nodes in node group %s (count: %d):", nodeGroupID, len(emptyToDeleteByGroup))
		for _, node := range emptyToDeleteByGroup {
			klog.V(4).Infof("\t-\t%s\n", node.Name)
		}
		klog.V(4).Infof("[**]Original drain nodes in node group %s (count: %d):", nodeGroupID, len(drainToDeleteByGroup))
		for _, node := range drainToDeleteByGroup {
			klog.V(4).Infof("\t-\t%s\n", node.Name)
		}

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
		for _, node := range nodesToDeleteByGroup {
			klog.V(4).Infof("\t-\t%s\n", node.Name)
		}

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
		err := a.taintNodesSync(nodesToDeleteNodeGroupViews)
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
			return status.ScaleDownError, nil, errors.NewAutoscalerError(errors.CloudProviderError, "failed to drain nodes: %v", cpErr)
		}
		klog.V(4).Infof("Finish draining to-be-deleted nodes for node group %s", nodeGroupID)

		klog.V(4).Infof("Start scaling down nodes for node group %s", nodeGroupID)
		// Delete the last n nodes in the cluster.
		dErr := nodeGroupWithNodes.Group.DeleteNodes(nodesToDeleteByGroup)
		if dErr != nil {
			for _, finishFunc := range finishFuncList {
				finishFunc(status.NodeDeleteErrorFailedToDelete, dErr)
			}
			return status.ScaleDownError, nil, errors.NewAutoscalerError(errors.CloudProviderError, "failed to delete nodes: %v", dErr)
		}
		for _, finishFunc := range finishFuncList {
			finishFunc(status.NodeDeleteOk, nil)
		}
		klog.V(4).Infof(" ------ Finish scaling down nodes for node group %s", nodeGroupID)
	}
	klog.V(4).Infof("Finish scaling down nodes")
	return status.ScaleDownNodeDeleteStarted, scaledDownNodes, nil
}

// deleteAsyncEmpty immediately starts deletions asynchronously.
// scaledDownNodes return value contains all nodes for which deletion successfully started.
func (a *Actuator) deleteAsyncEmpty(NodeGroupViews []*budgets.NodeGroupView) (reportedSDNodes []*status.ScaleDownNode) {
	for _, bucket := range NodeGroupViews {
		for _, node := range bucket.Nodes {
			klog.V(0).Infof("Scale-down: removing empty node %q", node.Name)
			a.ctx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDownEmpty", "Scale-down: removing empty node %q", node.Name)

			if sdNode, err := a.scaleDownNodeToReport(node, false); err == nil {
				reportedSDNodes = append(reportedSDNodes, sdNode)
			} else {
				klog.Errorf("Scale-down: couldn't report scaled down node, err: %v", err)
			}

			a.nodeDeletionTracker.StartDeletion(bucket.Group.Id(), node.Name)
		}
	}

	for _, bucket := range NodeGroupViews {
		go a.deleteNodesAsync(bucket.Nodes, bucket.Group, false, bucket.BatchSize)
	}

	return reportedSDNodes
}

// taintNodesSync synchronously taints all provided nodes with NoSchedule. If tainting fails for any of the nodes, already
// applied taints are cleaned up.
func (a *Actuator) taintNodesSync(NodeGroupViews []*budgets.NodeGroupView) errors.AutoscalerError {
	var taintedNodes []*apiv1.Node
	var updateLatencyTracker *UpdateLatencyTracker
	if a.ctx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
		updateLatencyTracker = NewUpdateLatencyTracker(a.ctx.AutoscalingKubeClients.ListerRegistry.AllNodeLister())
		go updateLatencyTracker.Start()
	}
	for _, bucket := range NodeGroupViews {
		for _, node := range bucket.Nodes {
			if a.ctx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
				updateLatencyTracker.StartTimeChan <- nodeTaintStartTime{node.Name, time.Now()}
			}
			err := a.taintNode(node)
			if err != nil {
				a.ctx.Recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to mark the node as toBeDeleted/unschedulable: %v", err)
				// Clean up already applied taints in case of issues.
				for _, taintedNode := range taintedNodes {
					_, _ = taints.CleanToBeDeleted(taintedNode, a.ctx.ClientSet, a.ctx.CordonNodeBeforeTerminate)
				}
				if a.ctx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
					close(updateLatencyTracker.AwaitOrStopChan)
				}
				return errors.NewAutoscalerError(errors.ApiCallError, "couldn't taint node %q with ToBeDeleted", node)
			}
			taintedNodes = append(taintedNodes, node)
		}
	}
	if a.ctx.AutoscalingOptions.DynamicNodeDeleteDelayAfterTaintEnabled {
		updateLatencyTracker.AwaitOrStopChan <- true
		latency, ok := <-updateLatencyTracker.ResultChan
		if ok {
			// CA is expected to wait 3 times the round-trip time between CA and the api-server.
			// Therefore, the nodeDeleteDelayAfterTaint is set 2 times the latency.
			// A delay of one round trip time is implicitly there when measuring the latency.
			a.nodeDeleteDelayAfterTaint = 2 * latency
		}
	}
	return nil
}

func (a *Actuator) drainNodesSyncForGridscaleProvider(nodeGroupID string, nodes []*apiv1.Node) ([]func(resultType status.NodeDeleteResultType, err error), errors.AutoscalerError) {
	var finishFuncList []func(resultType status.NodeDeleteResultType, err error)
	clusterSnapshot, err := a.createSnapshot(nodes)
	if err != nil {
		klog.Errorf("Scale-down: couldn't create delete snapshot, err: %v", err)
		nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "createSnapshot returned error %v", err)}
		for _, node := range nodes {
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroupID, true, "failed to create delete snapshot", nodeDeleteResult)
		}
		return nil, errors.NewAutoscalerError(errors.InternalError, "couldn't create delete snapshot, err: %v", err)
	}
	for _, node := range nodes {
		nodeInfo, err := clusterSnapshot.NodeInfos().Get(node.Name)
		if err != nil {
			klog.Errorf("Scale-down: can't retrieve node %q from snapshot, err: %v", node.Name, err)
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "nodeInfos.Get for %q returned error: %v", node.Name, err)}
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
			return nil, errors.NewAutoscalerError(errors.ApiCallError, "couldn't drain node %q", node)
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

// deleteAsyncDrain asynchronously starts deletions with drain for all provided nodes. scaledDownNodes return value contains all nodes for which
// deletion successfully started.
func (a *Actuator) deleteAsyncDrain(NodeGroupViews []*budgets.NodeGroupView) (reportedSDNodes []*status.ScaleDownNode) {
	for _, bucket := range NodeGroupViews {
		for _, drainNode := range bucket.Nodes {
			if sdNode, err := a.scaleDownNodeToReport(drainNode, true); err == nil {
				klog.V(0).Infof("Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", drainNode.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
				a.ctx.LogRecorder.Eventf(apiv1.EventTypeNormal, "ScaleDown", "Scale-down: removing node %s, utilization: %v, pods to reschedule: %s", drainNode.Name, sdNode.UtilInfo, joinPodNames(sdNode.EvictedPods))
				reportedSDNodes = append(reportedSDNodes, sdNode)
			} else {
				klog.Errorf("Scale-down: couldn't report scaled down node, err: %v", err)
			}

			a.nodeDeletionTracker.StartDeletionWithDrain(bucket.Group.Id(), drainNode.Name)
		}
	}

	for _, bucket := range NodeGroupViews {
		go a.deleteNodesAsync(bucket.Nodes, bucket.Group, true, bucket.BatchSize)
	}

	return reportedSDNodes
}

func (a *Actuator) deleteNodesAsync(nodes []*apiv1.Node, nodeGroup cloudprovider.NodeGroup, drain bool, batchSize int) {
	var remainingPdbTracker pdb.RemainingPdbTracker
	var registry kube_util.ListerRegistry

	if len(nodes) == 0 {
		return
	}

	if a.nodeDeleteDelayAfterTaint > time.Duration(0) {
		klog.V(0).Infof("Scale-down: waiting %v before trying to delete nodes", a.nodeDeleteDelayAfterTaint)
		time.Sleep(a.nodeDeleteDelayAfterTaint)
	}

	clusterSnapshot, err := a.createSnapshot(nodes)
	if err != nil {
		klog.Errorf("Scale-down: couldn't create delete snapshot, err: %v", err)
		nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "createSnapshot returned error %v", err)}
		for _, node := range nodes {
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroup.Id(), drain, "failed to create delete snapshot", nodeDeleteResult)
		}
		return
	}

	if drain {
		pdbs, err := a.ctx.PodDisruptionBudgetLister().List()
		if err != nil {
			klog.Errorf("Scale-down: couldn't fetch pod disruption budgets, err: %v", err)
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "podDisruptionBudgetLister.List returned error %v", err)}
			for _, node := range nodes {
				a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroup.Id(), drain, "failed to fetch pod disruption budgets", nodeDeleteResult)
			}
			return
		}
		remainingPdbTracker = pdb.NewBasicRemainingPdbTracker()
		remainingPdbTracker.SetPdbs(pdbs)
		registry = a.ctx.ListerRegistry
	}

	if batchSize == 0 {
		batchSize = len(nodes)
	}

	for _, node := range nodes {
		nodeInfo, err := clusterSnapshot.NodeInfos().Get(node.Name)
		if err != nil {
			klog.Errorf("Scale-down: can't retrieve node %q from snapshot, err: %v", node.Name, err)
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "nodeInfos.Get for %q returned error: %v", node.Name, err)}
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroup.Id(), drain, "failed to get node info", nodeDeleteResult)
			continue
		}

		podsToRemove, _, _, err := simulator.GetPodsToMove(nodeInfo, a.deleteOptions, a.drainabilityRules, registry, remainingPdbTracker, time.Now())
		if err != nil {
			klog.Errorf("Scale-down: couldn't delete node %q, err: %v", node.Name, err)
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "GetPodsToMove for %q returned error: %v", node.Name, err)}
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroup.Id(), drain, "failed to get pods to move on node", nodeDeleteResult)
			continue
		}

		if !drain && len(podsToRemove) != 0 {
			klog.Errorf("Scale-down: couldn't delete empty node %q, new pods got scheduled", node.Name)
			nodeDeleteResult := status.NodeDeleteResult{ResultType: status.NodeDeleteErrorInternal, Err: errors.NewAutoscalerError(errors.InternalError, "failed to delete empty node %q, new pods scheduled", node.Name)}
			a.nodeDeletionScheduler.AbortNodeDeletion(node, nodeGroup.Id(), drain, "node is not empty", nodeDeleteResult)
			continue
		}

		go a.nodeDeletionScheduler.ScheduleDeletion(nodeInfo, nodeGroup, batchSize, drain)
	}
}

func (a *Actuator) scaleDownNodeToReport(node *apiv1.Node, drain bool) (*status.ScaleDownNode, error) {
	nodeGroup, err := a.ctx.CloudProvider.NodeGroupForNode(node)
	if err != nil {
		return nil, err
	}
	nodeInfo, err := a.ctx.ClusterSnapshot.NodeInfos().Get(node.Name)
	if err != nil {
		return nil, err
	}

	ignoreDaemonSetsUtilization, err := a.configGetter.GetIgnoreDaemonSetsUtilization(nodeGroup)
	if err != nil {
		return nil, err
	}

	gpuConfig := a.ctx.CloudProvider.GetNodeGpuConfig(node)
	utilInfo, err := utilization.Calculate(nodeInfo, ignoreDaemonSetsUtilization, a.ctx.IgnoreMirrorPodsUtilization, gpuConfig, time.Now())
	if err != nil {
		return nil, err
	}
	var evictedPods []*apiv1.Pod
	if drain {
		_, nonDsPodsToEvict := podsToEvict(nodeInfo, a.ctx.DaemonSetEvictionForOccupiedNodes)
		evictedPods = nonDsPodsToEvict
	}
	return &status.ScaleDownNode{
		Node:        node,
		NodeGroup:   nodeGroup,
		EvictedPods: evictedPods,
		UtilInfo:    utilInfo,
	}, nil
}

// taintNode taints the node with NoSchedule to prevent new pods scheduling on it.
func (a *Actuator) taintNode(node *apiv1.Node) error {
	if err := taints.MarkToBeDeleted(node, a.ctx.ClientSet, a.ctx.CordonNodeBeforeTerminate); err != nil {
		a.ctx.Recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to mark the node as toBeDeleted/unschedulable: %v", err)
		return errors.ToAutoscalerError(errors.ApiCallError, err)
	}
	a.ctx.Recorder.Eventf(node, apiv1.EventTypeNormal, "ScaleDown", "marked the node as toBeDeleted/unschedulable")
	return nil
}

func (a *Actuator) createSnapshot(nodes []*apiv1.Node) (clustersnapshot.ClusterSnapshot, error) {
	knownNodes := make(map[string]bool)
	snapshot := clustersnapshot.NewBasicClusterSnapshot()
	pods, err := a.ctx.AllPodLister().List()
	if err != nil {
		return nil, err
	}

	scheduledPods := kube_util.ScheduledPods(pods)
	nonExpendableScheduledPods := utils.FilterOutExpendablePods(scheduledPods, a.ctx.ExpendablePodsPriorityCutoff)

	for _, node := range nodes {
		if err := snapshot.AddNode(node); err != nil {
			return nil, err
		}

		knownNodes[node.Name] = true
	}

	for _, pod := range nonExpendableScheduledPods {
		if knownNodes[pod.Spec.NodeName] {
			if err := snapshot.AddPod(pod, pod.Spec.NodeName); err != nil {
				return nil, err
			}
		}
	}

	return snapshot, nil
}

func joinPodNames(pods []*apiv1.Pod) string {
	var names []string
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return strings.Join(names, ",")
}
