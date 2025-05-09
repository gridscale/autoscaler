/*
Copyright 2019 The Kubernetes Authors.

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

package gridscale

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/gridscale/gsclient-go/v3"
)

const (
	defaultGridscaleAPIURL        = "https://api.gridscale.io"
	defaultDelayIntervalMilliSecs = 5000
	defaultMaxNumberOfRetries     = 5
	defaultMinNodeCount           = 1

	gridscaleK8sActiveStatus = "active"
)

type nodeGroupClient interface {
	GetPaaSService(ctx context.Context, id string) (gsclient.PaaSService, error)

	UpdatePaaSService(ctx context.Context, id string, body gsclient.PaaSServiceUpdateRequest) error

	GetServerList(ctx context.Context) ([]gsclient.Server, error)
}

// Manager handles gridscale communication and data caching of
// node groups (node pools in DOKS)
type Manager struct {
	client       nodeGroupClient
	clusterUUID  string
	nodeGroups   []*NodeGroup
	maxNodeCount int
	minNodeCount int
}

func newManager() (*Manager, error) {
	gridscaleUUID := os.Getenv("API_UUID")
	if gridscaleUUID == "" {
		return nil, errors.New("env var API_UUID is not provided")
	}
	gridscaleToken := os.Getenv("API_TOKEN")
	if gridscaleToken == "" {
		return nil, errors.New("env var API_TOKEN is not provided")
	}
	gskClusterUUID := os.Getenv("CLUSTER_UUID")
	if gskClusterUUID == "" {
		return nil, errors.New("env var CLUSTER_UUID is not provided")
	}
	minNodeCount := defaultMinNodeCount
	minNodeCountStr := os.Getenv("CLUSTER_MIN_NODE_COUNT")
	if minNodeCountStr != "" {
		var err error
		// convert minNodeCount to int
		minNodeCount, err = strconv.Atoi(minNodeCountStr)
		if err != nil {
			return nil, fmt.Errorf("env var CLUSTER_MIN_NODE_COUNT is not a valid integer: %v", err)
		}
	}
	// min node count must be at least 1
	if minNodeCount < 1 {
		return nil, errors.New("env var CLUSTER_MIN_NODE_COUNT must be at least 1")
	}
	maxNodeCountStr := os.Getenv("CLUSTER_MAX_NODE_COUNT")
	if maxNodeCountStr == "" {
		return nil, errors.New("env var CLUSTER_MAX_NODE_COUNT is not provided")
	}
	// convert maxNodeCount to int
	maxNodeCount, err := strconv.Atoi(maxNodeCountStr)
	if err != nil {
		return nil, fmt.Errorf("env var CLUSTER_MAX_NODE_COUNT is not a valid integer: %v", err)
	}
	// max node count must be larger than min node count
	if maxNodeCount < minNodeCount {
		return nil, errors.New("env var CLUSTER_MAX_NODE_COUNT must be larger than CLUSTER_MIN_NODE_COUNT")
	}
	apiURL := defaultGridscaleAPIURL
	envVarApiURL := os.Getenv("API_API_URL")
	if envVarApiURL != "" {
		apiURL = envVarApiURL
	}
	gsConfig := gsclient.NewConfiguration(apiURL, gridscaleUUID, gridscaleToken, false, true, defaultDelayIntervalMilliSecs, defaultMaxNumberOfRetries)
	client := gsclient.NewClient(gsConfig)
	// check if gsk cluster exists
	_, err = client.GetPaaSService(context.Background(), gskClusterUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get gsk cluster: %v", err)
	}
	m := &Manager{
		client:       client,
		clusterUUID:  gskClusterUUID,
		nodeGroups:   make([]*NodeGroup, 0),
		maxNodeCount: maxNodeCount,
		minNodeCount: minNodeCount,
	}

	return m, nil
}

// Refresh refreshes the cache holding the nodegroups. This is called by the CA
// based on the `--scan-interval`. By default it's 10 seconds.
func (m *Manager) Refresh() error {
	ctx := context.Background()

	k8sCluster, err := m.client.GetPaaSService(ctx, m.clusterUUID)
	if err != nil {
		return err
	}
	// if k8s cluster's status is not active, return error
	if k8sCluster.Properties.Status != gridscaleK8sActiveStatus {
		return fmt.Errorf("k8s cluster status is not active: %s", k8sCluster.Properties.Status)
	}
	nodePools, ok := k8sCluster.Properties.Parameters["pools"].([]interface{})
	if !ok {
		return errors.New("'pools' is not found in cluster parameters")
	}
	nodeGroupList := make([]*NodeGroup, 0)
	for _, pool := range nodePools {
		nodePoolProperties, ok := pool.(map[string]interface{})
		if !ok {
			return errors.New("node pool properties is not a map")
		}
		nodePoolName, ok := nodePoolProperties["name"].(string)
		if !ok {
			return errors.New("'name' is not found in node pool properties")
		}
		nodePoolCount, ok := nodePoolProperties["count"].(float64)
		if !ok {
			return errors.New("'count' is not found in node pool properties")
		}
		nodeGroup := &NodeGroup{
			id:          fmt.Sprintf("%s-%s", m.clusterUUID, nodePoolName),
			name:        nodePoolName,
			clusterUUID: m.clusterUUID,
			client:      m.client,
			nodeCount:   int(nodePoolCount),
			minSize:     m.minNodeCount,
			maxSize:     m.maxNodeCount,
		}
		nodeGroupList = append(nodeGroupList, nodeGroup)
	}

	m.nodeGroups = nodeGroupList
	return nil
}
