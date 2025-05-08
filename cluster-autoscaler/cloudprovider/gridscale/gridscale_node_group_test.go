package gridscale

import (
	"context"
	"errors"
	"fmt"
	"github.com/gridscale/gsclient-go/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type nodeGroupMockClient struct {
	GetPaaSServiceFunc    func(ctx context.Context, id string) (gsclient.PaaSService, error)
	UpdatePaaSServiceFunc func(ctx context.Context, id string, body gsclient.PaaSServiceUpdateRequest) error
	GetServerListFunc     func(ctx context.Context) ([]gsclient.Server, error)
}

var _ = nodeGroupClient(&nodeGroupMockClient{}) // Interface guard

func (n nodeGroupMockClient) GetPaaSService(ctx context.Context, id string) (gsclient.PaaSService, error) {
	if n.GetPaaSServiceFunc != nil {
		return n.GetPaaSServiceFunc(ctx, id)
	}
	panic("GetPaaSServiceFunc is not set")
}

func (n nodeGroupMockClient) UpdatePaaSService(ctx context.Context, id string, body gsclient.PaaSServiceUpdateRequest) error {
	if n.UpdatePaaSServiceFunc != nil {
		return n.UpdatePaaSServiceFunc(ctx, id, body)
	}
	panic("UpdatePaaSServiceFunc is not set")
}

func (n nodeGroupMockClient) GetServerList(ctx context.Context) ([]gsclient.Server, error) {
	if n.GetServerListFunc != nil {
		return n.GetServerListFunc(ctx)
	}
	panic("GetServerListFunc is not set")
}

func TestNodeGroup_Nodes(t *testing.T) {
	var client *nodeGroupMockClient
	var group *NodeGroup

	// clusterID is the ID if this node groups cluster.
	const clusterID = "42"
	// nodePoolName is the name of this node group (= node pool).
	const nodePoolName = "pool-dev"

	setup := func() {
		client = &nodeGroupMockClient{}
		group = &NodeGroup{
			client:      client,
			clusterUUID: clusterID,
			name:        nodePoolName,
			// TODO: Add other relevant fields
		}
	}

	t.Run("returns no nodes if api returns an empty server list", func(t *testing.T) {
		setup()

		client.GetServerListFunc = func(ctx context.Context) ([]gsclient.Server, error) {
			return make([]gsclient.Server, 0), nil
		}

		nodes, err := group.Nodes()
		require.NoError(t, err)
		require.Len(t, nodes, 0)
	})

	t.Run("returns error if client returns an error", func(t *testing.T) {
		setup()

		client.GetServerListFunc = func(ctx context.Context) ([]gsclient.Server, error) {
			return nil, errors.New("some unexpected error")
		}

		nodes, err := group.Nodes()
		assert.Error(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("returns no nodes if no server belongs to this cluster", func(t *testing.T) {
		setup()

		client.GetServerListFunc = func(ctx context.Context) ([]gsclient.Server, error) {
			return []gsclient.Server{
				// Server which belongs to another cluster
				{
					Properties: gsclient.ServerProperties{
						Name: "some-other-cluster-node-pool-dev-0",
						Labels: []string{
							"#gsk#12345",
						},
					},
				},
				// Server which does not even belong to any cluster
				{
					Properties: gsclient.ServerProperties{
						Name: "my-server",
						Labels: []string{
							"All your server are belong to us",
						},
					},
				},
			}, nil
		}

		nodes, err := group.Nodes()
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("returns nodes which belongs to this cluster and node pool", func(t *testing.T) {
		setup()

		client.GetServerListFunc = func(ctx context.Context) ([]gsclient.Server, error) {
			return []gsclient.Server{
				// Server which belongs to another cluster
				{
					Properties: gsclient.ServerProperties{
						Name: "some-other-cluster-node-pool-dev-0",
						Labels: []string{
							"#gsk#12345",
						},
					},
				},
				// Server which belongs to this cluster
				{
					Properties: gsclient.ServerProperties{
						Name: "my-cluster-node-pool-dev-0",
						Labels: []string{
							fmt.Sprintf("#gsk#%s", clusterID),
						},
					},
				},
				// Server which belongs to this cluster
				{
					Properties: gsclient.ServerProperties{
						Name: "my-cluster-node-pool-dev-1",
						Labels: []string{
							fmt.Sprintf("#gsk#%s", clusterID),
						},
					},
				},
				// Server which belongs to this cluster but to another node pool, should not be included!
				{
					Properties: gsclient.ServerProperties{
						Name: "my-cluster-node-pool-prod-1",
						Labels: []string{
							fmt.Sprintf("#gsk#%s", clusterID),
						},
					},
				},
			}, nil
		}

		nodes, err := group.Nodes()
		require.NoError(t, err)
		assert.Len(t, nodes, 2)
	})

	t.Run("returns nodes which belongs to this cluster and node pool even on weired node pool names", func(t *testing.T) {
		setup()

		// This test assumes we have 2 pools named "pool0" and "pool01", which could be used
		// by users.
		group.name = "pool0"

		client.GetServerListFunc = func(ctx context.Context) ([]gsclient.Server, error) {
			return []gsclient.Server{
				// Server which belongs to pool0
				{
					Properties: gsclient.ServerProperties{
						Name: "my-cluster-node-pool0-0",
						Labels: []string{
							fmt.Sprintf("#gsk#%s", clusterID),
						},
					},
				},
				// Server which belongs to pool1
				{
					Properties: gsclient.ServerProperties{
						Name: "my-cluster-node-pool01-0",
						Labels: []string{
							fmt.Sprintf("#gsk#%s", clusterID),
						},
					},
				},
			}, nil
		}

		nodes, err := group.Nodes()
		require.NoError(t, err)
		assert.Len(t, nodes, 1)
	})
}
