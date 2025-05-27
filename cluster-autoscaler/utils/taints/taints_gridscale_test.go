package taints

import (
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func Test_skipTaintingGridscaleNode(t *testing.T) {
	type Test struct {
		Name        string
		NodeName    string
		ExpectsSkip bool
	}

	tests := []Test{
		{
			Name:        "skips node if it is the first node",
			NodeName:    "prod-my-pool-pool0-0",
			ExpectsSkip: true,
		},
		{
			Name:        "does not skip second node",
			NodeName:    "prod-my-pool-pool0-1",
			ExpectsSkip: false,
		},
		{
			Name:        "skips node with pre-node-pool name if it is the first node",
			NodeName:    "prod-node-pool0-0",
			ExpectsSkip: true,
		},
		{
			Name:        "does not skip node with pre-node-pool name if it is the second node",
			NodeName:    "prod-node-pool0-1",
			ExpectsSkip: false,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			node := &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: test.NodeName,
				},
			}

			reason, skip := skipTaintingGridscaleNode(node)

			if test.ExpectsSkip {
				assert.True(t, skip, "Node should be skipped")
				assert.NotEmpty(t, reason, "Skipping nodes must return a reason")
			} else {
				assert.False(t, skip, "Node should not be skipped")
				assert.Empty(t, reason, "Non-skipped nodes should not return a reason")
			}
		})
	}
}
