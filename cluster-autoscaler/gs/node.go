package gs

import (
	"strings"

	apiv1 "k8s.io/api/core/v1"
)

// IsFirstPoolNode wraps IsFirstPoolNodeName
func IsFirstPoolNode(node *apiv1.Node) bool {
	return IsFirstPoolNodeName(node.Name)
}

// IsFirstPoolNodeName returns true if the given node name indicates that this node
// is the first node in its node pool.
func IsFirstPoolNodeName(nodeName string) bool {
	return strings.HasSuffix(nodeName, "-0")
}

// MapWithoutZeroNode removes elements whose node is the first node of its pool.
func MapWithoutZeroNode[V any](in map[string]V) map[string]V {
	out := make(map[string]V)
	for k, v := range in {
		if IsFirstPoolNodeName(k) {
			continue
		}
		out[k] = v
	}
	return out
}

// SliceWithoutZeroNode removes elements whose node is the first node of its pool.
func SliceWithoutZeroNode(in []*apiv1.Node) []*apiv1.Node {
	out := make([]*apiv1.Node, 0, len(in))
	for _, node := range in {
		if IsFirstPoolNodeName(node.Name) {
			continue
		}
		out = append(out, node)
	}
	return out
}
