package mmdbwriter

import (
	"fmt"
	"math"
)

type nodes struct {
	nodes []node
	next  uint32
}

func newNodes(initialSize int) *nodes {
	return &nodes{
		nodes: make([]node, initialSize),
		next:  1,
	}
}

func (n *nodes) newNode() (*node, uint32) {
	if n.next >= uint32(len(n.nodes)) {
		newSize := len(n.nodes) * 2
		if newSize == 0 {
			newSize = 1024 * 1024
		}
		newNodes := make([]node, newSize)
		copy(newNodes, n.nodes)
		n.nodes = newNodes
	}
	num := n.next
	newNode := &n.nodes[num]
	n.next++

	// This is done to make bugs easier to track down. If we leave it at zero, it
	// tends to get confused with the root node
	newNode.children[0].node = math.MaxUint32
	newNode.children[1].node = math.MaxUint32

	return newNode, num
}

func (n *nodes) root() *node {
	return &n.nodes[0]
}

func (n *nodes) get(num uint32) (*node, error) {
	if num >= n.next {
		return nil, fmt.Errorf("requested node %d greater than or equal to max node %d", num, n.next)
	}
	return &n.nodes[int(num)], nil
}
