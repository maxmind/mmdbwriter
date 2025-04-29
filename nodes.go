package mmdbwriter

import (
	"errors"
	"fmt"
)

type nodes struct {
	nodes    []node
	returned []uint32
	next     uint32
}

func newNodes(initialSize int) *nodes {
	n := make([]node, initialSize)

	root := &n[0]
	root.children[0].node = nonExistentNode
	root.children[0].recordType = recordTypeEmpty
	root.children[1].node = nonExistentNode
	root.children[1].recordType = recordTypeEmpty

	return &nodes{
		nodes: n,
		next:  1,
	}
}

func (n *nodes) acquireNode() (*node, uint32) {
	if len(n.returned) != 0 {
		last := len(n.returned) - 1
		num := n.returned[last]
		node := &n.nodes[num]
		n.returned = n.returned[:last]

		return node, num
	}
	if n.next >= uint32(len(n.nodes)) {
		newSize := len(n.nodes) * 2
		newNodes := make([]node, newSize)
		copy(newNodes, n.nodes)
		n.nodes = newNodes
	}
	num := n.next
	newNode := &n.nodes[num]
	n.next++

	newNode.reset()

	return newNode, num
}

func (n *nodes) returnNode(num uint32) error {
	if num == 0 {
		return errors.New("the root node cannot be returned")
	}
	if num >= n.next {
		return fmt.Errorf("returning node %d greater than or equal to max node %d", num, n.next)
	}

	returnedNode := &n.nodes[num]

	if returnedNode.children[0].recordType == recordTypeFixedNode || returnedNode.children[1].recordType == recordTypeFixedNode {
		return errors.New("node with fixed record type for children cannot be returned")
	}

	returnedNode.reset()

	n.returned = append(n.returned, num)

	return nil
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
