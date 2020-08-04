package mmdbwriter

import (
	"net"
)

// node represents a node of any type. Only leaf nodes should have a value
// associated with it. Although this potentially could be clearer and more
// compact as an interface with different concrete types, the compiler has
// a harder time optimizing such code and the benefit doesn't seem that
// great.
type node struct {
	children [2]*node
	// not sure what this is yet
	value   *dataType
	nodeNum int
}

func (n *node) insert(
	ip net.IP,
	prefixLen int,
	depth int,
	value dataType,
) {
	if depth == prefixLen {
		n.value = &value
		n.children = [2]*node{}
		return
	}

	pos := bitAt(ip, depth)
	child := n.children[pos]

	if child == nil {
		// We create both children with the value of the parent as we are
		// splitting this node.
		child = &node{value: n.value}
		n.children[pos] = child

		// the other child
		n.children[1-pos] = &node{value: n.value}
	}

	// This may have been a node with a value.
	n.value = nil
	child.insert(ip, prefixLen, depth+1, value)
}

func (n *node) get(
	ip net.IP,
	depth int,
) (int, *dataType) {
	child := n.children[bitAt(ip, depth)]
	if child == nil {
		return depth, n.value
	}
	return child.get(ip, depth+1)
}

// finalize current just returns the node count. However, it will prune the
// tree eventually.
func (n *node) finalize(currentNum int) int {
	n.nodeNum = currentNum
	if n.isLeaf() {
		return currentNum
	}

	currentNum++
	currentNum = n.children[0].finalize(currentNum)
	return n.children[1].finalize(currentNum)
}

func (n *node) isLeaf() bool {
	return n.children[0] == nil
}

func bitAt(ip net.IP, depth int) byte {
	return (ip[depth/8] >> (7 - (depth % 8))) & 1
}
