package mmdbwriter

// finalizeUnsharedTree gives each owning node a distinct preorder number so the
// regular serializer can produce a trie for comparison with its DAG output.
// This test-only oracle bypasses canonicalization, but shares no hashing or
// subtree-equivalence logic with it. Mutation or clearing nodeCount restores
// normal finalization on the next write.
func finalizeUnsharedTree(tree *Tree) {
	tree.expandTree()
	tree.nodeNumbers = make([]uint32, tree.nodeCountAllocated)
	tree.nodeCount = 0
	var visit func(nodeIndex)
	visit = func(index nodeIndex) {
		tree.nodeNumbers[index] = uint32(newNodeIndex(tree.nodeCount))
		tree.nodeCount++
		for _, child := range tree.nodeAt(index).children {
			if child.recordType == recordTypeNode || child.recordType == recordTypeFixedNode {
				visit(child.nodeIndex)
			}
		}
	}
	visit(tree.root)
}
