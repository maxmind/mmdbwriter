package mmdbwriter

// Calculates the search tree size in bytes as listed in the mmdb file specification
func (t *Tree) CalculateTreeSize() uint64 {
	return ((uint64(t.recordSize) * 2) / 8) * uint64(t.nodeCount)
}

// Calculates the data section start offset as listed in the mmdb file specification
func (t *Tree) CalculateDataSectionStartOffset() uint64 {
	return t.treeSize + 16
}
