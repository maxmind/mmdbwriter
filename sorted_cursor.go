package mmdbwriter

import "math/bits"

// sortedInsertCursor remembers the node path for the most recent disjoint
// replacement. Ascending inserts resume at the deepest common node and unwind
// sibling merges only through the skipped ancestors.
type sortedInsertCursor struct {
	valid    bool
	disabled bool

	lastStart [16]byte
	lastEnd   [16]byte
	nodes     []nodeIndex // nodes[depth] is the node at that bit depth
	nextNodes []nodeIndex

	lastResumeDepth int // test/benchmark visibility
}

func (c *sortedInsertCursor) start(
	ip [16]byte,
	prefixLen int,
	treeDepth int,
	root nodeIndex,
) (nodeIndex, int) {
	startDepth := 0
	startNode := root
	canResume := c.valid && compareInternalIP(c.lastEnd, ip) < 0
	if canResume {
		common := commonPrefixBits(c.lastStart, ip, treeDepth)
		if common >= len(c.nodes) {
			common = len(c.nodes) - 1
		}
		if common > prefixLen {
			common = prefixLen
		}
		if common > 0 {
			startDepth = common
			startNode = c.nodes[common]
		}
	}

	c.disabled = false
	c.nextNodes = c.nextNodes[:0]
	if canResume && startDepth < len(c.nodes) {
		c.nextNodes = append(c.nextNodes, c.nodes[:startDepth+1]...)
	} else {
		c.nextNodes = append(c.nextNodes, root)
		startDepth = 0
		startNode = root
	}
	c.lastStart = ip
	c.lastEnd = prefixEnd(ip, prefixLen, treeDepth)
	c.lastResumeDepth = startDepth
	return startNode, startDepth
}

func (c *sortedInsertCursor) recordNode(index nodeIndex, depth int) {
	if c.disabled {
		return
	}
	switch {
	case depth < len(c.nextNodes):
		if c.nextNodes[depth] != index {
			c.disabled = true
		}
	case depth == len(c.nextNodes):
		c.nextNodes = append(c.nextNodes, index)
	default:
		c.disabled = true
	}
}

func (c *sortedInsertCursor) branched()    { c.disabled = true }
func (c *sortedInsertCursor) invalidated() { c.disabled = true }

func (c *sortedInsertCursor) finish(success bool) {
	if !success || c.disabled {
		c.valid = false
		c.nodes = c.nodes[:0]
		return
	}
	c.valid = true
	c.nodes, c.nextNodes = c.nextNodes, c.nodes[:0]
}

func (c *sortedInsertCursor) reset() {
	c.valid = false
	c.disabled = false
	c.nodes = c.nodes[:0]
	c.nextNodes = c.nextNodes[:0]
}

func commonPrefixBits(left, right [16]byte, limit int) int {
	depth := 0
	for depth+8 <= limit {
		difference := left[depth/8] ^ right[depth/8]
		if difference != 0 {
			return depth + bits.LeadingZeros8(difference)
		}
		depth += 8
	}
	for depth < limit && bitAt(left, depth) == bitAt(right, depth) {
		depth++
	}
	return depth
}

func compareInternalIP(left, right [16]byte) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func prefixEnd(ip [16]byte, prefixLen, treeDepth int) [16]byte {
	end := ip
	for depth := prefixLen; depth < treeDepth; depth++ {
		setBit(&end, depth, 1)
	}
	return end
}
