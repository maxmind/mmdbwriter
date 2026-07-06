package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewNodeIndexRejectsSentinel(t *testing.T) {
	require.Panics(t, func() {
		newNodeIndex(int(noNodeIndex))
	})
}

func TestRecordValueRejectsCompressedPath(t *testing.T) {
	tree := &Tree{}

	_, err := tree.recordValue(&record{recordType: recordTypePath}, nil)
	require.EqualError(t, err, "compressed path record cannot be written before finalization")
}

func TestFinalizeNodeRejectsCompressedPath(t *testing.T) {
	tree := &Tree{
		nodeBlocks:         [][]node{make([]node, nodeBlockSize)},
		nodeCountAllocated: 1,
		nodeNumbers:        make([]int, 1),
	}
	tree.nodeAt(rootNodeIndex).children[0] = record{recordType: recordTypePath}

	require.PanicsWithValue(t, "compressed path found after expandPaths", func() {
		tree.finalizeNode(rootNodeIndex, 0)
	})
}
