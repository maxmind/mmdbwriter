package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
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

// TestMaybeMergeChildrenKeepsCollidingDistinctValues pins the pointer-identity
// merge test. Sibling data records whose values collide in the hash but differ
// in content must not be merged.
func TestMaybeMergeChildrenKeepsCollidingDistinctValues(t *testing.T) {
	tree, err := New(Options{
		DatabaseType:            "mmdbwriter-merge",
		Description:             map[string]string{"en": "Test database"},
		IPVersion:               4,
		RecordSize:              24,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	first := tree.dataMap.storeByHash(mmdbtype.String("first"), 1)
	second := tree.dataMap.storeByHash(mmdbtype.String("second"), 1)
	require.NotSame(t, first, second)

	parent := record{
		nodeIndex: tree.newNode([2]record{
			{value: first, recordType: recordTypeData},
			{value: second, recordType: recordTypeData},
		}),
		recordType: recordTypeNode,
	}

	iRec := insertRecord{dataMap: tree.dataMap, tree: tree}
	require.NoError(t, iRec.maybeMergeChildren(&parent))

	assert.Equal(t, recordTypeNode, parent.recordType,
		"records holding different colliding values were merged")
}

// TestMaybeMergeChildrenMergesIdenticalValues is the control for
// TestMaybeMergeChildrenKeepsCollidingDistinctValues.
func TestMaybeMergeChildrenMergesIdenticalValues(t *testing.T) {
	tree, err := New(Options{
		DatabaseType:            "mmdbwriter-merge",
		Description:             map[string]string{"en": "Test database"},
		IPVersion:               4,
		RecordSize:              24,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	shared := tree.dataMap.storeByHash(mmdbtype.String("shared"), 1)
	tree.dataMap.addRef(shared)

	parent := record{
		nodeIndex: tree.newNode([2]record{
			{value: shared, recordType: recordTypeData},
			{value: shared, recordType: recordTypeData},
		}),
		recordType: recordTypeNode,
	}

	iRec := insertRecord{dataMap: tree.dataMap, tree: tree}
	require.NoError(t, iRec.maybeMergeChildren(&parent))

	assert.Equal(t, recordTypeData, parent.recordType)
	assert.Same(t, shared, parent.value)
}
