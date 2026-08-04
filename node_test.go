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

// TestMaybeMergeChildren covers the pointer-identity merge test. Sibling data
// records merge only when they hold the same value; colliding but distinct
// values must be left alone.
func TestMaybeMergeChildren(t *testing.T) {
	tests := []struct {
		name      string
		sameValue bool
		want      recordType
	}{
		{
			name:      "colliding distinct values are not merged",
			sameValue: false,
			want:      recordTypeNode,
		},
		{
			name:      "identical values are merged",
			sameValue: true,
			want:      recordTypeData,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := newTestTree(t, "mmdbwriter-merge")

			// Both values share a bucket, so only the exact comparison can
			// tell them apart.
			first := tree.dataMap.storeByHash(mmdbtype.String("first"), 1)
			second := first
			if test.sameValue {
				tree.dataMap.addRef(first)
			} else {
				second = tree.dataMap.storeByHash(mmdbtype.String("second"), 1)
				require.NotSame(t, first, second)
			}

			parent := record{
				nodeIndex: tree.newNode([2]record{
					{value: first, recordType: recordTypeData},
					{value: second, recordType: recordTypeData},
				}),
				recordType: recordTypeNode,
			}

			iRec := insertRecord{dataMap: tree.dataMap, tree: tree}
			require.NoError(t, iRec.maybeMergeChildren(&parent))

			assert.Equal(t, test.want, parent.recordType)
			if test.want == recordTypeData {
				assert.Same(t, first, parent.value)
			}
		})
	}
}
