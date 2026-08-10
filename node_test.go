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

// TestMaybeMergeChildren covers the reference-equality merge check. Sibling
// data records merge only when they hold the same canonical reference;
// colliding but distinct values must be left alone.
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
			tree.valueStore = newValueStoreWithHash(func([]byte) uint64 { return 1 })
			first, err := tree.valueStore.intern(mmdbtype.String("first"))
			require.NoError(t, err)
			second := first
			if test.sameValue {
				tree.valueStore.retain(first)
			} else {
				second, err = tree.valueStore.intern(mmdbtype.String("second"))
				require.NoError(t, err)
				require.NotEqual(t, first, second)
			}

			parent := record{
				nodeIndex: tree.newNode([2]record{
					{value: first, recordType: recordTypeData},
					{value: second, recordType: recordTypeData},
				}),
				recordType: recordTypeNode,
			}

			iRec := insertRecord{store: tree.valueStore, tree: tree}
			require.NoError(t, iRec.maybeMergeChildren(&parent))

			assert.Equal(t, test.want, parent.recordType)
			if test.want == recordTypeData {
				assert.Equal(t, first, parent.value)
			}
		})
	}
}
