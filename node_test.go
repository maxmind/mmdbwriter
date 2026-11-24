package mmdbwriter

import (
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestMergeChildrenAfterInsertFixedNode(t *testing.T) {
	for _, insertErr := range []error{nil, errors.New("insert failed")} {
		t.Run(fmt.Sprintf("failure=%t", insertErr != nil), func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)
			ref, err := tree.valueStore.intern(mmdbtype.String("equal"))
			require.NoError(t, err)
			tree.valueStore.retain(ref)
			child := record{recordType: recordTypeData, value: ref}
			index := tree.newNode([2]record{child, child})
			parent := &tree.nodeAt(tree.root).children[0]
			*parent = record{recordType: recordTypeFixedNode, nodeIndex: index}
			// An alias needs this exact node even when both children are equal.
			tree.nodeAt(tree.root).children[1] = record{
				recordType: recordTypeAlias,
				nodeIndex:  index,
			}
			iRec := insertRecord{tree: tree, store: tree.valueStore}
			require.ErrorIs(t, iRec.mergeChildrenAfterInsert(parent, insertErr), insertErr)
			require.Equal(t, recordTypeFixedNode, parent.recordType)
			require.Equal(t, index, parent.nodeIndex)
			require.Equal(t, [2]record{child, child}, tree.nodeAt(index).children)
			require.NoError(t, tree.auditValueStore())
		})
	}
}

func TestMergeChildrenAfterInsertErrors(t *testing.T) {
	for _, mergeFails := range []bool{false, true} {
		for _, insertErr := range []error{nil, errors.New("insert failed")} {
			t.Run(
				fmt.Sprintf("mergeFailure=%t/insertFailure=%t", mergeFails, insertErr != nil),
				func(t *testing.T) {
					tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
					require.NoError(t, err)
					child := record{recordType: recordTypeEmpty}
					if mergeFails {
						// Alias children are valid ownership edges, but cannot be merged.
						child = record{recordType: recordTypeAlias, nodeIndex: tree.root}
					}
					parent := &tree.nodeAt(tree.root).children[0]
					*parent = record{
						recordType: recordTypeNode,
						nodeIndex:  tree.newNode([2]record{child, child}),
					}
					iRec := insertRecord{tree: tree, store: tree.valueStore}
					err = iRec.mergeChildrenAfterInsert(parent, insertErr)
					if mergeFails {
						const mergeMessage = "merging record type 3 is not implemented"
						if insertErr == nil {
							require.EqualError(t, err, mergeMessage)
						} else {
							require.ErrorIs(t, err, insertErr)
							require.ErrorContains(
								t,
								err,
								"restoring record boundaries after insert failure: "+mergeMessage,
							)
							var joined interface{ Unwrap() []error }
							require.ErrorAs(t, err, &joined)
							require.Len(t, joined.Unwrap(), 2)
							require.EqualError(t, errors.Unwrap(joined.Unwrap()[1]), mergeMessage)
						}
						require.Equal(t, recordTypeNode, parent.recordType)
					} else {
						require.ErrorIs(t, err, insertErr)
						require.Equal(t, recordTypeEmpty, parent.recordType)
					}
					require.NoError(t, tree.auditValueStore())
				},
			)
		}
	}
}

func TestNewNodeIndexRejectsSentinel(t *testing.T) {
	// On 32-bit platforms the sentinel wraps to a negative int, which the
	// negative-index guard rejects instead of the sentinel comparison.
	sentinel := uint64(noNodeIndex)
	require.Panics(t, func() {
		newNodeIndex(int(sentinel)) //nolint:gosec // intentional boundary conversion
	})
}

func TestRecordValueRejectsCompressedPath(t *testing.T) {
	tree := &Tree{}

	_, err := tree.recordValue(&record{recordType: recordTypePath}, nil)
	require.EqualError(t, err, "compressed path record cannot be written before finalization")
}

func TestCanonicalizeSubtreesRejectsCompressedPath(t *testing.T) {
	tree := &Tree{
		nodeBlocks:         [][]node{make([]node, nodeBlockSize)},
		nodeCountAllocated: 1,
		nodeNumbers:        make([]uint32, 1),
		valueStore:         newValueStore(),
	}
	tree.nodeAt(rootNodeIndex).children[0] = record{recordType: recordTypePath}

	require.PanicsWithValue(
		t,
		"mmdbwriter: compressed path found after expandPaths at node 0 during subtree canonicalization",
		func() {
			tree.canonicalizeSubtrees()
		},
	)
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

// Each distinct call stack seen by the heap profiler costs a bucket that is
// never freed (https://github.com/golang/go/issues/69590). Callbacks, and the
// allocations around them, should therefore see the same stack depth however
// deep the records they resolve.
func TestInsertCallbackStackDepthIsConstant(t *testing.T) {
	tree, err := New(Options{IPVersion: 6, DisableIPv4Aliasing: true})
	require.NoError(t, err)
	for i, prefix := range []string{
		"2a00:1450::/32",
		"2a00:1450:8000::/33",
		"2a00:1450::1/128",
		"2a00:1450::ff00/120",
		"2a00:1450:1:2:3:4:5:6/127",
	} {
		require.NoError(t, tree.Insert(netip.MustParsePrefix(prefix), mmdbtype.Uint32(i)))
	}

	depths := map[int]int{}
	stackDepths := map[int]bool{}
	pcs := make([]uintptr, 512)
	err = tree.InsertFunc(
		netip.MustParsePrefix("2a00::/16"),
		mmdbtype.String("cover"),
		func(_, newValue mmdbtype.DataType, metadata inserter.Metadata) (mmdbtype.DataType, error) {
			depths[metadata.ExistingNetwork().Bits()]++
			stackDepths[runtime.Callers(0, pcs)] = true
			return newValue, nil
		},
	)
	require.NoError(t, err)
	assert.Contains(t, depths, 128)
	assert.Len(t, stackDepths, 1, "callback stack depth varies with record depth")
}
