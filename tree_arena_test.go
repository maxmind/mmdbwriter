package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestTreeArenaReusesRetiredSlots(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true, BuildEpoch: 123})
	require.NoError(t, err)
	// Exercise recycling even when the CI environment enables poisoning.
	tree.poisonTreeSlots = false
	churnTree(t, tree)
	nodes, paths := tree.nodeCountAllocated, len(tree.paths)
	for range 100 {
		churnTree(t, tree)
		require.NoError(t, tree.auditValueStore())
	}
	require.Equal(t, nodes, tree.nodeCountAllocated)
	require.Len(t, tree.paths, paths)
	require.NotEmpty(t, tree.freeNodes)
	require.NotEmpty(t, tree.freePaths)
}

func TestTreeArenaFailedInsertReusesSlots(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	tree.poisonTreeSlots = false
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.String("base")))
	failure := errors.New("inserter failed")
	fail := func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) { return nil, failure }
	require.ErrorIs(t, tree.InsertFunc(netip.MustParsePrefix("1.2.3.4/32"), nil, fail), failure)
	nodes := tree.nodeCountAllocated
	for range 100 {
		require.ErrorIs(t, tree.InsertFunc(netip.MustParsePrefix("1.2.3.4/32"), nil, fail), failure)
		require.NoError(t, tree.auditValueStore())
	}
	require.Equal(t, nodes, tree.nodeCountAllocated)
	prefix, value := tree.Get(netip.MustParseAddr("1.2.3.4"))
	require.Equal(t, netip.MustParsePrefix("1.0.0.0/8"), prefix)
	require.Equal(t, mmdbtype.String("base"), value)
}

func TestTreeArenaWriteThenMutate(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true, BuildEpoch: 123})
	require.NoError(t, err)
	tree.poisonTreeSlots = false
	var first []byte
	for range 3 {
		require.NoError(
			t,
			tree.Insert(netip.MustParsePrefix("1.2.3.4/32"), mmdbtype.String("value")),
		)
		var output bytes.Buffer
		_, err = tree.WriteTo(&output)
		require.NoError(t, err)
		require.Nil(t, tree.paths)
		require.Nil(t, tree.freePaths)
		require.NoError(t, tree.auditValueStore())
		if first == nil {
			first = bytes.Clone(output.Bytes())
		} else {
			require.Equal(t, first, output.Bytes())
		}
		output.Reset()
		_, err = tree.WriteTo(&output)
		require.NoError(t, err)
		require.Equal(t, first, output.Bytes())
		require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/8"), nil))
	}
}

func TestTreeArenaPoison(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true, RefcountAudit: true})
	require.NoError(t, err)
	index := tree.newNode([2]record{})
	tree.retireNode(index)
	require.PanicsWithError(
		t,
		fmt.Sprintf("mmdbwriter: retired node index %d", index),
		func() { tree.nodeAt(index) },
	)
	require.Panics(t, func() { tree.retireNode(index) })
	fresh := tree.newNode([2]record{})
	require.NotEqual(t, index, fresh)
	tree.retireNode(fresh)
	path := tree.newPath([16]byte{}, 32, record{})
	tree.retirePath(path)
	require.PanicsWithError(
		t,
		fmt.Sprintf("mmdbwriter: retired path index %d", path),
		func() { tree.pathAt(path) },
	)
	require.Panics(t, func() { tree.retirePath(path) })
	next := tree.newPath([16]byte{}, 32, record{})
	require.NotEqual(t, path, next)
	tree.retirePath(next)
	require.NoError(t, tree.auditValueStore())
	require.Panics(t, func() { tree.retireNode(tree.root) })
}

func TestTreeArenaAuditRejectsCorruption(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(*Tree)
	}{
		{
			"duplicate free node",
			func(tree *Tree) { tree.freeNodes = append(tree.freeNodes, tree.freeNodes[0]) },
		},
		{
			"duplicate free path",
			func(tree *Tree) { tree.freePaths = append(tree.freePaths, tree.freePaths[0]) },
		},
		{"unclaimed node", func(tree *Tree) { tree.newNode([2]record{}) }},
		{"unclaimed path", func(tree *Tree) { tree.newPath([16]byte{}, 32, record{}) }},
		{"retired node edge", func(tree *Tree) {
			tree.nodeAt(tree.root).children[0] = record{
				recordType: recordTypeNode,
				nodeIndex:  tree.freeNodes[0],
			}
		}},
		{"retired path edge", func(tree *Tree) {
			tree.nodeAt(tree.root).children[0] = record{
				recordType: recordTypePath,
				nodeIndex:  tree.freePaths[0],
			}
		}},
		{"invalid alias", func(tree *Tree) {
			tree.nodeAt(tree.root).children[0] = record{
				recordType: recordTypeAlias,
				nodeIndex:  noNodeIndex,
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)
			tree.poisonTreeSlots = false
			churnTree(t, tree)
			require.NoError(t, tree.auditValueStore())
			tt.corrupt(tree)
			require.Error(t, tree.auditValueStore())
		})
	}
}
