package mmdbwriter

import (
	"fmt"
	"hash/maphash"
	"net/netip"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func newSubtreeTestTable(tree *Tree) *subtreeTable {
	return &subtreeTable{
		tree:      tree,
		ids:       make([]uint32, tree.nodeCountAllocated),
		slots:     make([]subtreeSlot, 256),
		seed:      maphash.MakeSeed(),
		protected: noNodeIndex,
	}
}

func TestSubtreeUniqueAncestorStillSharesDescendants(t *testing.T) {
	for direction := range 2 {
		t.Run(fmt.Sprintf("direction=%d", direction), func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)
			unique, err := tree.valueStore.intern(mmdbtype.String("unique"))
			require.NoError(t, err)
			shared, err := tree.valueStore.intern(mmdbtype.String("shared"))
			require.NoError(t, err)
			tree.valueStore.retain(shared)
			leaf := tree.newNode([2]record{{recordType: recordTypeData, value: shared}, {}})
			copyLeaf := tree.newNode(tree.nodeAt(leaf).children)
			var children [2]record
			children[direction] = record{recordType: recordTypeData, value: unique}
			children[1-direction] = record{recordType: recordTypeNode, nodeIndex: leaf}
			parent := tree.newNode(children)
			tree.nodeAt(tree.root).children = [2]record{
				{recordType: recordTypeNode, nodeIndex: parent},
				{recordType: recordTypeNode, nodeIndex: copyLeaf},
			}
			require.NoError(t, tree.auditValueStore())
			table := newSubtreeTestTable(tree)
			_, isUnique := table.visit(tree.root)
			require.True(t, isUnique)
			require.Equal(t, table.ids[leaf], table.ids[copyLeaf])
			want, _ := referenceSubtreeCount(tree)
			require.EqualValues(t, want, table.distinct)
			require.Equal(t, 1, table.used, "unique ancestors should bypass the interning table")
			require.NoError(t, tree.auditValueStore())
		})
	}
}

func TestSubtreeUniquenessIsConservative(t *testing.T) {
	for _, owner := range []string{"caller cache", "container"} {
		t.Run(owner, func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)
			var value mmdbtype.DataType = mmdbtype.String("value")
			if owner == "caller cache" {
				value = mmdbtype.Map{"key": value}
			}
			ref, err := tree.valueStore.intern(value)
			require.NoError(t, err)
			leaf := tree.newNode([2]record{{recordType: recordTypeData, value: ref}, {}})
			tree.nodeAt(tree.root).children[0] = record{recordType: recordTypeNode, nodeIndex: leaf}
			if owner == "caller cache" {
				tree.valueStore.rememberCallerIdentity(value, ref)
			} else {
				container, internErr := tree.valueStore.intern(mmdbtype.Map{"key": value})
				require.NoError(t, internErr)
				tree.nodeAt(tree.root).children[1] = record{
					recordType: recordTypeData,
					value:      container,
				}
			}
			require.EqualValues(t, 2, tree.valueStore.node(ref).refCount)
			table := newSubtreeTestTable(tree)
			_, unique := table.visit(leaf)
			require.False(t, unique, "non-tree ownership may prevent the shortcut")
			require.Equal(t, 1, table.used)
			require.NoError(t, tree.auditValueStore())
		})
	}
}

func TestSubtreeProtectionDoesNotProveUniqueness(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	protected := tree.newNode([2]record{{recordType: recordTypeReserved}, {}})
	ordinary := tree.newNode(tree.nodeAt(protected).children)
	tree.nodeAt(tree.root).children = [2]record{
		{recordType: recordTypeNode, nodeIndex: protected},
		{recordType: recordTypeNode, nodeIndex: ordinary},
	}
	table := newSubtreeTestTable(tree)
	table.protected = protected
	_, unique := table.visit(tree.root)
	require.False(t, unique)
	require.NotEqual(t, table.ids[protected], table.ids[ordinary])
	require.EqualValues(t, 3, table.distinct)
	require.Equal(t, 2, table.used, "only the protected node bypasses interning")
}

func TestSubtreeUniqueSharedTransitions(t *testing.T) {
	for _, version := range []int{4, 6} {
		for _, aliases := range []bool{false, true} {
			for _, size := range []int{24, 28, 32} {
				t.Run(
					fmt.Sprintf("ipv%d/aliases=%t/size=%d", version, aliases, size),
					func(t *testing.T) {
						opts := Options{
							BuildEpoch:   123,
							IPVersion:    version,
							RecordSize:   size,
							DatabaseType: "Subtree-Transitions",
							Description: map[string]string{
								"en": "Unique and shared subtree transitions",
							},
							IncludeReservedNetworks: true,
							DisableIPv4Aliasing:     !aliases,
						}
						control, err := New(opts)
						require.NoError(t, err)
						tree, err := New(opts)
						require.NoError(t, err)
						prefixes := []string{"1.2.3.4/32", "2.2.3.4/32", "1.2.3.4/32", "3.2.3.4/32"}
						if version == 6 {
							prefixes = append(prefixes, "2001:db8::1/128", "2001:db8:1::1/128")
						}
						for i, prefix := range prefixes {
							var value mmdbtype.DataType = mmdbtype.String("shared after insertion")
							if i == 2 {
								value = nil
							}
							require.NoError(t, control.Insert(netip.MustParsePrefix(prefix), value))
							require.NoError(t, tree.Insert(netip.MustParsePrefix(prefix), value))
							finalizeUnsharedTree(control)
							left, openErr := maxminddb.OpenBytes(writeTreeBytes(t, control))
							require.NoError(t, openErr)
							output := writeTreeBytes(t, tree)
							require.Equal(
								t,
								output,
								writeTreeBytes(t, tree),
								"cached writes must be stable",
							)
							right, openErr := maxminddb.OpenBytes(output)
							require.NoError(t, openErr)
							require.NoError(t, right.Verify())
							compareSubtreeReaders(t, left, right)
							compareSubtreeReaders(
								t,
								left,
								right,
								maxminddb.IncludeAliasedNetworks(),
								maxminddb.IncludeNetworksWithoutData(),
							)
							left.Close()
							right.Close()
							require.NoError(t, tree.auditValueStore())
						}
					},
				)
			}
		}
	}
}
