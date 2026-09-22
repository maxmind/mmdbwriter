package mmdbwriter

import (
	"fmt"
	"hash/maphash"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func newSubtreeTestTable(tree *Tree, capacity int) *subtreeTable {
	return &subtreeTable{
		tree:      tree,
		ids:       make([]uint32, tree.nodeCountAllocated),
		slots:     make([]subtreeSlot, capacity),
		seed:      maphash.MakeSeed(),
		protected: noNodeIndex,
	}
}

// referenceSubtreeCount uses recursive structural strings rather than the
// compact table's keys or IDs, so it can detect incorrect canonicalization.
func referenceSubtreeCount(tree *Tree) (distinct, total int) {
	seen := map[string]bool{}
	protected := subtreeIPv4Root(tree)
	var visit func(nodeIndex) string
	visit = func(index nodeIndex) string {
		total++
		n := tree.nodeAt(index)
		var key strings.Builder
		key.WriteByte('(')
		for _, r := range n.children {
			fmt.Fprintf(&key, "%d:", r.recordType)
			switch r.recordType {
			case recordTypeNode, recordTypeFixedNode:
				key.WriteString(visit(r.nodeIndex))
			case recordTypeData:
				fmt.Fprint(&key, r.value)
			case recordTypeAlias:
				fmt.Fprint(&key, r.nodeIndex)
			case recordTypeEmpty, recordTypeReserved:
			default:
				panic("unexpected reference record")
			}
			key.WriteByte(',')
		}
		key.WriteByte(')')
		if index == protected {
			key.WriteString("ipv4-root")
		}
		seen[key.String()] = true
		return key.String()
	}
	visit(tree.root)
	return len(seen), total
}

func subtreeReachableCount(tree *Tree, index nodeIndex) int {
	count := 1
	for _, child := range tree.nodeAt(index).children {
		if child.recordType == recordTypeNode || child.recordType == recordTypeFixedNode {
			count += subtreeReachableCount(tree, child.nodeIndex)
		}
	}
	return count
}

func TestSubtreeCounter(t *testing.T) {
	for _, ipVersion := range []int{4, 6} {
		for _, poison := range []bool{false, true} {
			t.Run(fmt.Sprintf("ipv%d/poison=%t", ipVersion, poison), func(t *testing.T) {
				tree, err := New(Options{IPVersion: ipVersion, RefcountAudit: poison})
				require.NoError(t, err)
				for i := range 80 {
					ip := netip.AddrFrom4([4]byte{2, byte(i + 1), 4, 5})
					require.NoError(t, tree.Insert(netip.PrefixFrom(ip, 32), mmdbtype.Uint32(i%3)))
				}
				churnTree(t, tree)
				tree.expandPaths(tree.root, 0)
				wantDistinct, wantTotal := referenceSubtreeCount(tree)
				tree.nodeNumbers = make([]uint32, tree.nodeCountAllocated)
				distinct := tree.canonicalizeSubtrees()
				require.Equal(t, wantDistinct, distinct)
				require.Equal(t, wantTotal, subtreeReachableCount(tree, tree.root))
				require.Less(t, distinct, wantTotal)
				for i := range tree.nodeCountAllocated {
					if tree.rawNodeAt(nodeIndex(i)).children[0].recordType == recordTypeRetired {
						require.Zero(t, tree.nodeNumbers[i])
					}
				}
				require.NoError(t, tree.auditValueStore())
			})
		}
	}
}

func TestSubtreeHashCollisions(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	a := tree.newNode([2]record{{recordType: recordTypeReserved}, {}})
	b := tree.newNode([2]record{{}, {recordType: recordTypeReserved}})
	copyA := tree.newNode(tree.nodeAt(a).children)
	table := newSubtreeTestTable(tree, 2)
	for _, index := range []nodeIndex{a, b, copyA} {
		table.ids[index] = table.intern(index, table.key(index), 1)
	}
	require.Equal(t, table.ids[a], table.ids[copyA])
	require.NotEqual(t, table.ids[a], table.ids[b])
	require.Len(t, table.slots, 4, "growth must preserve colliding entries")
}

func TestSubtreeHashCollisionPayloads(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind recordType
	}{
		{"data", recordTypeData},
		{"node", recordTypeNode},
		{"fixed-node", recordTypeFixedNode},
		{"alias", recordTypeAlias},
	} {
		for direction := range 2 {
			t.Run(fmt.Sprintf("%s/child=%d", tc.name, direction), func(t *testing.T) {
				tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
				require.NoError(t, err)
				valueA, err := tree.valueStore.intern(mmdbtype.String("A"))
				require.NoError(t, err)
				valueB, err := tree.valueStore.intern(mmdbtype.String("B"))
				require.NoError(t, err)
				childA := tree.newNode([2]record{{recordType: recordTypeReserved}, {}})
				childB := tree.newNode([2]record{{}, {recordType: recordTypeReserved}})
				copyChildA := tree.newNode(tree.nodeAt(childA).children)
				recordA, recordB := record{recordType: tc.kind}, record{recordType: tc.kind}
				if tc.kind == recordTypeData {
					recordA.value, recordB.value = valueA, valueB
				} else {
					recordA.nodeIndex, recordB.nodeIndex = childA, childB
				}
				children := [2]record{}
				children[direction] = recordA
				a := tree.newNode(children)
				copyA := tree.newNode(children)
				children[direction] = recordB
				b := tree.newNode(children)
				copyB := tree.newNode(children)
				children = tree.nodeAt(a).children
				if tc.kind == recordTypeNode || tc.kind == recordTypeFixedNode {
					children[direction].nodeIndex = copyChildA
				}
				equivalentA := tree.newNode(children)
				table := newSubtreeTestTable(tree, 4)
				// These model already-canonicalized children. Equal child IDs
				// must compare equal even when their arena indexes differ.
				table.ids[childA], table.ids[childB], table.ids[copyChildA] = 100, 101, 100
				var keyA, keyB subtreeKey
				keyA[2] = uint32(tc.kind) << (8 * direction)
				keyB[2] = keyA[2]
				switch tc.kind {
				case recordTypeData:
					keyA[direction], keyB[direction] = uint32(valueA), uint32(valueB)
				case recordTypeNode, recordTypeFixedNode:
					keyA[direction], keyB[direction] = 100, 101
				case recordTypeAlias:
					keyA[direction], keyB[direction] = uint32(childA), uint32(childB)
				default:
					t.Fatal("unexpected record type")
				}
				require.Equal(t, keyA, table.key(a))
				require.Equal(t, keyB, table.key(b))
				require.Equal(t, keyA, table.key(equivalentA))
				for _, probe := range []struct {
					index nodeIndex
					want  uint32
				}{
					{a, 1}, {b, 2}, {copyA, 1}, {copyB, 2}, {equivalentA, 1}, {a, 1}, {b, 2},
				} {
					id := table.intern(probe.index, table.key(probe.index), 7)
					table.ids[probe.index] = id
					require.Equal(t, probe.want, id)
				}
				require.EqualValues(t, 2, table.distinct)
				require.Equal(t, 2, table.used)
			})
		}
	}
}

func TestSubtreeHashCollisionGrowth(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	var nodes [4][3]nodeIndex
	for i := range nodes {
		value, internErr := tree.valueStore.intern(mmdbtype.Uint32(i))
		require.NoError(t, internErr)
		for copyIndex := range nodes[i] {
			nodes[i][copyIndex] = tree.newNode(
				[2]record{{recordType: recordTypeData, value: value}, {}},
			)
		}
	}
	table := newSubtreeTestTable(tree, 4)
	intern := func(index nodeIndex) uint32 {
		// Hash 7 wraps the cluster around the end of both the old and new tables.
		id := table.intern(index, table.key(index), 7)
		table.ids[index] = id
		return id
	}
	var ids [4]uint32
	for i := range 3 {
		ids[i] = intern(nodes[i][0])
	}
	require.EqualValues(t, 3, table.distinct)
	require.Equal(t, 3, table.used)
	require.Len(t, table.slots, 4)
	for i := range 3 {
		require.Equal(t, ids[i], intern(nodes[i][1]))
	}
	require.Len(t, table.slots, 4, "duplicates at the growth threshold must not grow the table")
	ids[3] = intern(nodes[3][0])
	require.EqualValues(t, 4, table.distinct)
	require.Equal(t, 4, table.used)
	require.Len(t, table.slots, 8, "growth must preserve all three colliding residents")
	for _, copyIndex := range []int{2, 0} {
		for i := range nodes {
			require.Equal(t, ids[i], intern(nodes[i][copyIndex]))
		}
	}
	require.EqualValues(t, 4, table.distinct)
	require.Equal(t, 4, table.used)
	require.Len(t, table.slots, 8)
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
			table := newSubtreeTestTable(tree, 256)
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
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	value := mmdbtype.String("value")
	ref, err := tree.valueStore.intern(value)
	require.NoError(t, err)
	leaf := tree.newNode([2]record{{recordType: recordTypeData, value: ref}, {}})
	tree.nodeAt(tree.root).children[0] = record{recordType: recordTypeNode, nodeIndex: leaf}
	container, err := tree.valueStore.intern(mmdbtype.Map{"key": value})
	require.NoError(t, err)
	tree.nodeAt(tree.root).children[1] = record{recordType: recordTypeData, value: container}
	require.EqualValues(t, 2, tree.valueStore.node(ref).refCount)
	table := newSubtreeTestTable(tree, 256)
	_, unique := table.visit(leaf)
	require.False(t, unique, "non-tree ownership may prevent the shortcut")
	require.Equal(t, 1, table.used)
	require.NoError(t, tree.auditValueStore())
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
	table := newSubtreeTestTable(tree, 256)
	table.protected = protected
	_, unique := table.visit(tree.root)
	require.False(t, unique)
	require.NotEqual(t, table.ids[protected], table.ids[ordinary])
	require.EqualValues(t, 3, table.distinct)
	require.Equal(t, 2, table.used, "only the protected node bypasses interning")
}
