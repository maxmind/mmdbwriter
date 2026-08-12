package mmdbwriter

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestValueStoreRefcountAudit(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.Map{
		"names": mmdbtype.Map{"en": mmdbtype.String("one")},
	}))
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.0.0/16"), mmdbtype.Map{
		"names": mmdbtype.Map{"en": mmdbtype.String("two")},
	}))
	require.NoError(t, tree.auditValueStore())

	_, foundRecord := tree.getNode(tree.root, [16]byte{1, 3}, 0)
	require.Equal(t, recordTypeData, foundRecord.recordType)
	ref := foundRecord.value
	tree.valueStore.nodes[ref].refCount++
	err = tree.auditValueStore()
	require.Error(t, err)
	assert.Regexp(t, `refcount audit for ref \d+: stored \d+, expected \d+`, err.Error())
	tree.valueStore.nodes[ref].refCount--
	require.NoError(t, tree.auditValueStore())
}

func TestRefcountAuditMode(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
		RefcountAudit:           true,
	})
	require.NoError(t, err)
	assert.True(t, tree.refcountAudit)

	// Each insert runs the audit; the second merges the two half networks.
	value := mmdbtype.String("same")
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/25"), value))
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.128/25"), value))

	prefix, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), prefix,
		"the half networks did not merge")
	assert.Equal(t, value, got)
}

func TestRefcountAuditEnvironmentOverride(t *testing.T) {
	t.Setenv("MMDBWRITER_REFCOUNT_AUDIT", "1")
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	assert.True(t, tree.refcountAudit,
		"the environment variable did not enable the audit")
}

// TestValueStoreAuditRejectsCorruptStores corrupts each store structure the
// audit validates and asserts the matching error.
func TestValueStoreAuditRejectsCorruptStores(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(t *testing.T, tree *Tree)
		want    string
	}{
		{
			name: "live ref missing from its bucket",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				requireDataRef(t, tree)
				clear(tree.valueStore.buckets)
			},
			want: "in 0 hash buckets",
		},
		{
			name: "bucket chain cycle",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref := requireDataRef(t, tree)
				tree.valueStore.nodes[ref].nextInBucket = ref
			},
			want: "bucket chain cycle",
		},
		{
			name: "duplicate freelist entry",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				scratch, err := tree.valueStore.internUncached(mmdbtype.String("scratch"))
				require.NoError(t, err)
				tree.valueStore.release(scratch)
				tree.valueStore.freeRefs = append(tree.valueStore.freeRefs, scratch)
			},
			want: "listed 2 times",
		},
		{
			name: "invalid child ref",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref := requireDataRef(t, tree)
				node := &tree.valueStore.nodes[ref]
				require.NotZero(t, node.childrenLen, "the fixture value must be a container")
				tree.valueStore.children.data[node.childrenOffset] = nilValueRef
			},
			want: "invalid child ref",
		},
		{
			name: "dead materialized identity",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				identity, ok := dataIdentity(mmdbtype.Map{"stale": mmdbtype.String("x")})
				require.True(t, ok)
				tree.valueStore.materializedByIdentity[identity] = outOfRangeRef(tree.valueStore)
			},
			want: "dead materialized ref",
		},
		{
			name: "caller identity ref out of range",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				value := mmdbtype.Map{"pinned": mmdbtype.String("x")}
				require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.4.0/24"), value))
				identity, ok := dataIdentity(value)
				require.True(t, ok)
				index, ok := tree.valueStore.callerByIdentity[identity]
				require.True(t, ok, "the insert did not register the caller identity")
				tree.valueStore.callerIdentity[index].ref = outOfRangeRef(tree.valueStore)
			},
			want: "invalid caller identity ref",
		},
		{
			name: "high-bit caller identity ref",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				value := mmdbtype.Map{"pinned": mmdbtype.String("x")}
				require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.4.0/24"), value))
				identity, ok := dataIdentity(value)
				require.True(t, ok)
				index, ok := tree.valueStore.callerByIdentity[identity]
				require.True(t, ok, "the insert did not register the caller identity")
				// The high bit makes the int conversion negative on 386, so a
				// signed bounds check would pass and the audit would panic.
				tree.valueStore.callerIdentity[index].ref = valueRef(1 << 31)
			},
			want: "invalid caller identity ref",
		},
		{
			name: "high-bit materialized identity ref",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				identity, ok := dataIdentity(mmdbtype.Map{"stale": mmdbtype.String("x")})
				require.True(t, ok)
				tree.valueStore.materializedByIdentity[identity] = valueRef(1 << 31)
			},
			want: "dead materialized ref",
		},
		{
			name: "caller identity index mismatch",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				identity, ok := dataIdentity(mmdbtype.Map{"phantom": mmdbtype.String("x")})
				require.True(t, ok)
				tree.valueStore.callerByIdentity[identity] = 0
			},
			want: "entries but",
		},
		{
			name: "invalid LRU link",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				requireCallerIdentityEntry(t, tree)
				tree.valueStore.callerIdentity[0].next = 7
			},
			want: "invalid LRU link at 7",
		},
		{
			name: "inconsistent LRU entry",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				requireCallerIdentityEntry(t, tree)
				tree.valueStore.callerIdentity[0].prev = 3
			},
			want: "inconsistent entry at 0",
		},
		{
			name: "incomplete LRU chain",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				requireCallerIdentityEntry(t, tree)
				tree.valueStore.callerIdentityTail = 5
			},
			want: "incomplete LRU chain",
		},
		{
			name: "head left in an emptied cache",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				requireCallerIdentityEntry(t, tree)
				tree.valueStore.callerIdentity = nil
				clear(tree.valueStore.callerByIdentity)
			},
			want: "head or tail in an empty cache",
		},
		{
			name: "ref in the wrong hash bucket",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref := requireDataRef(t, tree)
				tree.valueStore.nodes[ref].hash++
			},
			want: "in the wrong hash bucket",
		},
		{
			name: "live ref on the freelist",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref := requireDataRef(t, tree)
				tree.valueStore.freeRefs = append(tree.valueStore.freeRefs, ref)
			},
			want: "on the freelist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)
			require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.0.0/16"), mmdbtype.Map{
				"names": mmdbtype.Map{"en": mmdbtype.String("one")},
			}))
			require.NoError(t, tree.auditValueStore())

			test.corrupt(t, tree)
			err = tree.auditValueStore()
			require.ErrorContains(t, err, test.want)
		})
	}
}

// outOfRangeRef returns the first ref past the store's node arena, so the
// subtest stays valid if the fixture grows.
func outOfRangeRef(store *valueStore) valueRef {
	return valueRef(uint32(len(store.nodes))) // #nosec G115 -- test stores stay tiny.
}

// requireCallerIdentityEntry asserts that the fixture insert registered a
// caller-identity entry, so corruptions of entry zero test what they intend.
func requireCallerIdentityEntry(t *testing.T, tree *Tree) {
	t.Helper()
	require.NotEmpty(t, tree.valueStore.callerIdentity,
		"the fixture insert did not register a caller identity")
}

func requireDataRef(t *testing.T, tree *Tree) valueRef {
	t.Helper()
	_, dataRecord := tree.getNode(tree.root, [16]byte{1, 2}, 0)
	require.Equal(t, recordTypeData, dataRecord.recordType)
	return dataRecord.value
}

func TestValueStoreAuditRejectsInvalidPathOwnership(t *testing.T) {
	t.Run("repeated path", func(t *testing.T) {
		tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
		require.NoError(t, err)
		pathIndex := tree.newPath([16]byte{}, 1, record{})
		pathRecord := record{nodeIndex: pathIndex, recordType: recordTypePath}
		tree.nodeAt(tree.root).children = [2]record{pathRecord, pathRecord}

		err = tree.auditValueStore()
		require.ErrorContains(t, err, "path 0 with multiple owning paths")
	})

	t.Run("cyclic path", func(t *testing.T) {
		tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
		require.NoError(t, err)
		pathIndex := tree.newPath([16]byte{}, 1, record{})
		pathRecord := record{nodeIndex: pathIndex, recordType: recordTypePath}
		tree.paths[pathIndex].record = pathRecord
		tree.nodeAt(tree.root).children[0] = pathRecord

		err = tree.auditValueStore()
		require.ErrorContains(t, err, "path 0 with multiple owning paths")
	})
}
