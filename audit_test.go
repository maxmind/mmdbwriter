package mmdbwriter

import (
	"errors"
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
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
	assert.True(t, tree.valueStore.poisonFreedRefs,
		"audit mode did not poison released refs")

	// Each insert runs the audit; the second merges the two half networks.
	value := mmdbtype.String("same")
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/25"), value))
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.128/25"), value))

	prefix, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), prefix,
		"the half networks did not merge")
	assert.Equal(t, value, got)

	staleIP, _ := tree.prefixInsertIP(prefix)
	_, staleRecord := tree.getNode(tree.root, staleIP, 0)
	require.Equal(t, recordTypeData, staleRecord.recordType)
	stale := staleRecord.value
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"), mmdbtype.String("replacement")))
	assert.Empty(t, tree.valueStore.freeRefs,
		"audit mode queued a released ref for reuse")
	require.PanicsWithValue(t,
		fmt.Sprintf("mmdbwriter: invalid value reference %d", stale),
		func() { tree.valueStore.node(stale) })

	freshPrefix := netip.MustParsePrefix("2.2.2.0/24")
	require.NoError(t, tree.Insert(freshPrefix, mmdbtype.String("fresh")))
	freshIP, _ := tree.prefixInsertIP(freshPrefix)
	_, freshRecord := tree.getNode(tree.root, freshIP, 0)
	require.Equal(t, recordTypeData, freshRecord.recordType)
	assert.NotEqual(t, stale, freshRecord.value,
		"audit mode recycled a released ref")
}

func TestRefcountAuditEnvironmentOverride(t *testing.T) {
	t.Setenv("MMDBWRITER_REFCOUNT_AUDIT", "1")
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	assert.True(t, tree.refcountAudit,
		"the environment variable did not enable the audit")
	assert.True(t, tree.valueStore.poisonFreedRefs,
		"the environment variable did not enable ref poisoning")
}

// TestAuditFailureReturnsTypedError pins that an audit failure surfaces as a
// *RefcountAuditError, distinguishable from a rejected input.
func TestAuditFailureReturnsTypedError(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
		RefcountAudit:           true,
	})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.0.0/16"), mmdbtype.String("first")))

	ref := requireDataRef(t, tree)
	tree.valueStore.nodes[ref].refCount++
	err = tree.Insert(netip.MustParsePrefix("2.2.2.0/24"), mmdbtype.String("second"))
	require.Error(t, err)
	var auditErr *RefcountAuditError
	require.ErrorAs(t, err, &auditErr)
}

// TestFailedInsertsPassTheAudit pins that the audit runs and balances after
// error-path inserts, which is where ownership mistakes hide.
func TestFailedInsertsPassTheAudit(t *testing.T) {
	tests := []struct {
		name   string
		insert func(t *testing.T, tree *Tree) error
	}{
		{
			name: "reserved network",
			insert: func(_ *testing.T, tree *Tree) error {
				return tree.Insert(
					netip.MustParsePrefix("10.0.0.0/8"), mmdbtype.String("nope"))
			},
		},
		{
			name: "inserter error mid-walk",
			insert: func(t *testing.T, tree *Tree) error {
				require.NoError(t, tree.Insert(
					netip.MustParsePrefix("1.0.0.0/25"), mmdbtype.String("left")))
				require.NoError(t, tree.Insert(
					netip.MustParsePrefix("1.0.0.128/25"), mmdbtype.String("right")))
				calls := 0
				return tree.InsertPureFunc(
					netip.MustParsePrefix("1.0.0.0/24"),
					mmdbtype.String("new"),
					func(_, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
						calls++
						if calls == 2 {
							return nil, errors.New("inserter failure")
						}
						return newValue, nil
					},
				)
			},
		},
		{
			name: "invalid nested value from an inserter",
			insert: func(t *testing.T, tree *Tree) error {
				require.NoError(t, tree.Insert(
					netip.MustParsePrefix("1.0.0.0/24"), mmdbtype.String("old")))
				return tree.InsertFunc(
					netip.MustParsePrefix("1.0.0.0/24"),
					mmdbtype.String("new"),
					func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
						return mmdbtype.Map{"p": mmdbtype.Pointer(7)}, nil
					},
				)
			},
		},
		{
			name: "invalid nested value from a direct insert",
			insert: func(_ *testing.T, tree *Tree) error {
				return tree.Insert(
					netip.MustParsePrefix("1.0.0.0/24"),
					mmdbtype.Map{
						"a-valid":   mmdbtype.String("value"),
						"z-invalid": mmdbtype.Pointer(7),
					},
				)
			},
		},
		{
			name: "invalid nested value from a direct range insert",
			insert: func(_ *testing.T, tree *Tree) error {
				return tree.InsertRange(
					netip.MustParseAddr("1.0.0.0"),
					netip.MustParseAddr("1.0.0.255"),
					mmdbtype.Map{
						"a-valid":   mmdbtype.String("value"),
						"z-invalid": mmdbtype.Pointer(7),
					},
				)
			},
		},
		{
			name: "partial range into a reserved subnet",
			insert: func(_ *testing.T, tree *Tree) error {
				return tree.InsertRange(
					netip.MustParseAddr("9.255.255.0"),
					netip.MustParseAddr("10.0.0.255"),
					mmdbtype.String("partial"),
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, RefcountAudit: true})
			require.NoError(t, err)
			err = test.insert(t, tree)
			require.Error(t, err)
			var auditErr *RefcountAuditError
			require.NotErrorAs(t, err, &auditErr,
				"the failed insert left the store unbalanced")
			require.NoError(t, tree.auditValueStore(),
				"the failed insert left the store unbalanced")
		})
	}
}

// TestDirectValueFailureRunsAudit pins that validation failures after direct
// value interning begins still surface an existing audit failure.
func TestDirectValueFailureRunsAudit(t *testing.T) {
	tests := []struct {
		name   string
		insert func(tree *Tree, value mmdbtype.DataType) error
	}{
		{
			name: "insert",
			insert: func(tree *Tree, value mmdbtype.DataType) error {
				return tree.Insert(netip.MustParsePrefix("2.0.0.0/24"), value)
			},
		},
		{
			name: "range insert",
			insert: func(tree *Tree, value mmdbtype.DataType) error {
				return tree.InsertRange(
					netip.MustParseAddr("2.0.0.0"),
					netip.MustParseAddr("2.0.0.255"),
					value,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := New(Options{
				IPVersion:               4,
				IncludeReservedNetworks: true,
				RefcountAudit:           true,
			})
			require.NoError(t, err)
			require.NoError(t, tree.Insert(
				netip.MustParsePrefix("1.2.0.0/16"), mmdbtype.String("existing")))
			ref := requireDataRef(t, tree)
			tree.valueStore.nodes[ref].refCount++

			err = test.insert(tree, mmdbtype.Map{
				"a-valid":   mmdbtype.String("value"),
				"z-invalid": mmdbtype.Pointer(7),
			})
			require.ErrorContains(t, err, "unsupported MMDB data type mmdbtype.Pointer")
			var auditErr *RefcountAuditError
			require.ErrorAs(t, err, &auditErr,
				"the direct-value failure skipped the audit")
		})
	}
}

// TestPoisonedStoreDetectsUseAfterRelease pins that a poisoned store never
// recycles a released slot, so a stale ref panics instead of silently
// reading whatever value reused it.
func TestPoisonedStoreDetectsUseAfterRelease(t *testing.T) {
	store := newValueStore()
	store.poisonFreedRefs = true

	stale, err := store.internUncached(mmdbtype.String("stale"))
	require.NoError(t, err)
	store.release(stale)
	assert.Empty(t, store.freeRefs, "the poisoned store queued a freed slot")

	fresh, err := store.internUncached(mmdbtype.String("fresh"))
	require.NoError(t, err)
	assert.NotEqual(t, stale, fresh, "the poisoned store recycled a slot")
	require.PanicsWithValue(t,
		fmt.Sprintf("mmdbwriter: invalid value reference %d", stale),
		func() { store.node(stale) })

	store.release(fresh)
	require.NoError(t, store.audit(map[valueRef]uint64{}))
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
				// The subtest corrupts the recycling freelist, so force the
				// recycling mode even when the audit env var poisons refs.
				tree.valueStore.poisonFreedRefs = false
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
			name: "overlapping payload extents",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				first, err := tree.valueStore.internUncached(mmdbtype.String("abcd"))
				require.NoError(t, err)
				second, err := tree.valueStore.internUncached(mmdbtype.String("wxyz"))
				require.NoError(t, err)
				tree.valueStore.nodes[second].payloadOffset = tree.valueStore.nodes[first].payloadOffset
			},
			want: "overlapping extents at payload arena offset",
		},
		{
			name: "free extent overlapping a live payload",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref, err := tree.valueStore.internUncached(mmdbtype.String("live"))
				require.NoError(t, err)
				node := tree.valueStore.nodes[ref]
				tree.valueStore.payloads.release(node.payloadOffset, node.payloadLen)
			},
			want: "overlapping extents at payload arena offset",
		},
		{
			name: "double-listed free child extent",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref, err := tree.valueStore.intern(mmdbtype.Slice{mmdbtype.String("gone")})
				require.NoError(t, err)
				node := tree.valueStore.nodes[ref]
				offset, length := node.childrenOffset, node.childrenLen
				tree.valueStore.release(ref)
				tree.valueStore.children.release(offset, length)
			},
			want: "overlapping extents at child arena offset",
		},
		{
			name: "free extent past the arena end",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				tree.valueStore.payloads.release(
					// #nosec G115 -- test arenas stay far below 2^32 bytes.
					uint32(len(tree.valueStore.payloads.data)), 8)
			},
			want: "past the arena end",
		},
		{
			name: "zero-length live extent with nonzero offset",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref, err := tree.valueStore.intern(mmdbtype.Slice{})
				require.NoError(t, err)
				tree.valueStore.nodes[ref].childrenOffset = 1
			},
			want: "zero-length child extent at offset 1",
		},
		{
			name: "zero-length free extent",
			corrupt: func(_ *testing.T, tree *Tree) {
				if tree.valueStore.payloads.free == nil {
					tree.valueStore.payloads.free = map[uint32][]uint32{}
				}
				tree.valueStore.payloads.free[0] = []uint32{0}
			},
			want: "zero-length free payload extent",
		},
		{
			name: "unclaimed payload extent",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				ref, err := tree.valueStore.internUncached(mmdbtype.String("unclaimed"))
				require.NoError(t, err)
				length := tree.valueStore.nodes[ref].payloadLen
				tree.valueStore.release(ref)
				delete(tree.valueStore.payloads.free, length)
			},
			want: "unclaimed payload arena offset",
		},
		{
			name: "non-data record holding a value ref",
			corrupt: func(t *testing.T, tree *Tree) {
				t.Helper()
				record := &tree.nodeAt(tree.root).children[1]
				require.NotEqual(t, recordTypeData, record.recordType,
					"the fixture must leave the root's right child without data")
				record.value = requireDataRef(t, tree)
			},
			want: "record holding value ref",
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
