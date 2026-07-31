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
	require.ErrorContains(t, err, "stored")
	tree.valueStore.nodes[ref].refCount--
	require.NoError(t, tree.auditValueStore())
}

func TestRefcountAuditAndPoisonModes(t *testing.T) {
	t.Setenv("MMDBWRITER_REFCOUNT_AUDIT", "1")
	t.Setenv("MMDBWRITER_DEBUG_POISON", "1")
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	assert.True(t, tree.refcountAudit)
	assert.True(t, tree.debugPoison)
	value := mmdbtype.String("same")
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/25"), value))
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.128/25"), value))
	require.NotEmpty(t, tree.freeNodes)
	for _, index := range tree.freeNodes {
		node := tree.nodeAt(index)
		assert.Equal(t, recordType(0xff), node.children[0].recordType)
		assert.Equal(t, recordType(0xff), node.children[1].recordType)
	}
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
