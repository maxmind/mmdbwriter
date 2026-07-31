package mmdbwriter

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestValueStoreCanonicalizesAndMaterializesValues(t *testing.T) {
	uint128 := mmdbtype.Uint128(*big.NewInt(1 << 20))
	value := mmdbtype.Map{
		"bool":    mmdbtype.Bool(true),
		"bytes":   mmdbtype.Bytes{1, 2, 3},
		"float32": mmdbtype.Float32(1.25),
		"float64": mmdbtype.Float64(2.5),
		"int32":   mmdbtype.Int32(-1),
		"slice": mmdbtype.Slice{
			mmdbtype.String("value"),
			mmdbtype.Uint16(16),
			mmdbtype.Uint32(32),
			mmdbtype.Uint64(64),
			&uint128,
		},
	}

	store := newValueStore()
	first, err := store.intern(value)
	require.NoError(t, err)
	second, err := store.intern(value.Copy())
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.True(t, value.Equal(store.materialize(first)))

	store.release(first)
	store.release(second)
}

func TestValueStoreMapOrderDoesNotAffectIdentity(t *testing.T) {
	store := newValueStore()
	first, err := store.intern(mmdbtype.Map{
		"a": mmdbtype.String("first"),
		"b": mmdbtype.String("second"),
	})
	require.NoError(t, err)
	second, err := store.intern(mmdbtype.Map{
		"b": mmdbtype.String("second"),
		"a": mmdbtype.String("first"),
	})
	require.NoError(t, err)

	assert.Equal(t, first, second)
	store.release(first)
	store.release(second)
}

func TestValueStoreHashCollisionsUseExactEquality(t *testing.T) {
	store := newValueStoreWithHash(func([]byte) uint64 { return 7 })
	first, err := store.intern(mmdbtype.String("first"))
	require.NoError(t, err)
	second, err := store.intern(mmdbtype.String("second"))
	require.NoError(t, err)
	equalFirst, err := store.intern(mmdbtype.String("first"))
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
	assert.Equal(t, first, equalFirst)
	assert.Equal(t, mmdbtype.String("first"), store.materialize(first))
	assert.Equal(t, mmdbtype.String("second"), store.materialize(second))

	store.release(first)
	store.release(second)
	store.release(equalFirst)
}

func TestValueStoreCascadeReleaseAndFreelistReuse(t *testing.T) {
	store := newValueStore()
	first, err := store.internUncached(mmdbtype.String("key"))
	require.NoError(t, err)
	second, err := store.internUncached(mmdbtype.Uint32(42))
	require.NoError(t, err)
	ref, err := store.internOwnedChildren(valueKindSlice, nil, []valueRef{first, second})
	require.NoError(t, err)
	nodeCount := len(store.nodes)
	store.release(ref)

	for index := 1; index < nodeCount; index++ {
		assert.Equal(t, valueKindInvalid, store.nodes[index].kind)
	}
	assert.Len(t, store.freeRefs, nodeCount-1)

	reused, err := store.internUncached(mmdbtype.String("replacement"))
	require.NoError(t, err)
	assert.Less(t, int(reused), nodeCount)
	store.release(reused)
}

func TestValueStoreSharesMaterializedSubvalues(t *testing.T) {
	shared := mmdbtype.Map{"en": mmdbtype.String("London")}
	store := newValueStore()
	first, err := store.intern(mmdbtype.Map{"names": shared, "id": mmdbtype.Uint32(1)})
	require.NoError(t, err)
	second, err := store.intern(mmdbtype.Map{"names": shared.Copy(), "id": mmdbtype.Uint32(2)})
	require.NoError(t, err)

	firstNames := store.materialize(first).(mmdbtype.Map)["names"].(mmdbtype.Map)
	secondNames := store.materialize(second).(mmdbtype.Map)["names"].(mmdbtype.Map)
	assert.Equal(t, reflect.ValueOf(firstNames).Pointer(), reflect.ValueOf(secondNames).Pointer())

	store.release(first)
	store.release(second)
}

func TestValueStoreMaterializedBytesDoNotAliasReusedArena(t *testing.T) {
	store := newValueStore()
	ref, err := store.internUncached(mmdbtype.Bytes{1, 2, 3})
	require.NoError(t, err)
	materialized := store.materialize(ref).(mmdbtype.Bytes)
	store.release(ref)

	replacement, err := store.internUncached(mmdbtype.Bytes{9, 9, 9})
	require.NoError(t, err)
	assert.Equal(t, mmdbtype.Bytes{1, 2, 3}, materialized)
	store.release(replacement)
}

func TestDataIdentityDistinguishesKindsAndRejectsNilUint128(t *testing.T) {
	bytesIdentity, ok := dataIdentity(mmdbtype.Bytes{})
	require.True(t, ok)
	sliceIdentity, ok := dataIdentity(mmdbtype.Slice{})
	require.True(t, ok)
	assert.NotEqual(t, bytesIdentity, sliceIdentity)

	var uint128 *mmdbtype.Uint128
	_, ok = dataIdentity(uint128)
	assert.False(t, ok)
}
