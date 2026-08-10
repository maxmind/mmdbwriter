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

func TestValueStoreNormalizesPointerBackedValues(t *testing.T) {
	boolValue := mmdbtype.Bool(true)
	bytesValue := mmdbtype.Bytes{1, 2}
	float32Value := mmdbtype.Float32(1.5)
	float64Value := mmdbtype.Float64(2.5)
	int32Value := mmdbtype.Int32(-3)
	mapValue := mmdbtype.Map{"key": mmdbtype.String("value")}
	sliceValue := mmdbtype.Slice{mmdbtype.Uint16(1)}
	stringValue := mmdbtype.String("value")
	uint16Value := mmdbtype.Uint16(16)
	uint32Value := mmdbtype.Uint32(32)
	uint64Value := mmdbtype.Uint64(64)

	tests := []struct {
		name    string
		value   mmdbtype.DataType
		pointer mmdbtype.DataType
	}{
		{name: "Bool", value: boolValue, pointer: &boolValue},
		{name: "Bytes", value: bytesValue, pointer: &bytesValue},
		{name: "Float32", value: float32Value, pointer: &float32Value},
		{name: "Float64", value: float64Value, pointer: &float64Value},
		{name: "Int32", value: int32Value, pointer: &int32Value},
		{name: "Map", value: mapValue, pointer: &mapValue},
		{name: "Slice", value: sliceValue, pointer: &sliceValue},
		{name: "String", value: stringValue, pointer: &stringValue},
		{name: "Uint16", value: uint16Value, pointer: &uint16Value},
		{name: "Uint32", value: uint32Value, pointer: &uint32Value},
		{name: "Uint64", value: uint64Value, pointer: &uint64Value},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newValueStore()
			direct, err := store.intern(test.value)
			require.NoError(t, err)
			viaPointer, err := store.intern(test.pointer)
			require.NoError(t, err)

			assert.Equal(t, direct, viaPointer)
			assert.True(t, test.value.Equal(store.materialize(direct)))
			store.release(direct)
			store.release(viaPointer)
		})
	}
}

func TestValueStoreRejectsInvalidUint128(t *testing.T) {
	store := newValueStore()

	negative := mmdbtype.Uint128(*big.NewInt(-1))
	_, err := store.intern(&negative)
	require.EqualError(t, err, "cannot intern a negative *mmdbtype.Uint128")

	wide := mmdbtype.Uint128(*new(big.Int).Lsh(big.NewInt(1), 128))
	_, err = store.intern(&wide)
	require.EqualError(t, err, "cannot intern a *mmdbtype.Uint128 wider than 128 bits")

	// A nested rejection must leave no live nodes behind.
	_, err = store.intern(mmdbtype.Map{"value": &negative})
	require.ErrorContains(t, err, "cannot intern a negative *mmdbtype.Uint128")
	for index := 1; index < len(store.nodes); index++ {
		assert.Equal(t, valueKindInvalid, store.nodes[index].kind)
	}

	maxValue := mmdbtype.Uint128(
		*new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1)))
	ref, err := store.intern(&maxValue)
	require.NoError(t, err)
	assert.True(t, maxValue.Equal(store.materialize(ref)))
	store.release(ref)
}

func TestValueStoreRejectsInvalidSliceChildren(t *testing.T) {
	store := newValueStore()

	_, err := store.intern(mmdbtype.Slice{mmdbtype.String("ok"), nil})
	require.EqualError(t, err, "slice index 1 has a nil value")

	var nilMap *mmdbtype.Map
	_, err = store.intern(mmdbtype.Slice{mmdbtype.String("ok"), nilMap})
	require.ErrorContains(t, err, "cannot intern a nil *mmdbtype.Map")

	// Both rejections must release the children already interned.
	for index := 1; index < len(store.nodes); index++ {
		assert.Equal(t, valueKindInvalid, store.nodes[index].kind)
	}
}

func TestValueStoreRejectsNilPointerBackedValue(t *testing.T) {
	store := newValueStore()
	var nilMap *mmdbtype.Map
	_, err := store.intern(nilMap)
	require.EqualError(t, err, "cannot intern a nil *mmdbtype.Map")

	var nilUint128 *mmdbtype.Uint128
	_, err = store.intern(nilUint128)
	require.EqualError(t, err, "cannot intern a nil *mmdbtype.Uint128")
}

func TestValueStoreRejectsPointerValue(t *testing.T) {
	store := newValueStore()
	_, err := store.intern(mmdbtype.Pointer(1))
	require.EqualError(t, err, "unsupported MMDB data type mmdbtype.Pointer")
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

func TestValueStoreContainerHashCollisionsUseExactEquality(t *testing.T) {
	store := newValueStoreWithHash(func([]byte) uint64 { return 7 })
	firstValue := mmdbtype.Map{
		"nested": mmdbtype.Slice{mmdbtype.String("first")},
	}
	secondValue := mmdbtype.Map{
		"nested": mmdbtype.Slice{mmdbtype.String("second")},
	}
	first, err := store.intern(firstValue)
	require.NoError(t, err)
	second, err := store.intern(secondValue)
	require.NoError(t, err)
	equalFirst, err := store.intern(firstValue.Copy())
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
	assert.Equal(t, first, equalFirst)
	assert.Equal(t, firstValue, store.materialize(first))
	assert.Equal(t, secondValue, store.materialize(second))

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
	ref, err := store.internOwnedChildren(valueKindSlice, []valueRef{first, second})
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

// TestReleaseUnregistersMaterializedIdentity pins that releasing a node
// removes its materialized identity before the reference can be reused. A
// stale entry would silently substitute unrelated data.
func TestReleaseUnregistersMaterializedIdentity(t *testing.T) {
	store := newValueStore()
	// Keep the caller-identity cache out of the way so release fully frees
	// the node.
	store.callerIdentityLimit = 0
	ref, err := store.intern(mmdbtype.Map{"en": mmdbtype.String("one")})
	require.NoError(t, err)
	view := store.materialize(ref)
	identity, ok := dataIdentity(view)
	require.True(t, ok)
	require.Contains(t, store.materializedByIdentity, identity)

	store.release(ref)
	assert.NotContains(t, store.materializedByIdentity, identity,
		"a released node left its materialized identity registered")

	// Reusing the freed ref must not resurrect the old identity mapping.
	replacement, err := store.intern(mmdbtype.Map{"en": mmdbtype.String("two")})
	require.NoError(t, err)
	assert.NotContains(t, store.materializedByIdentity, identity)
	store.release(replacement)
}

// TestReleaseUnlinksMidChainNode covers unlinking a node that is neither the
// head nor the tail of a forced hash-collision chain.
func TestReleaseUnlinksMidChainNode(t *testing.T) {
	store := newValueStoreWithHash(func([]byte) uint64 { return 7 })
	first, err := store.intern(mmdbtype.String("first"))
	require.NoError(t, err)
	second, err := store.intern(mmdbtype.String("second"))
	require.NoError(t, err)
	third, err := store.intern(mmdbtype.String("third"))
	require.NoError(t, err)

	// The chain head is the most recent node, so second is mid-chain.
	store.release(second)

	firstAgain, err := store.intern(mmdbtype.String("first"))
	require.NoError(t, err)
	assert.Equal(t, first, firstAgain)
	thirdAgain, err := store.intern(mmdbtype.String("third"))
	require.NoError(t, err)
	assert.Equal(t, third, thirdAgain)

	store.release(first)
	store.release(firstAgain)
	store.release(third)
	store.release(thirdAgain)
}

// TestCallerIdentityEvictionReleasesReference pins that evicting the
// least-recently-used entry releases the reference the cache held.
func TestCallerIdentityEvictionReleasesReference(t *testing.T) {
	store := newValueStore()
	store.callerIdentityLimit = 1
	first := mmdbtype.Map{"value": mmdbtype.String("first")}
	firstRef, err := store.intern(first)
	require.NoError(t, err)
	store.rememberCallerIdentity(first, firstRef)
	// The cache now holds the only reference.
	store.release(firstRef)

	second := mmdbtype.Map{"value": mmdbtype.String("second")}
	secondRef, err := store.intern(second)
	require.NoError(t, err)
	store.rememberCallerIdentity(second, secondRef)

	assert.Equal(t, valueKindInvalid, store.nodes[firstRef].kind,
		"the evicted entry did not release its reference")
	firstIdentity, ok := dataIdentity(first)
	require.True(t, ok)
	assert.NotContains(t, store.callerByIdentity, firstIdentity)
	store.release(secondRef)
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

func TestValueStoreCallerIdentityCacheIsLRU(t *testing.T) {
	store := newValueStore()
	store.callerIdentityLimit = 2
	first := mmdbtype.Map{"value": mmdbtype.String("first")}
	second := mmdbtype.Map{"value": mmdbtype.String("second")}
	third := mmdbtype.Map{"value": mmdbtype.String("third")}

	firstRef, err := store.intern(first)
	require.NoError(t, err)
	store.release(firstRef)
	secondRef, err := store.intern(second)
	require.NoError(t, err)
	store.release(secondRef)

	// Refresh first so second becomes the least-recently-used entry.
	firstRef, err = store.intern(first)
	require.NoError(t, err)
	store.release(firstRef)
	thirdRef, err := store.intern(third)
	require.NoError(t, err)
	store.release(thirdRef)

	firstIdentity, _ := dataIdentity(first)
	secondIdentity, _ := dataIdentity(second)
	thirdIdentity, _ := dataIdentity(third)
	assert.Contains(t, store.callerByIdentity, firstIdentity)
	assert.NotContains(t, store.callerByIdentity, secondIdentity)
	assert.Contains(t, store.callerByIdentity, thirdIdentity)
}
