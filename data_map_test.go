package mmdbwriter

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestDataMap(t *testing.T) {
	v := mmdbtype.String("test")

	dm := newDataMap()

	dmv, err := dm.store(v)
	require.NoError(t, err)

	assert.Equal(t, v, dmv.data)
	assert.Equal(t, uint32(1), dmv.refCount)
	assert.Nil(t, dmv.next)

	mapDMV := dm.data[dmv.hash]

	assert.Equal(t, dmv, mapDMV)

	dmv, err = dm.store(v)
	require.NoError(t, err)

	assert.Equal(t, uint32(2), dmv.refCount, "refCount incremented on store")

	dm.remove(dmv)

	mapDMV = dm.data[dmv.hash]

	assert.Equal(t, uint32(1), mapDMV.refCount, "refCount decremented on remove")

	dm.remove(dmv)
	_, ok := dm.data[dmv.hash]
	assert.False(t, ok, "map value removed when refCount drops to 0")
}

func TestDataMapCachesDefaultComplexValueIdentity(t *testing.T) {
	v := mmdbtype.Map{
		"test": mmdbtype.String("value"),
	}

	identity, ok := keyIdentity(v)
	require.True(t, ok)

	dm := newDataMap()

	dmv, err := dm.storeWithIdentity(v)
	require.NoError(t, err)

	assert.Same(t, dmv, dm.valueByDataIdentity[identity])

	sameDMV, err := dm.storeWithIdentity(v)
	require.NoError(t, err)

	assert.Same(t, dmv, sameDMV, "same value identity returns same dataMapValue")
	assert.Equal(t, uint32(2), dmv.refCount)

	dm.remove(dmv)
	assert.Same(t, dmv, dm.valueByDataIdentity[identity])

	dm.remove(dmv)
	assert.NotContains(t, dm.valueByDataIdentity, identity)
}

func TestDataMapOnlyCachesRetainedComplexValueIdentity(t *testing.T) {
	tests := []struct {
		name  string
		value func() mmdbtype.DataType
	}{
		{
			name: "bytes",
			value: func() mmdbtype.DataType {
				return mmdbtype.Bytes{1, 2, 3}
			},
		},
		{
			name: "map",
			value: func() mmdbtype.DataType {
				return mmdbtype.Map{"test": mmdbtype.String("value")}
			},
		},
		{
			name: "slice",
			value: func() mmdbtype.DataType {
				return mmdbtype.Slice{mmdbtype.String("value")}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := test.value()
			second := test.value()
			firstIdentity, ok := keyIdentity(first)
			require.True(t, ok)
			secondIdentity, ok := keyIdentity(second)
			require.True(t, ok)
			require.NotEqual(t, firstIdentity, secondIdentity)

			dm := newDataMap()
			dmv, err := dm.storeWithIdentity(first)
			require.NoError(t, err)

			sameDMV, err := dm.storeWithIdentity(second)
			require.NoError(t, err)

			assert.Same(t, dmv, sameDMV, "equal content returns retained dataMapValue")
			assert.Same(t, dmv, dm.valueByDataIdentity[firstIdentity])
			assert.NotContains(t, dm.valueByDataIdentity, secondIdentity)
		})
	}
}

func TestDataMapDoesNotUseStaleIdentityForMutatedNonRetainedValue(t *testing.T) {
	first := mmdbtype.Bytes{1, 2, 3}
	second := mmdbtype.Bytes{1, 2, 3}

	dm := newDataMap()
	firstDMV, err := dm.storeWithIdentity(first)
	require.NoError(t, err)

	secondDMV, err := dm.storeWithIdentity(second)
	require.NoError(t, err)
	require.Same(t, firstDMV, secondDMV)

	second[0] = 9
	mutatedDMV, err := dm.storeWithIdentity(second)
	require.NoError(t, err)

	assert.NotSame(t, firstDMV, mutatedDMV)
	assert.Equal(t, second, mutatedDMV.data)
}

func TestDataMapDoesNotUseStaleIdentityEntry(t *testing.T) {
	retained := mmdbtype.Bytes{1, 2, 3}
	requested := mmdbtype.Bytes{4, 5, 6}

	dm := newDataMap()
	retainedDMV, err := dm.storeWithIdentity(retained)
	require.NoError(t, err)

	requestedIdentity, ok := keyIdentity(requested)
	require.True(t, ok)
	dm.valueByDataIdentity[requestedIdentity] = retainedDMV

	requestedDMV, err := dm.storeWithIdentity(requested)
	require.NoError(t, err)

	assert.NotSame(t, retainedDMV, requestedDMV)
	assert.Equal(t, requested, requestedDMV.data)
	assert.Same(t, requestedDMV, dm.valueByDataIdentity[requestedIdentity])
}

func TestDataMapCachesUint128PointerIdentity(t *testing.T) {
	value := mmdbtype.Uint128(*big.NewInt(12345))
	valuePointer := &value
	identity, ok := keyIdentity(valuePointer)
	require.True(t, ok)

	dm := newDataMap()
	dmv, err := dm.storeWithIdentity(valuePointer)
	require.NoError(t, err)

	sameDMV, err := dm.storeWithIdentity(valuePointer)
	require.NoError(t, err)

	assert.Same(t, dmv, sameDMV)
	assert.Equal(t, uint32(2), dmv.refCount)
	assert.Same(t, dmv, dm.valueByDataIdentity[identity])
}

func TestKeyIdentityDistinguishesKinds(t *testing.T) {
	bytesIdentity, ok := keyIdentity(mmdbtype.Bytes{})
	require.True(t, ok)

	sliceIdentity, ok := keyIdentity(mmdbtype.Slice{})
	require.True(t, ok)

	assert.NotEqual(t, bytesIdentity, sliceIdentity)

	var uint128 *mmdbtype.Uint128
	_, ok = keyIdentity(uint128)
	assert.False(t, ok)
}

func TestKeyIdentityCollapsesEmptyMaps(t *testing.T) {
	var nilMap mmdbtype.Map
	nilIdentity, ok := keyIdentity(nilMap)
	require.True(t, ok)

	emptyIdentity, ok := keyIdentity(mmdbtype.Map{})
	require.True(t, ok)

	assert.Equal(t, nilIdentity, emptyIdentity)
}

func TestDataMapResolvesHashCollisionsByExactValue(t *testing.T) {
	dm := newDataMap()
	first := mmdbtype.Map{"value": mmdbtype.String("first")}
	second := mmdbtype.Map{"value": mmdbtype.String("second")}

	firstValue := dm.storeByHash(first, 1)
	secondValue := dm.storeByHash(second, 1)

	assert.NotSame(t, firstValue, secondValue)
	assert.Same(t, secondValue, dm.data[1])
	assert.Same(t, firstValue, secondValue.next)

	duplicate := dm.storeByHash(first.Copy(), 1)
	assert.Same(t, firstValue, duplicate)

	dm.remove(secondValue)
	assert.Same(t, firstValue, dm.data[1])
}

func TestDataMapRemovesNonHeadCollisionEntry(t *testing.T) {
	dm := newDataMap()
	firstValue := dm.storeByHash(mmdbtype.String("first"), 1)
	secondValue := dm.storeByHash(mmdbtype.String("second"), 1)

	dm.remove(firstValue)

	assert.Same(t, secondValue, dm.data[1])
	assert.Nil(t, secondValue.next)
}

func TestDataMapRejectsReferenceCountUnderflow(t *testing.T) {
	dm := newDataMap()
	value := dm.storeByHash(mmdbtype.String("value"), 1)
	dm.remove(value)

	assert.PanicsWithValue(
		t,
		"mmdbwriter: dataMap.remove called on a value with no references",
		func() { dm.remove(value) },
	)
}

func TestDataMapCollisionComparisonPreservesFloatEncoding(t *testing.T) {
	dm := newDataMap()
	positiveZero := mmdbtype.Map{"value": mmdbtype.Float64(0)}
	negativeZero := mmdbtype.Map{"value": mmdbtype.Float64(math.Copysign(0, -1))}

	positiveValue := dm.storeByHash(positiveZero, 1)
	negativeValue := dm.storeByHash(negativeZero, 1)

	assert.NotSame(t, positiveValue, negativeValue)
}

// TestWireDataEqualRejectsUnequalValues covers the negative branches of the
// exact comparison that hash matches are confirmed against.
func TestWireDataEqualRejectsUnequalValues(t *testing.T) {
	tests := []struct {
		name   string
		first  mmdbtype.DataType
		second mmdbtype.DataType
	}{
		{
			name:   "slice length mismatch",
			first:  mmdbtype.Slice{mmdbtype.Uint32(1)},
			second: mmdbtype.Slice{mmdbtype.Uint32(1), mmdbtype.Uint32(2)},
		},
		{
			name:   "slice element mismatch",
			first:  mmdbtype.Slice{mmdbtype.Uint32(1)},
			second: mmdbtype.Slice{mmdbtype.Uint32(2)},
		},
		{
			name:   "map length mismatch",
			first:  mmdbtype.Map{"a": mmdbtype.Uint32(1)},
			second: mmdbtype.Map{"a": mmdbtype.Uint32(1), "b": mmdbtype.Uint32(2)},
		},
		{
			name:   "map key mismatch",
			first:  mmdbtype.Map{"a": mmdbtype.Uint32(1)},
			second: mmdbtype.Map{"b": mmdbtype.Uint32(1)},
		},
		{
			name:   "map value mismatch",
			first:  mmdbtype.Map{"a": mmdbtype.Uint32(1)},
			second: mmdbtype.Map{"a": mmdbtype.Uint32(2)},
		},
		{
			name:   "float64 signed zero",
			first:  mmdbtype.Float64(0),
			second: mmdbtype.Float64(math.Copysign(0, -1)),
		},
		{
			name:   "float32 signed zero",
			first:  mmdbtype.Float32(0),
			second: mmdbtype.Float32(float32(math.Copysign(0, -1))),
		},
		{
			name:   "nil operand",
			first:  nil,
			second: mmdbtype.Uint32(1),
		},
		{
			name:   "nil pointer form",
			first:  (*mmdbtype.Map)(nil),
			second: mmdbtype.Map{},
		},
		{
			name:   "different types with the same value",
			first:  mmdbtype.Uint32(1),
			second: mmdbtype.Int32(1),
		},
		{
			// Bytes and String hash the same pre-salt bytes, so this pair is
			// the one most dependent on the per-type salts.
			name:   "bytes and string with the same contents",
			first:  mmdbtype.Bytes("x"),
			second: mmdbtype.String("x"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.False(t, wireDataEqual(test.first, test.second))
			assert.False(t, wireDataEqual(test.second, test.first))
		})
	}
}

// TestWireDataEqualAcceptsEqualValues covers the positive branches, including
// the empty-container short circuits.
func TestWireDataEqualAcceptsEqualValues(t *testing.T) {
	nested := mmdbtype.Slice{mmdbtype.Map{"a": mmdbtype.String("x")}}

	assert.True(t, wireDataEqual(nested, nested.Copy()))
	assert.True(t, wireDataEqual(mmdbtype.Slice{}, mmdbtype.Slice{}))
	assert.True(t, wireDataEqual(mmdbtype.Map{}, mmdbtype.Map{}))
	assert.True(t, wireDataEqual(nil, nil))
}

// TestDataMapCollisionComparisonPreservesFloat32Encoding is the Float32 twin of
// TestDataMapCollisionComparisonPreservesFloatEncoding. Go treats the two zeros
// as equal, but they have different wire encodings.
func TestDataMapCollisionComparisonPreservesFloat32Encoding(t *testing.T) {
	dm := newDataMap()
	positiveZero := mmdbtype.Map{"value": mmdbtype.Float32(0)}
	negativeZero := mmdbtype.Map{
		"value": mmdbtype.Float32(float32(math.Copysign(0, -1))),
	}

	positiveValue := dm.storeByHash(positiveZero, 1)
	negativeValue := dm.storeByHash(negativeZero, 1)

	assert.NotSame(t, positiveValue, negativeValue)
}

// TestDataMapRemovesMiddleCollisionEntry removes an entry with entries on both
// sides of it, so the unlink has a tail it must preserve.
func TestDataMapRemovesMiddleCollisionEntry(t *testing.T) {
	dm := newDataMap()
	first := dm.storeByHash(mmdbtype.String("first"), 1)
	middle := dm.storeByHash(mmdbtype.String("middle"), 1)
	last := dm.storeByHash(mmdbtype.String("last"), 1)

	// Entries are prepended, so the chain is last -> middle -> first.
	require.Same(t, last, dm.data[1])
	require.Same(t, middle, last.next)

	dm.remove(middle)

	assert.Same(t, last, dm.data[1])
	assert.Same(t, first, last.next, "the entries after the removed one were lost")
	assert.Nil(t, first.next)
}

// TestDataMapIgnoresStaleIdentityForReleasedValue covers the reference count
// guard on the identity fast path. A cached identity may outlive the value it
// points at, and a released value must never be resurrected.
func TestDataMapIgnoresStaleIdentityForReleasedValue(t *testing.T) {
	dm := newDataMap()
	slice := mmdbtype.Slice{mmdbtype.String("a")}
	identity, ok := keyIdentity(slice)
	require.True(t, ok)

	first, err := dm.storeWithIdentity(slice)
	require.NoError(t, err)
	dm.remove(first)
	require.Zero(t, first.refCount)

	// Simulate an identity entry left behind pointing at the released value.
	if dm.valueByDataIdentity == nil {
		dm.valueByDataIdentity = map[dataMapIdentityKey]*dataMapValue{}
	}
	dm.valueByDataIdentity[identity] = first

	second, err := dm.storeWithIdentity(slice)
	require.NoError(t, err)

	assert.NotSame(t, first, second,
		"a released value was resurrected from the identity cache")
	assert.NotZero(t, second.refCount)
}

// TestDataMapRejectsValueMissingFromItsBucket pins the twin of the reference
// count assertion. A value whose hash no longer names its bucket means the
// index is corrupt; unlinking blind would sever the chain and orphan every
// entry behind it.
func TestDataMapRejectsValueMissingFromItsBucket(t *testing.T) {
	dm := newDataMap()
	value := dm.storeByHash(mmdbtype.String("value"), 1)

	// Simulate a corrupt index by pointing the value at an empty bucket.
	value.hash = 2

	assert.PanicsWithValue(
		t,
		"mmdbwriter: dataMap.remove called on a value missing from its bucket",
		func() { dm.remove(value) },
	)
}
