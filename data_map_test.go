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

	assert.Same(t, dmv, dm.keyByDataIdentity[identity])

	sameDMV, err := dm.storeWithIdentity(v)
	require.NoError(t, err)

	assert.Same(t, dmv, sameDMV, "same value identity returns same dataMapValue")
	assert.Equal(t, uint32(2), dmv.refCount)

	dm.remove(dmv)
	assert.Same(t, dmv, dm.keyByDataIdentity[identity])

	dm.remove(dmv)
	assert.NotContains(t, dm.keyByDataIdentity, identity)
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
			assert.Same(t, dmv, dm.keyByDataIdentity[firstIdentity])
			assert.NotContains(t, dm.keyByDataIdentity, secondIdentity)
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
	dm.keyByDataIdentity[requestedIdentity] = retainedDMV

	requestedDMV, err := dm.storeWithIdentity(requested)
	require.NoError(t, err)

	assert.NotSame(t, retainedDMV, requestedDMV)
	assert.Equal(t, requested, requestedDMV.data)
	assert.Same(t, requestedDMV, dm.keyByDataIdentity[requestedIdentity])
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
	assert.Same(t, dmv, dm.keyByDataIdentity[identity])
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

func TestDataMapCollisionComparisonPreservesFloatEncoding(t *testing.T) {
	dm := newDataMap()
	positiveZero := mmdbtype.Map{"value": mmdbtype.Float64(0)}
	negativeZero := mmdbtype.Map{"value": mmdbtype.Float64(math.Copysign(0, -1))}

	positiveValue := dm.storeByHash(positiveZero, 1)
	negativeValue := dm.storeByHash(negativeZero, 1)

	assert.NotSame(t, positiveValue, negativeValue)
}
