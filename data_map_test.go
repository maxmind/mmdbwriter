package mmdbwriter

import (
	"math"
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
