package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestDataMap(t *testing.T) {
	v := mmdbtype.String("test")

	dm := newDataMap(newKeyWriter())

	dmv, err := dm.store(v)
	require.NoError(t, err)

	assert.Equal(
		t,
		&dataMapValue{
			data: v,
			key: "\x87\x02\xf53\x8b\x96\xfdǻQ\x97\x9c\xe2\xcc\\\xda\xf2\xb1\xd7" +
				"\xc1L\xc5l\xfd\x83\xfc\x97\xd6\x03\xf5\xedr",
			refCount: 1,
		},
		dmv,
	)

	mapDMV := dm.data[dmv.key]

	assert.Equal(t, dmv, mapDMV)

	dmv, err = dm.store(v)
	require.NoError(t, err)

	assert.Equal(t, uint32(2), dmv.refCount, "refCount incremented on store")

	dm.remove(dmv)

	mapDMV = dm.data[dmv.key]

	assert.Equal(t, uint32(1), mapDMV.refCount, "refCount decremented on remove")

	dm.remove(dmv)
	_, ok := dm.data[dmv.key]
	assert.False(t, ok, "map value removed when refCount drops to 0")
}

func TestDataMapCachesDefaultComplexValueIdentity(t *testing.T) {
	v := mmdbtype.Map{
		"test": mmdbtype.String("value"),
	}

	identity, ok := keyIdentity(v)
	require.True(t, ok)

	dm := newDataMap(newKeyWriter())

	dmv, err := dm.storeWithIdentity(v)
	require.NoError(t, err)

	assert.Equal(t, dmv.key, dm.keyByDataIdentity[identity])

	sameDMV, err := dm.storeWithIdentity(v)
	require.NoError(t, err)

	assert.Same(t, dmv, sameDMV, "same value identity returns same dataMapValue")
	assert.Equal(t, uint32(2), dmv.refCount)

	dm.remove(dmv)
	assert.Equal(t, dmv.key, dm.keyByDataIdentity[identity])

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

			dm := newDataMap(newKeyWriter())
			dmv, err := dm.storeWithIdentity(first)
			require.NoError(t, err)

			sameDMV, err := dm.storeWithIdentity(second)
			require.NoError(t, err)

			assert.Same(t, dmv, sameDMV, "equal content returns retained dataMapValue")
			assert.Equal(t, dmv.key, dm.keyByDataIdentity[firstIdentity])
			assert.NotContains(t, dm.keyByDataIdentity, secondIdentity)
		})
	}
}

func TestDataMapDoesNotUseStaleIdentityForMutatedNonRetainedValue(t *testing.T) {
	first := mmdbtype.Bytes{1, 2, 3}
	second := mmdbtype.Bytes{1, 2, 3}

	dm := newDataMap(newKeyWriter())
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

func TestDataMapDoesNotCacheCustomKeyGenerator(t *testing.T) {
	v := mmdbtype.Map{
		"test": mmdbtype.String("value"),
	}
	keyGenerator := newCountingKeyGenerator()

	dm := newDataMap(keyGenerator)
	require.Nil(t, dm.keyByDataIdentity)

	_, err := dm.storeWithIdentity(v)
	require.NoError(t, err)

	_, err = dm.storeWithIdentity(v)
	require.NoError(t, err)

	assert.Equal(t, 2, keyGenerator.calls)
}

type countingKeyGenerator struct {
	keyWriter *keyWriter
	calls     int
}

func newCountingKeyGenerator() *countingKeyGenerator {
	return &countingKeyGenerator{keyWriter: newKeyWriter()}
}

func (kg *countingKeyGenerator) Key(v mmdbtype.DataType) ([]byte, error) {
	kg.calls++
	return kg.keyWriter.Key(v)
}
