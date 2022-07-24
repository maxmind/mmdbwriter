package mmdbwriter

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

func TestDataMap(t *testing.T) {
	v := mmdbtype.String("test")

	dm := newDataMap()

	dmv, err := dm.store(v)
	require.NoError(t, err)

	valueKey, _ := dm.keyWriter.key(v)

	assert.Equal(
		t,
		&dataMapValue{
			data:     v,
			key:      dataMapKey(valueKey),
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

func TestMapValueAlignment(t *testing.T) {
	t.Logf("Alignment of dataMapValue: %d", unsafe.Alignof(dataMapValue{}))
	t.Logf("Size of dataMapValue: %d", unsafe.Sizeof(dataMapValue{}))
}
