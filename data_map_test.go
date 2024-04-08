package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/mmdbtype"
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
