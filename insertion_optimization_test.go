package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestMapShapesSurviveChurnAndCollisions(t *testing.T) {
	store := newValueStoreWithHash(func([]byte) uint64 { return 1 })
	var refs []valueRef
	external := map[valueRef]uint64{}
	for index := range 100 {
		value := mmdbtype.Map{
			"a": mmdbtype.Uint32(index),
			mmdbtype.String(string(rune('b' + index%8))): mmdbtype.Map{
				"a": mmdbtype.String("nested"), "b": mmdbtype.Bool(true),
			},
		}
		ref, err := store.intern(value)
		require.NoError(t, err)
		external[ref]++
		refs = append(refs, ref)
		require.True(t, value.Equal(store.materialize(ref)))
		_, err = store.intern(mmdbtype.Map{"a": nil, "b": mmdbtype.String("invalid")})
		require.Error(t, err)
		if len(refs) > 4 {
			old := refs[0]
			refs = refs[1:]
			external[old]--
			if external[old] == 0 {
				delete(external, old)
			}
			store.release(old)
		}
		require.NoError(t, store.audit(external))
	}
	for _, ref := range refs {
		store.release(ref)
	}
	require.NoError(t, store.audit(nil))
	for _, shape := range store.mapShapes {
		require.Zero(t, shape.ref)
	}
}
