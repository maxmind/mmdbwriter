package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestRecordOrdererGainAndSize(t *testing.T) {
	// Encoded sizes: the keys take 2 bytes each, short takes 3, long takes 7,
	// an empty map takes 1, and an empty slice takes 2.
	short := mmdbtype.String("ab")
	long := mmdbtype.String("abcdef")
	root := mmdbtype.Map{"v": long}
	for _, test := range []struct {
		name    string
		records []mmdbtype.DataType
		// want holds the gain and size of each record, in visit order.
		want [][2]uint64
	}{
		{
			name: "values no larger than a pointer",
			records: []mmdbtype.DataType{
				mmdbtype.Map{"l": long, "s": short},
				mmdbtype.Map{"l": long, "s": short, "t": short},
			},
			want: [][2]uint64{
				// Only long gets a pointer use: the second record holds it.
				{1, 1 + 2 + 7 + 2 + 3},
				// A repeated long costs a pointer. A repeated key or short costs
				// its own size, because it is no larger than a pointer.
				{0, 1 + 2 + 3 + 2 + 3 + 2 + 3},
			},
		},
		{
			name: "root written first",
			records: []mmdbtype.DataType{
				root,
				mmdbtype.Map{"r": root},
				mmdbtype.Map{"q": root},
			},
			want: [][2]uint64{
				// Both container slots can point to the top-level copy.
				{2, 1 + 2 + 7},
				{0, 1 + 2 + 3},
				{0, 1 + 2 + 3},
			},
		},
		{
			name: "root written nested first",
			records: []mmdbtype.DataType{
				mmdbtype.Map{"r": root},
				root,
				mmdbtype.Map{"q": root},
			},
			want: [][2]uint64{
				// The nested copy uses one slot, and the other slot points to it.
				{1, 1 + 2 + 1 + 2 + 7},
				// The tree record points to the nested copy.
				{0, 0},
				{0, 1 + 2 + 3},
			},
		},
		{
			name: "empty containers",
			records: []mmdbtype.DataType{
				mmdbtype.Map{"e": mmdbtype.Map{}, "s": mmdbtype.Slice{}},
				mmdbtype.Map{"e": mmdbtype.Map{}, "s": mmdbtype.Slice{}, "x": long},
			},
			want: [][2]uint64{
				// Neither container is larger than a pointer, so neither gets a
				// pointer use, and each repeat costs its own size.
				{0, 1 + 2 + 1 + 2 + 2},
				{0, 1 + 2 + 1 + 2 + 2 + 2 + 7},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newValueStore()
			refs := make([]valueRef, len(test.records))
			for i, record := range test.records {
				ref, err := store.intern(record)
				require.NoError(t, err)
				t.Cleanup(func() { store.release(ref) })
				refs[i] = ref
			}
			// Each interned record stands for one data record in the tree.
			treeRefs := make([]uint32, len(store.nodes))
			for _, ref := range refs {
				treeRefs[ref]++
			}

			o := recordOrderer{
				store:    store,
				treeRefs: treeRefs,
				cost:     make([]uint8, len(store.nodes)),
			}
			for i, ref := range refs {
				o.gain, o.size = 0, 0
				o.visit(ref, true)
				got := [2]uint64{o.gain, o.size}
				assert.Equal(t, test.want[i], got, "record %d", i)
			}
		})
	}
}

func TestOrderRecordValues(t *testing.T) {
	store := newValueStore()
	intern := func(value mmdbtype.DataType) valueRef {
		ref, err := store.intern(value)
		require.NoError(t, err)
		t.Cleanup(func() { store.release(ref) })
		return ref
	}
	x := mmdbtype.String("shared twice, x")
	y := mmdbtype.String("shared twice, y")
	z := mmdbtype.String("shared 3 times")
	// These containers are not records. Each one adds a container slot, so
	// the records that hold x, y, and z get a gain.
	intern(mmdbtype.Map{"hx": x})
	intern(mmdbtype.Map{"hy": y})
	intern(mmdbtype.Map{"hz1": z})
	intern(mmdbtype.Map{"hz2": z})

	// Records in first-seen order. b and d tie. e has the highest score. The
	// 4 records with no gain outnumber the 3 with a gain, so moving them to
	// the end copies between overlapping ranges.
	a := intern(mmdbtype.String("record a, no gain"))
	b := intern(mmdbtype.Map{"b": x})
	c := intern(mmdbtype.String("record c, no gain"))
	d := intern(mmdbtype.Map{"d": y})
	f := intern(mmdbtype.String("record f, no gain"))
	e := intern(mmdbtype.Map{"e": z})
	g := intern(mmdbtype.String("record g, no gain"))
	records := []valueRef{a, b, c, d, f, e, g}
	treeRefs := make([]uint32, len(store.nodes))
	for _, ref := range records {
		treeRefs[ref]++
	}

	orderRecordValues(store, records, treeRefs)
	// Records with a gain come first, by score, and ties keep first-seen
	// order. The records with no gain follow in first-seen order.
	assert.Equal(t, []valueRef{e, b, d, a, c, f, g}, records)
}
