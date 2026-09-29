package mmdbwriter

import (
	"bytes"
	"fmt"
	"net/netip"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestDisablingPointers(t *testing.T) {
	// The repeated value must be larger than its pointer for enabling pointers
	// to reduce the encoded size.
	v := mmdbtype.Slice{
		mmdbtype.String("a repeated string"),
		mmdbtype.String("a repeated string"),
		mmdbtype.String("a repeated string"),
		mmdbtype.String("a repeated string"),
	}
	store := newValueStore()
	ref, err := store.intern(v)
	require.NoError(t, err)
	defer store.release(ref)

	usePointers := true
	pointerWriter := newDataWriter(store, usePointers)

	_, err = pointerWriter.maybeWrite(ref)
	require.NoError(t, err)

	usePointers = false
	noPointerWriter := newDataWriter(store, usePointers)
	_, err = noPointerWriter.maybeWrite(ref)
	require.NoError(t, err)

	assert.Less(t, pointerWriter.Len(), noPointerWriter.Len())
}

func TestDataWriterSharesNestedAndTopLevelOffsets(t *testing.T) {
	nested := mmdbtype.Map{
		"value": mmdbtype.String("shared"),
	}
	outer := mmdbtype.Map{"nested": nested}
	store := newValueStore()
	outerRef, err := store.intern(outer)
	require.NoError(t, err)
	defer store.release(outerRef)
	nestedRef, err := store.intern(nested.Copy())
	require.NoError(t, err)
	defer store.release(nestedRef)

	writer := newDataWriter(store, true)
	_, err = writer.maybeWrite(outerRef)
	require.NoError(t, err)
	length := writer.Len()

	offset, err := writer.maybeWrite(nestedRef)
	require.NoError(t, err)
	assert.Positive(t, offset)
	assert.Equal(t, length, writer.Len(), "equal nested value was written twice")
}

func TestDataWriterKeepsCollidingRefsSeparate(t *testing.T) {
	store := newValueStoreWithHash(func([]byte) uint64 { return 1 })
	first, err := store.intern(mmdbtype.String("first value"))
	require.NoError(t, err)
	defer store.release(first)
	second, err := store.intern(mmdbtype.String("second value"))
	require.NoError(t, err)
	defer store.release(second)
	writer := newDataWriter(store, true)

	firstOffset, err := writer.maybeWrite(first)
	require.NoError(t, err)
	firstLength := writer.Len()
	secondOffset, err := writer.maybeWrite(second)
	require.NoError(t, err)

	assert.NotEqual(t, firstOffset, secondOffset)
	assert.Greater(t, writer.Len(), firstLength)

	length := writer.Len()
	duplicateOffset, err := writer.maybeWrite(first)
	require.NoError(t, err)
	assert.Equal(t, firstOffset, duplicateOffset)
	assert.Equal(t, length, writer.Len())
}

// TestDataWriterOnlyPointersWhenSmaller pins the size heuristic: a repeated
// value is only replaced by a pointer when the pointer is the smaller of the
// two encodings.
func TestDataWriterOnlyPointersWhenSmaller(t *testing.T) {
	t.Run("a short value is written inline again", func(t *testing.T) {
		store := newValueStore()
		ref, err := store.intern(mmdbtype.Uint16(1))
		require.NoError(t, err)
		defer store.release(ref)
		dw := newDataWriter(store, true)

		firstSize, err := dw.writeOrWritePointer(ref)
		require.NoError(t, err)
		secondSize, err := dw.writeOrWritePointer(ref)
		require.NoError(t, err)

		assert.Equal(t, firstSize, secondSize,
			"a value no larger than a pointer should not become a pointer")
	})

	t.Run("a long value becomes a pointer", func(t *testing.T) {
		store := newValueStore()
		ref, err := store.intern(
			mmdbtype.String("this string is comfortably longer than a pointer"),
		)
		require.NoError(t, err)
		defer store.release(ref)
		dw := newDataWriter(store, true)

		firstSize, err := dw.writeOrWritePointer(ref)
		require.NoError(t, err)
		secondSize, err := dw.writeOrWritePointer(ref)
		require.NoError(t, err)

		assert.Less(t, secondSize, firstSize,
			"a repeated long value should become a pointer")
	})
}

// TestWriterInterfaceMethodsBypassTheStore pins that the mmdbtype writer
// interface methods write values in full without interning them or recording
// offsets. An offset for a reference the caller releases could be recycled
// and then point at unrelated data.
func TestWriterInterfaceMethodsBypassTheStore(t *testing.T) {
	store := newValueStore()
	dw := newDataWriter(store, true)
	value := mmdbtype.String("this string is comfortably longer than a pointer")

	first, err := dw.WriteOrWritePointer(value)
	require.NoError(t, err)
	second, err := dw.WriteOrWritePointer(value)
	require.NoError(t, err)
	third, err := dw.WriteOrWritePointerString(value)
	require.NoError(t, err)

	assert.Equal(t, first, second, "a repeated value must be written in full")
	assert.Equal(t, first, third)
	assert.Zero(t, liveValueNodeCount(store),
		"the interface methods must not touch the store")
}

func TestWriteToPutsSharedRecordValuesFirst(t *testing.T) {
	tree, err := New(Options{
		DatabaseType:            "Test",
		Description:             map[string]string{"en": "Test"},
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)
	// The tree walk reaches this record first, but it shares nothing.
	lone := mmdbtype.Map{"lone": mmdbtype.String("a value that no other record holds")}
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/8"), lone))
	hot := mmdbtype.Map{"hot": mmdbtype.String("a value that many records hold")}
	for i := range 4 {
		prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{2, byte(i), 0, 0}), 16)
		value := mmdbtype.Map{"id": mmdbtype.Uint32(i), "shared": hot}
		require.NoError(t, tree.Insert(prefix, value))
	}

	reader, err := maxminddb.OpenBytes(writeTreeBytes(t, tree))
	require.NoError(t, err)
	defer reader.Close()
	require.NoError(t, reader.Verify())

	loneResult := reader.Lookup(netip.MustParseAddr("1.0.0.0"))
	// The first shared record writes the shared values, so it moves ahead.
	sharedResult := reader.Lookup(netip.MustParseAddr("2.0.0.0"))
	assert.Less(t, sharedResult.Offset(), loneResult.Offset())

	var loneValue string
	require.NoError(t, loneResult.DecodePath(&loneValue, "lone"))
	assert.Equal(t, "a value that no other record holds", loneValue)
	var hotValue string
	require.NoError(t, sharedResult.DecodePath(&hotValue, "shared", "hot"))
	assert.Equal(t, "a value that many records hold", hotValue)
}

func TestWriteContainerHeaderSizeBoundaries(t *testing.T) {
	for _, test := range []struct {
		name string
		kind valueKind
		size int
		want []byte
	}{
		{"map empty", valueKindMap, 0, []byte{0xe0}},
		{"map inline", valueKindMap, 28, []byte{0xfc}},
		{"map one byte start", valueKindMap, 29, []byte{0xfd, 0x00}},
		{"map one byte end", valueKindMap, 284, []byte{0xfd, 0xff}},
		{"map two byte start", valueKindMap, 285, []byte{0xfe, 0x00, 0x00}},
		{"map two byte end", valueKindMap, 65820, []byte{0xfe, 0xff, 0xff}},
		{"map three byte start", valueKindMap, 65821, []byte{0xff, 0x00, 0x00, 0x00}},
		{"map three byte end", valueKindMap, 16843036, []byte{0xff, 0xff, 0xff, 0xff}},
		{"slice inline", valueKindSlice, 0, []byte{0x00, 0x04}},
		{"slice inline end", valueKindSlice, 28, []byte{0x1c, 0x04}},
		{"slice extended type", valueKindSlice, 29, []byte{0x1d, 0x04, 0x00}},
		{"slice one byte end", valueKindSlice, 284, []byte{0x1d, 0x04, 0xff}},
		{"slice two byte start", valueKindSlice, 285, []byte{0x1e, 0x04, 0x00, 0x00}},
		{"slice two byte end", valueKindSlice, 65820, []byte{0x1e, 0x04, 0xff, 0xff}},
		{"slice three byte start", valueKindSlice, 65821, []byte{0x1f, 0x04, 0x00, 0x00, 0x00}},
		{"slice three byte end", valueKindSlice, 16843036, []byte{0x1f, 0x04, 0xff, 0xff, 0xff}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, writeContainerHeader(&output, test.kind, test.size))
			assert.Equal(t, test.want, output.Bytes())
			assert.Equal(t, uint64(len(test.want)), containerHeaderSize(test.kind, test.size))
		})
	}

	// The writer rejects a size that is too large. The size estimate then
	// gives the largest header.
	for kind, largest := range map[valueKind]uint64{valueKindMap: 4, valueKindSlice: 5} {
		var output bytes.Buffer
		err := writeContainerHeader(&output, kind, 16843037)
		require.ErrorContains(t, err, "cannot store")
		assert.Equal(t, largest, containerHeaderSize(kind, 16843037))
	}
}

// A record value that an earlier record already wrote nested gets that nested
// offset. The output must still pass Verify and give the inserted values.
func TestWriteToRecordValueNestedInEarlierRecord(t *testing.T) {
	type insert struct {
		network string
		value   mmdbtype.DataType
	}
	hot := mmdbtype.Map{"hot": mmdbtype.String("a value that many records hold")}
	// The small value has no gain, so the record that holds it nested beside
	// hot comes first.
	smallValue := func(value mmdbtype.DataType) []insert {
		inserts := []insert{
			{"1.0.0.0/8", value},
			{"2.0.0.0/8", mmdbtype.Map{"e": value, "shared": hot}},
		}
		for i := range 4 {
			inserts = append(inserts, insert{
				fmt.Sprintf("3.%d.0.0/16", i),
				mmdbtype.Map{"id": mmdbtype.Uint16(i), "shared": hot},
			})
		}
		return inserts
	}
	inner := mmdbtype.Map{"x": mmdbtype.String("some long value here")}
	root := mmdbtype.Map{"v": mmdbtype.String("abcdef")}
	for _, test := range []struct {
		name    string
		inserts []insert
	}{
		{"nested first in tree order", []insert{
			{"1.0.0.0/24", mmdbtype.Map{"a": inner}},
			{"2.0.0.0/24", inner},
		}},
		{"empty map", smallValue(mmdbtype.Map{})},
		{"empty slice", smallValue(mmdbtype.Slice{})},
		{"small slice", smallValue(mmdbtype.Slice{mmdbtype.Uint16(0)})},
		{"short string", smallValue(mmdbtype.String("US"))},
		{"small integer", smallValue(mmdbtype.Uint16(0))},
		// Both records tie with no gain, and the holder has the lower address.
		{"tie", []insert{
			{"1.0.0.0/10", mmdbtype.Map{"r": root}},
			{"1.128.0.0/9", root},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tree, err := New(Options{
				DatabaseType:            "Test",
				Description:             map[string]string{"en": "Test"},
				IPVersion:               4,
				IncludeReservedNetworks: true,
			})
			require.NoError(t, err)
			for _, insert := range test.inserts {
				require.NoError(t, tree.Insert(netip.MustParsePrefix(insert.network), insert.value))
			}

			reader, err := maxminddb.OpenBytes(writeTreeBytes(t, tree))
			require.NoError(t, err)
			defer reader.Close()
			require.NoError(t, reader.Verify())
			loaded, err := Load(writeTempDB(t, tree), Options{IncludeReservedNetworks: true})
			require.NoError(t, err)
			for _, insert := range test.inserts {
				requireTreeLookup(t, loaded, tree, netip.MustParsePrefix(insert.network).Addr())
			}
		})
	}
}
