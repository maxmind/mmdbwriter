package mmdbwriter

import (
	"bytes"
	"testing"

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
			mmdbtype.String("this string is comfortably longer than a pointer"))
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

func TestWriteContainerHeaderSizeBoundaries(t *testing.T) {
	for _, test := range []struct {
		name string
		kind valueKind
		size int
		want []byte
	}{
		{"map inline", valueKindMap, 28, []byte{0xfc}},
		{"map one byte start", valueKindMap, 29, []byte{0xfd, 0x00}},
		{"map one byte end", valueKindMap, 284, []byte{0xfd, 0xff}},
		{"map two byte start", valueKindMap, 285, []byte{0xfe, 0x00, 0x00}},
		{"map two byte end", valueKindMap, 65820, []byte{0xfe, 0xff, 0xff}},
		{"map three byte start", valueKindMap, 65821, []byte{0xff, 0x00, 0x00, 0x00}},
		{"map three byte end", valueKindMap, 16843036, []byte{0xff, 0xff, 0xff, 0xff}},
		{"slice extended type", valueKindSlice, 29, []byte{0x1d, 0x04, 0x00}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, writeContainerHeader(&output, test.kind, test.size))
			assert.Equal(t, test.want, output.Bytes())
		})
	}

	var output bytes.Buffer
	err := writeContainerHeader(&output, valueKindMap, 16843037)
	require.ErrorContains(t, err, "cannot store")
}
