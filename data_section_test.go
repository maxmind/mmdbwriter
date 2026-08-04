package mmdbwriter

import (
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
	dm := newDataMap()

	key, err := dm.store(v)
	require.NoError(t, err)

	usePointers := true
	pointerWriter := newDataWriter(dm, usePointers)

	_, err = pointerWriter.maybeWrite(key)
	require.NoError(t, err)

	usePointers = false
	noPointerWriter := newDataWriter(dm, usePointers)
	_, err = noPointerWriter.maybeWrite(key)
	require.NoError(t, err)

	assert.Less(t, pointerWriter.Len(), noPointerWriter.Len())
}

func TestDataWriterSharesNestedAndTopLevelOffsets(t *testing.T) {
	nested := mmdbtype.Map{
		"value": mmdbtype.String("shared"),
	}
	outer := mmdbtype.Map{"nested": nested}
	dm := newDataMap()
	outerValue, err := dm.store(outer)
	require.NoError(t, err)
	nestedValue, err := dm.store(nested.Copy())
	require.NoError(t, err)

	writer := newDataWriter(dm, true)
	_, err = writer.maybeWrite(outerValue)
	require.NoError(t, err)
	length := writer.Len()

	offset, err := writer.maybeWrite(nestedValue)
	require.NoError(t, err)
	assert.Positive(t, offset)
	assert.Equal(t, length, writer.Len(), "equal nested value was written twice")
}

func TestDataWriterResolvesHashCollisionsByExactValue(t *testing.T) {
	dm := newDataMap()
	first := dm.storeByHash(mmdbtype.String("first value"), 1)
	second := dm.storeByHash(mmdbtype.String("second value"), 1)
	writer := newDataWriter(dm, true)

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
		dw := newDataWriter(newDataMap(), true)
		value := mmdbtype.Uint16(1)

		firstSize, err := dw.WriteOrWritePointer(value)
		require.NoError(t, err)
		secondSize, err := dw.WriteOrWritePointer(value)
		require.NoError(t, err)

		assert.Equal(t, firstSize, secondSize,
			"a value no larger than a pointer should not become a pointer")
	})

	t.Run("a long value becomes a pointer", func(t *testing.T) {
		dw := newDataWriter(newDataMap(), true)
		value := mmdbtype.String("this string is comfortably longer than a pointer")

		firstSize, err := dw.WriteOrWritePointer(value)
		require.NoError(t, err)
		secondSize, err := dw.WriteOrWritePointer(value)
		require.NoError(t, err)

		assert.Less(t, secondSize, firstSize,
			"a repeated long value should become a pointer")
	})
}

// TestWriteOrWritePointerRejectsCollidingOffset pins the exact comparison that
// guards the nested-value pointer path. A hash match alone must not produce a
// pointer: without the comparison the writer would emit a pointer to different
// data, which no reader could detect.
func TestWriteOrWritePointerRejectsCollidingOffset(t *testing.T) {
	dw := newDataWriter(newDataMap(), true)

	value := mmdbtype.String("the value actually being written out here")
	other := mmdbtype.String("entirely different data sharing its bucket")

	hash, err := dw.dataMap.hasher.Hash(value)
	require.NoError(t, err)

	// Plant a colliding entry for different data, large enough that a pointer
	// would win on size if the exact comparison were skipped.
	dw.rememberOffset(hash, other, writtenType{
		pointer: mmdbtype.Pointer(0),
		size:    int64(len(other) + 2),
	})

	size, err := dw.WriteOrWritePointer(value)
	require.NoError(t, err)

	assert.Greater(t, size, mmdbtype.Pointer(0).WrittenSize(),
		"a colliding entry for different data produced a pointer")
	assert.Contains(t, dw.String(), string(value),
		"the value was not written out")
}

// TestWriteOrWritePointerStringMatchesGenericPath pins the two invariants the
// unboxed map key path depends on: it must hash the same as the generic path,
// and it must recognize a stored pointer form, or an equal value would stop
// sharing an offset.
func TestWriteOrWritePointerStringMatchesGenericPath(t *testing.T) {
	value := mmdbtype.String("a shared string worth pointing at")

	t.Run("hashes agree with the generic path", func(t *testing.T) {
		hasher := newDataHasher()

		generic, err := hasher.Hash(value)
		require.NoError(t, err)

		assert.Equal(t, generic, hasher.HashString(value))
	})

	t.Run("a stored pointer form is reused", func(t *testing.T) {
		dw := newDataWriter(newDataMap(), true)

		pointerForm := value
		_, err := dw.WriteOrWritePointer(&pointerForm)
		require.NoError(t, err)

		size, err := dw.WriteOrWritePointerString(value)
		require.NoError(t, err)

		assert.Equal(t, mmdbtype.Pointer(0).WrittenSize(), size,
			"the map key did not reuse the offset written for the pointer form")
	})
}
