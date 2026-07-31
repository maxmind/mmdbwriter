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
