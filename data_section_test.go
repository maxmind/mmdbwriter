package mmdbwriter

import (
	"bytes"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestDisablingPointers(t *testing.T) {
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
	defer pointerWriter.Close()

	_, err = pointerWriter.maybeWrite(ref)
	require.NoError(t, err)

	usePointers = false
	noPointerWriter := newDataWriter(store, usePointers)
	defer noPointerWriter.Close()
	_, err = noPointerWriter.maybeWrite(ref)
	require.NoError(t, err)

	assert.Less(t, pointerWriter.Len(), noPointerWriter.Len())
}

func TestSpoolSpillsAndRemovesTemporaryFile(t *testing.T) {
	spool := newSpool(t.TempDir())
	spool.threshold = 4

	_, err := spool.WriteString("abcdef")
	require.NoError(t, err)
	require.NotNil(t, spool.file)
	assert.Zero(t, cap(spool.buffer.Bytes()))
	path := spool.path
	_, err = os.Stat(path)
	require.NoError(t, err)

	var output bytes.Buffer
	_, err = spool.WriteTo(&output)
	require.NoError(t, err)
	assert.Equal(t, "abcdef", output.String())

	require.NoError(t, spool.Close())
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestSpoolWritesBufferedDataRepeatably(t *testing.T) {
	spool := newSpool(t.TempDir())
	_, err := spool.WriteString("buffered")
	require.NoError(t, err)

	for range 2 {
		var output bytes.Buffer
		_, err := spool.WriteTo(&output)
		require.NoError(t, err)
		assert.Equal(t, "buffered", output.String())
	}
}

func TestSpoolWriteErrorIsSticky(t *testing.T) {
	path := t.TempDir() + "/read-only"
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)

	spool := newSpool(t.TempDir())
	spool.file = file
	spool.path = path
	_, firstErr := spool.WriteString("cannot write")
	require.Error(t, firstErr)
	_, err = spool.WriteString("still cannot write")
	require.ErrorIs(t, err, firstErr)
	require.NoError(t, spool.Close())
}

func TestDataWriterRejectsOversizedOffset(t *testing.T) {
	store := newValueStore()
	ref, err := store.intern(mmdbtype.String("value"))
	require.NoError(t, err)
	defer store.release(ref)

	writer := newDataWriter(store, true)
	writer.size = int64(math.MaxUint32) + 1
	writer.threshold = math.MaxInt64
	_, err = writer.maybeWrite(ref)
	require.ErrorContains(t, err, "exceeds maximum")
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

func TestSpoolReportsInvalidScratchPath(t *testing.T) {
	spool := newSpool(t.TempDir() + "/missing")
	spool.threshold = 1

	_, err := spool.WriteString(strings.Repeat("x", 2))
	require.ErrorContains(t, err, "checking scratch path")
}
