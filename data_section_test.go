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

func TestSpoolReportsInvalidScratchPath(t *testing.T) {
	spool := newSpool(t.TempDir() + "/missing")
	spool.threshold = 1

	_, err := spool.WriteString(strings.Repeat("x", 2))
	require.ErrorContains(t, err, "checking scratch path")
}
