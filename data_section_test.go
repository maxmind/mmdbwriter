package mmdbwriter

import (
	"bytes"
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

func TestSpoolReportsInvalidScratchPath(t *testing.T) {
	spool := newSpool(t.TempDir() + "/missing")
	spool.threshold = 1

	_, err := spool.WriteString(strings.Repeat("x", 2))
	require.ErrorContains(t, err, "checking scratch path")
}
