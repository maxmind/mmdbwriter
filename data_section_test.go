package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

func TestDisablingPointers(t *testing.T) {
	v := mmdbtype.Slice{
		mmdbtype.String("a repeated string"),
		mmdbtype.String("a repeated string"),
		mmdbtype.String("a repeated string"),
		mmdbtype.String("a repeated string"),
	}
	dm := newDataMap(newKeyWriter())

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
