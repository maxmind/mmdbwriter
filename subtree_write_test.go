package mmdbwriter

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteSubtreePartialWrite(t *testing.T) {
	for _, size := range []int{24, 28, 32} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			tree := subtreeTestTree(t, Options{IPVersion: 4, RecordSize: size})
			before := writeTreeBytes(t, tree)
			recordBytes := size / 4
			failure := errors.New("partial node write")
			writer := &subtreePartialWriter{remaining: recordBytes + recordBytes/2, err: failure}
			next := uint32(0)
			written, err := tree.writeSubtree(
				writer,
				tree.root,
				newDataWriter(tree.valueStore, true),
				make([]byte, recordBytes),
				&next,
			)
			require.ErrorIs(t, err, failure)
			require.EqualError(t, err, "writing node: partial node write")
			require.EqualValues(t, recordBytes+recordBytes/2, written)
			require.EqualValues(t, 1, next, "the partially written node must not advance numbering")
			require.Equal(
				t,
				before,
				writeTreeBytes(t, tree),
				"retry must start with fresh writer state",
			)
		})
	}
}

type subtreePartialWriter struct {
	err       error
	remaining int
}

func (w *subtreePartialWriter) Write(p []byte) (int, error) {
	if len(p) <= w.remaining {
		w.remaining -= len(p)
		return len(p), nil
	}
	written := w.remaining
	w.remaining = 0
	return written, w.err
}

func TestWriteSubtreeNodeCountMismatch(t *testing.T) {
	tree := subtreeTestTree(t, Options{IPVersion: 4})
	tree.finalize()
	writtenNodes := tree.nodeCount
	tree.nodeCount++
	written, err := tree.WriteTo(io.Discard)
	require.EqualError(t, err, fmt.Sprintf(
		"number of nodes written (%d) doesn't match number expected (%d)",
		writtenNodes,
		tree.nodeCount,
	))
	require.EqualValues(t, writtenNodes*tree.recordSize/4, written)
}
