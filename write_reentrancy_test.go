package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestWriteToRejectsInsertRegression(t *testing.T) {
	tree := newWriteReentrancyTree(t, 2000)
	var nestedErr, writeErr error
	called := false
	require.NotPanics(t, func() {
		_, writeErr = tree.WriteTo(reentrantWriteFunc(func(p []byte) (int, error) {
			if !called {
				called = true
				nestedErr = tree.Insert(netip.MustParsePrefix("9.9.9.9/32"), mmdbtype.Uint32(9))
			}
			return len(p), nil
		}))
	})
	require.True(t, called)
	require.ErrorIs(t, nestedErr, errReentrantMutation)
	require.NoError(t, writeErr)
	require.NoError(t, tree.auditValueStore())
}

func TestWriteToRejectsReentrantMutation(t *testing.T) {
	for _, count := range []int{1, 2000} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("networks=%d/cached=%t", count, cached), func(t *testing.T) {
				tree := newWriteReentrancyTree(t, count)
				control := newWriteReentrancyTree(t, count)
				expected := writeTreeBytes(t, control)
				if cached {
					require.Equal(t, expected, writeTreeBytes(t, tree))
				}
				other := newReentrancyTree(t, 4)
				target := netip.MustParsePrefix("9.9.9.9/32")
				var output bytes.Buffer
				calls := 0
				n, err := tree.WriteTo(reentrantWriteFunc(func(p []byte) (int, error) {
					calls++
					require.True(t, tree.mutating)
					prefix, value := tree.Get(netip.MustParseAddr("1.0.0.1"))
					require.Equal(t, netip.MustParsePrefix("1.0.0.0/24"), prefix)
					require.Equal(t, mmdbtype.Uint32(0), value)
					nodes := slices.Clone(tree.valueStore.nodes)
					numbers := slices.Clone(tree.nodeNumbers)
					cursor := tree.insertCursor
					allocated := tree.nodeCountAllocated
					paths := slices.Clone(tree.paths)
					for operation := range byte(11) {
						require.Error(t, rejectReentrantMutation(t, tree, target, operation))
					}
					// A valid prefix with an invalid value must be rejected before interning.
					require.ErrorIs(
						t,
						tree.Insert(target, mmdbtype.Pointer(1)),
						errReentrantMutation,
					)
					require.Equal(t, nodes, tree.valueStore.nodes)
					require.Equal(t, numbers, tree.nodeNumbers)
					require.Equal(t, cursor, tree.insertCursor)
					require.Equal(t, allocated, tree.nodeCountAllocated)
					require.Equal(t, paths, tree.paths)
					require.True(
						t,
						tree.mutating,
						"nested rejection must not release the outer guard",
					)
					require.NoError(t, other.Insert(target, mmdbtype.Uint32(9)))
					_, otherErr := other.WriteTo(io.Discard)
					require.NoError(t, otherErr)
					return output.Write(p)
				}))
				require.NoError(t, err)
				require.EqualValues(t, len(expected), n)
				require.Equal(t, expected, output.Bytes())
				if count == 1 {
					require.Equal(
						t,
						1,
						calls,
						"small trees reach the writer only at the final flush",
					)
				} else {
					require.Greater(t, calls, 1, "large trees must flush during traversal")
				}
				require.False(t, tree.mutating)
				requireReentrancyTreesEqual(t, tree, control, target.Addr())
				reader, err := maxminddb.OpenBytes(output.Bytes())
				require.NoError(t, err)
				defer reader.Close()
				require.NoError(t, reader.Verify())
				for _, candidate := range []*Tree{tree, control} {
					require.NoError(t, candidate.Insert(target, mmdbtype.Uint32(9)))
				}
				requireReentrancyTreesEqual(t, tree, control, target.Addr())
			})
		}
	}
}

func TestWriteToMutationGuardClearsAfterFailure(t *testing.T) {
	for _, count := range []int{1, 2000} {
		for _, outcome := range []string{"nested-error", "partial-error", "short-write", "panic"} {
			t.Run(fmt.Sprintf("networks=%d/%s", count, outcome), func(t *testing.T) {
				tree := newWriteReentrancyTree(t, count)
				control := newWriteReentrancyTree(t, count)
				expected := writeTreeBytes(t, control)
				failure := errors.New("writer failure")
				target := netip.MustParsePrefix("9.9.9.9/32")
				var output bytes.Buffer
				calls := 0
				write := func() error {
					_, err := tree.WriteTo(reentrantWriteFunc(func(p []byte) (int, error) {
						calls++
						require.True(t, tree.mutating)
						nestedErr := rejectReentrantMutation(t, tree, target, 0)
						// A panic triggers the deferred flush as well. That call must
						// still be guarded, but let it finish so the original panic survives.
						if outcome == "panic" {
							if calls == 1 {
								panic(failure)
							}
							return output.Write(p)
						}
						if outcome == "nested-error" {
							return 0, nestedErr
						}
						n, err := output.Write(p[:len(p)/2])
						require.NoError(t, err)
						if outcome == "partial-error" {
							return n, failure
						}
						return n, nil
					}))
					return err
				}
				switch outcome {
				case "panic":
					require.PanicsWithValue(t, failure, func() { require.NoError(t, write()) })
					require.Equal(
						t,
						2,
						calls,
						"guard must cover the deferred flush during unwinding",
					)
				case "nested-error":
					require.ErrorIs(t, write(), errReentrantMutation)
				case "partial-error":
					require.ErrorIs(t, write(), failure)
				case "short-write":
					require.ErrorIs(t, write(), io.ErrShortWrite)
				}
				require.Positive(t, calls)
				require.False(t, tree.mutating)
				require.NoError(t, tree.auditValueStore())
				if output.Len() != 0 {
					require.Equal(t, expected[:output.Len()], output.Bytes())
				}
				require.Equal(
					t,
					expected,
					writeTreeBytes(t, tree),
					"retry must write the original tree",
				)
				for _, candidate := range []*Tree{tree, control} {
					require.NoError(t, candidate.Insert(target, mmdbtype.Uint32(9)))
				}
				requireReentrancyTreesEqual(t, tree, control, target.Addr())
			})
		}
	}
}

func newWriteReentrancyTree(t *testing.T, count int) *Tree {
	t.Helper()
	tree := newReentrancyTree(t, 4)
	for i := range count {
		prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{1, byte(i >> 8), byte(i), 0}), 24)
		require.NoError(t, tree.Insert(prefix, mmdbtype.Uint32(i)))
	}
	return tree
}

type reentrantWriteFunc func([]byte) (int, error)

func (f reentrantWriteFunc) Write(p []byte) (int, error) {
	return f(p)
}
