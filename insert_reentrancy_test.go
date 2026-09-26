package mmdbwriter

import (
	"fmt"
	"net/netip"
	"slices"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func newReentrancyTree(t *testing.T, ipVersion int) *Tree {
	t.Helper()
	tree, err := New(Options{
		BuildEpoch:              123,
		DatabaseType:            "reentrancy-test",
		Description:             map[string]string{"en": "Reentrancy test"},
		IPVersion:               ipVersion,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)
	return tree
}

// Each callback entry point must keep the guard set until all its work ends.
// The pure callbacks intentionally violate their contract to test rejection.
func insertReentrancyCallback(
	t *testing.T, tree *Tree, prefix netip.Prefix, method string, callback inserter.Func,
) error {
	t.Helper()
	pure := func(existing, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
		return callback(existing, newValue, inserter.Metadata{})
	}
	start := prefix.Addr()
	switch method {
	case "InsertFunc":
		return tree.InsertFunc(prefix, nil, callback)
	case "InsertPureFunc":
		return tree.InsertPureFunc(prefix, nil, pure)
	case "InsertRangeFunc":
		return tree.InsertRangeFunc(start, start.Next().Next(), nil, callback)
	case "InsertRangePureFunc":
		return tree.InsertRangePureFunc(start, start.Next().Next(), nil, pure)
	case "Insert", "InsertRange":
		tree.inserter = pure
		defer func() { tree.inserter = nil }()
		if method == "Insert" {
			return tree.Insert(prefix, nil)
		}
		return tree.InsertRange(start, start.Next().Next(), nil)
	case "insertNormalizedRef":
		ref, err := tree.valueStore.intern(mmdbtype.Uint32(99))
		require.NoError(t, err)
		defer tree.valueStore.release(ref)
		return tree.insertNormalizedRef(prefix, pure, ref)
	default:
		t.Fatalf("unknown insertion method %s", method)
		return nil
	}
}

type reentrancyWriter struct{ calls int }

func (w *reentrancyWriter) Write(p []byte) (int, error) {
	w.calls++
	return len(p), nil
}

// Attempt every mutation entry point, including the reference-based Load path.
// Callback counters and invalid inputs establish that rejection precedes
// callback invocation, normalization, interning, and reference retention.
func rejectReentrantMutation(t *testing.T, tree *Tree, prefix netip.Prefix, operation byte) error {
	t.Helper()
	calls := 0
	pure := func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
		calls++
		return mmdbtype.Uint32(6), nil
	}
	callback := func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
		calls++
		return mmdbtype.Uint32(6), nil
	}
	var err error
	want := errNestedInsert
	switch operation % 11 {
	case 0:
		err = tree.Insert(prefix, mmdbtype.Uint32(6))
	case 1:
		err = tree.InsertFunc(prefix, nil, callback)
	case 2:
		err = tree.InsertPureFunc(prefix, nil, pure)
	case 3:
		err = tree.InsertRange(prefix.Addr(), prefix.Addr().Next(), mmdbtype.Uint32(6))
	case 4:
		err = tree.InsertRangeFunc(prefix.Addr(), prefix.Addr().Next(), nil, callback)
	case 5:
		err = tree.InsertRangePureFunc(prefix.Addr(), prefix.Addr().Next(), nil, pure)
	case 6:
		err = tree.InsertPureFunc(prefix, nil, inserter.Remove)
	case 7:
		// An invalid handle must never reach retain.
		err = tree.insertNormalizedRef(prefix, pure, valueRef(0x7fffffff))
	case 8:
		err = tree.Insert(netip.Prefix{}, mmdbtype.Pointer(1))
	case 9:
		err = tree.InsertRange(netip.Addr{}, netip.Addr{}, mmdbtype.Pointer(1))
	case 10:
		writer := &reentrancyWriter{}
		var n int64
		n, err = tree.WriteTo(writer)
		require.Zero(t, n)
		require.Zero(t, writer.calls)
		want = errWriteDuringInsert
	}
	require.ErrorIs(t, err, want)
	require.Equal(t, want, err, "a nested call must not audit the unfinished outer insert")
	require.Zero(t, calls)
	return err
}

func TestRejectReentrantMutationEntryPoints(t *testing.T) {
	for _, method := range []string{
		"Insert", "InsertFunc", "InsertPureFunc", "InsertRange",
		"InsertRangeFunc", "InsertRangePureFunc", "insertNormalizedRef",
	} {
		t.Run(method, func(t *testing.T) {
			tree := newReentrancyTree(t, 4)
			start := netip.MustParseAddr("1.2.3.0")
			for i := range 3 {
				require.NoError(t, tree.Insert(netip.PrefixFrom(start, 32), mmdbtype.Uint32(i+1)))
				start = start.Next()
			}
			start = netip.MustParseAddr("1.2.3.0")
			calls := 0
			err := insertReentrancyCallback(t, tree, netip.PrefixFrom(start, 32), method,
				func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
					calls++
					require.True(t, tree.inserting)
					_, expectedValue := tree.Get(start.Next())
					nodes := slices.Clone(tree.valueStore.nodes)
					count := tree.nodeCountAllocated
					paths := slices.Clone(tree.paths)
					numbers := slices.Clone(tree.nodeNumbers)
					for operation := range byte(11) {
						require.Error(t, rejectReentrantMutation(
							t,
							tree,
							netip.PrefixFrom(start.Next(), 32),
							operation,
						))
					}
					// Also reject valid prefixes with invalid direct values, before interning.
					require.ErrorIs(
						t,
						tree.Insert(netip.PrefixFrom(start, 32), mmdbtype.Pointer(1)),
						errNestedInsert,
					)
					require.Equal(t, nodes, tree.valueStore.nodes)
					require.Equal(t, count, tree.nodeCountAllocated)
					require.Equal(t, paths, tree.paths)
					require.Equal(t, numbers, tree.nodeNumbers)
					_, value := tree.Get(start.Next())
					require.Equal(t, expectedValue, value)
					return mmdbtype.Uint32(9), nil
				})
			require.NoError(t, err)
			require.Positive(t, calls)
			if method == "InsertRange" || method == "InsertRangeFunc" ||
				method == "InsertRangePureFunc" {
				require.Equal(t, 3, calls, "guard must span every decomposed range prefix")
			}
			require.False(t, tree.inserting)
			_, value := tree.Get(start)
			require.Equal(t, mmdbtype.Uint32(9), value)
			require.NoError(t, tree.auditValueStore())
		})
	}
}

func TestRejectReentrantMutationRegressions(t *testing.T) {
	for _, family := range []struct {
		name       string
		ipVersion  int
		start, far string
	}{
		{"IPv4", 4, "1.2.3.0", "128.0.0.0"},
		{"IPv4-in-IPv6", 6, "1.2.3.0", "128.0.0.0"},
		{"IPv6", 6, "2001:db8::", "a001:db8::"},
	} {
		for _, scenario := range []string{"sibling-merge", "containing-prefix", "remove-parent", "second-child", "disjoint", "slot-reuse"} {
			for _, handle := range []bool{false, true} {
				t.Run(
					fmt.Sprintf("%s/%s/handle=%t", family.name, scenario, handle),
					func(t *testing.T) {
						tree := newReentrancyTree(t, family.ipVersion)
						control := newReentrancyTree(t, family.ipVersion)
						start, far := netip.MustParseAddr(
							family.start,
						), netip.MustParseAddr(
							family.far,
						)
						bits := start.BitLen()
						for _, targetTree := range []*Tree{tree, control} {
							addr := start
							for i := range 8 {
								require.NoError(
									t,
									targetTree.Insert(
										netip.PrefixFrom(addr, bits),
										mmdbtype.Uint32(i+1),
									),
								)
								addr = addr.Next()
							}
							require.NoError(
								t,
								targetTree.Insert(netip.PrefixFrom(far, bits), mmdbtype.Uint32(4)),
							)
							require.NoError(
								t,
								targetTree.Insert(
									netip.PrefixFrom(far.Next(), bits),
									mmdbtype.Uint32(5),
								),
							)
						}
						outer := netip.PrefixFrom(start, bits)
						if scenario == "second-child" {
							outer = netip.PrefixFrom(start.Next(), bits)
						}
						err := tree.InsertFunc(
							outer,
							nil,
							func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
								var nestedErr error
								switch scenario {
								case "sibling-merge":
									nestedErr = tree.Insert(
										netip.PrefixFrom(start.Next(), bits),
										mmdbtype.Uint32(1),
									)
								case "containing-prefix", "second-child":
									nestedErr = tree.Insert(
										netip.PrefixFrom(start, bits-2),
										mmdbtype.Uint32(99),
									)
								case "remove-parent":
									nestedErr = tree.InsertPureFunc(
										netip.PrefixFrom(start, bits-8),
										nil,
										inserter.Remove,
									)
								case "disjoint":
									nestedErr = tree.Insert(
										netip.PrefixFrom(far.Next(), bits),
										mmdbtype.Uint32(6),
									)
								case "slot-reuse":
									// This used to retire the outer parent, reuse it far away,
									// and silently corrupt both the outer and following insert.
									for i, addr := range []netip.Addr{start.Next(), far, far.Next(), start.Next()} {
										values := []mmdbtype.Uint32{1, 4, 6, 2}
										nestedErr = tree.Insert(
											netip.PrefixFrom(addr, bits),
											values[i],
										)
										require.ErrorIs(t, nestedErr, errNestedInsert)
									}
								}
								require.ErrorIs(t, nestedErr, errNestedInsert)
								if handle {
									return mmdbtype.Uint32(5), nil
								}
								return nil, nestedErr
							},
						)
						if handle {
							require.NoError(t, err)
							require.NoError(t, control.Insert(outer, mmdbtype.Uint32(5)))
						} else {
							require.ErrorIs(t, err, errNestedInsert)
						}
						requireReentrancyTreesEqual(t, tree, control, start, far)
						// The following insertion must reach its own subtree.
						for _, targetTree := range []*Tree{tree, control} {
							require.NoError(
								t,
								targetTree.Insert(
									netip.PrefixFrom(start.Next(), bits),
									mmdbtype.Uint32(9),
								),
							)
						}
						requireReentrancyTreesEqual(t, tree, control, start, far)
					},
				)
			}
		}
	}
}

func requireReentrancyTreesEqual(t *testing.T, tree, control *Tree, addresses ...netip.Addr) {
	t.Helper()
	require.False(t, tree.inserting)
	require.NoError(t, tree.auditValueStore())
	require.NoError(t, control.auditValueStore())
	for _, addr := range addresses {
		for range 8 {
			expectedPrefix, expectedValue := control.Get(addr)
			actualPrefix, actualValue := tree.Get(addr)
			require.Equal(t, expectedPrefix, actualPrefix)
			require.Equal(t, expectedValue, actualValue)
			addr = addr.Next()
		}
	}
	actual := writeTreeBytes(t, tree)
	require.Equal(t, writeTreeBytes(t, control), actual)
	reader, err := maxminddb.OpenBytes(actual)
	require.NoError(t, err)
	defer reader.Close()
	require.NoError(t, reader.Verify())
}

func TestReentrantInsertPartialError(t *testing.T) {
	tree := newReentrancyTree(t, 4)
	start := netip.MustParseAddr("1.2.3.0")
	require.NoError(t, tree.Insert(netip.PrefixFrom(start, 32), mmdbtype.Uint32(1)))
	require.NoError(t, tree.Insert(netip.PrefixFrom(start.Next(), 32), mmdbtype.Uint32(2)))
	calls := 0
	target := netip.PrefixFrom(start, 31)
	err := tree.InsertFunc(target, nil,
		func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
			calls++
			if calls == 1 {
				return mmdbtype.Uint32(2), nil
			}
			return nil, tree.Insert(target, mmdbtype.Uint32(99))
		})
	require.ErrorIs(t, err, errNestedInsert)
	require.Equal(t, 2, calls)
	for _, addr := range []netip.Addr{start, start.Next()} {
		prefix, value := tree.Get(addr)
		require.Equal(t, target, prefix)
		require.Equal(t, mmdbtype.Uint32(2), value)
	}
	require.False(t, tree.inserting)
	require.NoError(t, tree.auditValueStore())
	require.NoError(t, tree.Insert(target, mmdbtype.Uint32(3)))
}

func TestInsertionGuardClearsAfterPanic(t *testing.T) {
	for _, method := range []string{"InsertFunc", "InsertRangeFunc", "insertNormalizedRef"} {
		t.Run(method, func(t *testing.T) {
			tree := newReentrancyTree(t, 4)
			prefix := netip.MustParsePrefix("1.2.3.0/32")
			require.PanicsWithValue(t, "callback panic", func() {
				require.NoError(t, insertReentrancyCallback(t, tree, prefix, method,
					func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
						require.Error(t, rejectReentrantMutation(t, tree, prefix, 0))
						panic("callback panic")
					}))
			})
			require.False(t, tree.inserting)
			require.NoError(t, tree.Insert(prefix, mmdbtype.Uint32(1)))
			require.NoError(t, tree.auditValueStore())
			writeTreeBytes(t, tree)
		})
	}
}

func TestInsertionCallbackCanUseAnotherTree(t *testing.T) {
	tree := newReentrancyTree(t, 4)
	other := newReentrancyTree(t, 4)
	prefix := netip.MustParsePrefix("1.2.3.0/32")
	require.NoError(t, tree.InsertFunc(prefix, nil,
		func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
			require.NoError(t, other.Insert(prefix, mmdbtype.Uint32(6)))
			writeTreeBytes(t, other)
			_, value := other.Get(prefix.Addr())
			return value, nil
		}))
	requireReentrancyTreesEqual(t, tree, other, prefix.Addr())
}
