package mmdbwriter

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func subtreeTestTree(t *testing.T, opts Options) *Tree {
	t.Helper()
	opts.BuildEpoch = 123
	opts.DatabaseType = "Subtree-Test"
	opts.Description = map[string]string{"en": "Subtree test"}
	tree, err := New(opts)
	require.NoError(t, err)
	for _, network := range []string{"1.2.3.0/25", "2.2.3.0/25", "3.2.3.0/25"} {
		prefix := netip.MustParsePrefix(network)
		require.NoError(t, tree.Insert(prefix, mmdbtype.String("A")))
		require.NoError(
			t,
			tree.Insert(
				netip.PrefixFrom(nextPrefixAddr(t, prefix), 25),
				mmdbtype.String("B"),
			),
		)
	}
	if opts.IPVersion != 4 {
		for _, network := range []string{"2003:1::1/128", "2003:2::1/128", "2003:3::1/128"} {
			require.NoError(t, tree.Insert(netip.MustParsePrefix(network), mmdbtype.String("v6")))
		}
	}
	return tree
}

// finalizeUnsharedTree gives each owning node a distinct preorder number so the
// regular serializer can produce a trie for comparison with its directed
// acyclic graph (DAG) output.
// This test-only oracle bypasses canonicalization, but shares no hashing or
// subtree-equivalence logic with it. Mutation or clearing nodeCount restores
// normal finalization on the next write.
func finalizeUnsharedTree(tree *Tree) {
	tree.expandTree()
	tree.nodeNumbers = make([]uint32, tree.nodeCountAllocated)
	tree.nodeCount = 0
	var visit func(nodeIndex)
	visit = func(index nodeIndex) {
		tree.nodeNumbers[index] = uint32(newNodeIndex(tree.nodeCount))
		tree.nodeCount++
		for _, child := range tree.nodeAt(index).children {
			if child.recordType == recordTypeNode || child.recordType == recordTypeFixedNode {
				visit(child.nodeIndex)
			}
		}
	}
	visit(tree.root)
}

// compareSubtreeReaders keeps only one result per reader, so the same full
// stream check can be used with production databases without buffering them.
func compareSubtreeReaders(
	t *testing.T,
	left, right *maxminddb.Reader,
	options ...maxminddb.NetworksOption,
) {
	t.Helper()
	leftNext, leftStop := iter.Pull(left.Networks(options...))
	defer leftStop()
	rightNext, rightStop := iter.Pull(right.Networks(options...))
	defer rightStop()
	// The zero-value decoder does not cache containers across records. Keeping
	// that cache here would retain both databases' entire decoded data sections.
	leftValue, rightValue := &mmdbtype.Unmarshaler{}, &mmdbtype.Unmarshaler{}
	for {
		l, lok := leftNext()
		r, rok := rightNext()
		require.Equal(t, lok, rok)
		if !lok {
			return
		}
		require.NoError(t, l.Err())
		require.NoError(t, r.Err())
		require.Equal(t, l.Prefix(), r.Prefix())
		leftValue.Clear()
		rightValue.Clear()
		require.NoError(t, l.Decode(leftValue))
		require.NoError(t, r.Decode(rightValue))
		lv, rv := leftValue.Result(), rightValue.Result()
		if lv == nil {
			require.Nil(t, rv)
		} else {
			require.NotNil(t, rv)
			require.True(t, lv.Equal(rv), "value differs at %s", l.Prefix())
		}
	}
}

// verifySubtreeReaders checks both reader validity and every network/value pair,
// including aliases and empty networks. Callers keep their scenario assertions.
func verifySubtreeReaders(t *testing.T, left, right *maxminddb.Reader) {
	t.Helper()
	require.NoError(t, left.Verify())
	require.NoError(t, right.Verify())
	compareSubtreeReaders(t, left, right)
	compareSubtreeReaders(
		t,
		left,
		right,
		maxminddb.IncludeAliasedNetworks(),
		maxminddb.IncludeNetworksWithoutData(),
	)
}

func TestDeduplicateSubtrees(t *testing.T) {
	for _, version := range []int{4, 6} {
		for _, size := range []int{24, 28, 32} {
			for _, reserved := range []bool{false, true} {
				t.Run(
					fmt.Sprintf("ipv%d/%d/reserved=%t", version, size, reserved),
					func(t *testing.T) {
						opts := Options{
							IPVersion:               version,
							RecordSize:              size,
							IncludeReservedNetworks: reserved,
						}
						baseline := subtreeTestTree(t, opts)
						finalizeUnsharedTree(baseline)
						before := writeTreeBytes(t, baseline)
						tree := subtreeTestTree(t, opts)
						after := writeTreeBytes(t, tree)
						if dir := os.Getenv("MMDBWRITER_SUBTREE_FIXTURES"); dir != "" && reserved {
							name := fmt.Sprintf("writer-ipv%d-%d.mmdb", version, size)
							//nolint:gosec // This opt-in harness writes to the caller's fixture directory.
							require.NoError(t, os.WriteFile(filepath.Join(dir, name), after, 0o600))
						}
						require.Less(t, tree.nodeCount, baseline.nodeCount)
						require.Less(t, len(after), len(before))
						require.Equal(t, after, writeTreeBytes(t, tree))
						require.Equal(t, after, writeTreeBytes(t, subtreeTestTree(t, opts)))
						require.NoError(t, tree.auditValueStore())
						left, err := maxminddb.OpenBytes(before)
						require.NoError(t, err)
						defer left.Close()
						right, err := maxminddb.OpenBytes(after)
						require.NoError(t, err)
						defer right.Close()
						require.EqualValues(t, tree.nodeCount, right.Metadata.NodeCount)
						verifySubtreeReaders(t, left, right)
						for result := range right.Networks() {
							for _, ip := range []netip.Addr{result.Prefix().Addr(), netipx.PrefixLastIP(result.Prefix())} {
								var value string
								lookup := right.Lookup(ip)
								require.NoError(t, lookup.Decode(&value))
								prefix, expected := tree.Get(ip)
								require.Equal(t, prefix, lookup.Prefix())
								require.Equal(t, expected, mmdbtype.String(value))
							}
						}
						loaded, err := Load(
							writeTempFile(t, after),
							Options{
								BuildEpoch:              123,
								IncludeReservedNetworks: reserved,
							},
						)
						require.NoError(t, err)
						require.Equal(t, after, writeTreeBytes(t, loaded))
					},
				)
			}
		}
	}
}

func TestDeduplicateSubtreesProtectsIPv4Root(t *testing.T) {
	for _, disableAliases := range []bool{false, true} {
		t.Run(strconv.FormatBool(disableAliases), func(t *testing.T) {
			tree, err := New(
				Options{
					BuildEpoch:              123,
					DatabaseType:            "IPv4-Root",
					Description:             map[string]string{"en": "Protected entry"},
					IncludeReservedNetworks: true,
					DisableIPv4Aliasing:     disableAliases,
				},
			)
			require.NoError(t, err)
			for _, item := range []struct{ prefix, value string }{
				{"0.0.0.0/1", "A"},
				{"128.0.0.0/1", "B"},
				{"2001:db8::/65", "A"},
				{"2001:db8:0:0:8000::/65", "B"},
				{"2001:db8:1::/65", "A"},
				{"2001:db8:1:0:8000::/65", "B"},
			} {
				require.NoError(
					t,
					tree.Insert(netip.MustParsePrefix(item.prefix), mmdbtype.String(item.value)),
				)
			}
			finalizeUnsharedTree(tree)
			before := writeTreeBytes(t, tree)
			tree.nodeCount = 0
			after := writeTreeBytes(t, tree)
			protected := subtreeIPv4Root(tree)
			require.NotEqual(t, noNodeIndex, protected)
			require.NotZero(t, tree.nodeNumbers[protected], "the IPv4 entry follows the IPv6 root")
			for index, number := range tree.nodeNumbers {
				if nodeIndex(index) != protected {
					require.NotEqual(t, tree.nodeNumbers[protected], number)
				}
			}
			left, err := maxminddb.OpenBytes(before)
			require.NoError(t, err)
			defer left.Close()
			right, err := maxminddb.OpenBytes(after)
			require.NoError(t, err)
			defer right.Close()
			verifySubtreeReaders(t, left, right)
			var value string
			require.NoError(t, right.Lookup(netip.MustParseAddr("2001:db8::1")).Decode(&value))
			require.Equal(t, "A", value)
		})
	}
}

func TestSubtreeUniqueSharedTransitions(t *testing.T) {
	for _, version := range []int{4, 6} {
		for _, aliases := range []bool{false, true} {
			for _, size := range []int{24, 28, 32} {
				t.Run(
					fmt.Sprintf("ipv%d/aliases=%t/size=%d", version, aliases, size),
					func(t *testing.T) {
						opts := Options{
							BuildEpoch:   123,
							IPVersion:    version,
							RecordSize:   size,
							DatabaseType: "Subtree-Transitions",
							Description: map[string]string{
								"en": "Unique and shared subtree transitions",
							},
							IncludeReservedNetworks: true,
							DisableIPv4Aliasing:     !aliases,
						}
						control, err := New(opts)
						require.NoError(t, err)
						tree, err := New(opts)
						require.NoError(t, err)
						prefixes := []string{"1.2.3.4/32", "2.2.3.4/32", "1.2.3.4/32", "3.2.3.4/32"}
						if version == 6 {
							prefixes = append(prefixes, "2001:db8::1/128", "2001:db8:1::1/128")
						}
						for i, prefix := range prefixes {
							var value mmdbtype.DataType = mmdbtype.String("shared after insertion")
							if i == 2 {
								value = nil
							}
							require.NoError(t, control.Insert(netip.MustParsePrefix(prefix), value))
							require.NoError(t, tree.Insert(netip.MustParsePrefix(prefix), value))
							finalizeUnsharedTree(control)
							left, openErr := maxminddb.OpenBytes(writeTreeBytes(t, control))
							require.NoError(t, openErr)
							output := writeTreeBytes(t, tree)
							require.Equal(
								t,
								output,
								writeTreeBytes(t, tree),
								"cached writes must be stable",
							)
							right, openErr := maxminddb.OpenBytes(output)
							require.NoError(t, openErr)
							verifySubtreeReaders(t, left, right)
							left.Close()
							right.Close()
							require.NoError(t, tree.auditValueStore())
						}
					},
				)
			}
		}
	}
}

func TestDeduplicateSubtreesMutationAndRetry(t *testing.T) {
	for _, poison := range []bool{false, true} {
		t.Run(strconv.FormatBool(poison), func(t *testing.T) {
			tree := subtreeTestTree(
				t,
				Options{IPVersion: 4, RefcountAudit: poison},
			)
			first := writeTreeBytes(t, tree)
			numbers := &tree.nodeNumbers[0]
			require.Equal(t, first, writeTreeBytes(t, tree))
			require.Same(t, numbers, &tree.nodeNumbers[0], "cached writes must reuse numbering")
			failure := errors.New("test writer failure")
			_, err := tree.WriteTo(&subtreePartialWriter{err: failure})
			require.ErrorIs(t, err, failure)
			require.Equal(t, first, writeTreeBytes(t, tree))
			for range 3 {
				churnTree(t, tree)
				require.Nil(t, tree.nodeNumbers)
				require.Zero(t, tree.nodeCount)
				writeTreeBytes(t, tree)
				require.NoError(t, tree.auditValueStore())
			}
			calls := 0
			err = tree.InsertFunc(netip.MustParsePrefix("2.0.0.0/8"), nil,
				func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
					calls++
					if calls == 2 {
						return nil, failure
					}
					return mmdbtype.String("changed"), nil
				})
			require.ErrorIs(t, err, failure)
			require.Nil(t, tree.nodeNumbers)
			require.Zero(t, tree.nodeCount)
			after := writeTreeBytes(t, tree)
			require.Equal(t, after, writeTreeBytes(t, tree))
			require.NoError(t, tree.auditValueStore())
		})
	}
}

func TestDeduplicateSubtreesWriteOrderError(t *testing.T) {
	tree := subtreeTestTree(t, Options{IPVersion: 4})
	tree.finalize()
	tree.nodeNumbers[tree.root] = 1
	_, err := tree.WriteTo(io.Discard)
	require.ErrorContains(t, err, "numbered 1 but 0 expected")
}

func TestDeduplicateSubtreesNoDuplicates(t *testing.T) {
	for _, filled := range []bool{false, true} {
		t.Run(strconv.FormatBool(filled), func(t *testing.T) {
			tree, err := New(Options{BuildEpoch: 123, IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)
			if filled {
				require.NoError(
					t,
					tree.Insert(netip.MustParsePrefix("0.0.0.0/1"), mmdbtype.String("A")),
				)
				require.NoError(
					t,
					tree.Insert(netip.MustParsePrefix("128.0.0.0/1"), mmdbtype.String("B")),
				)
			}
			finalizeUnsharedTree(tree)
			before := writeTreeBytes(t, tree)
			tree.nodeCount = 0
			require.Equal(t, before, writeTreeBytes(t, tree))
		})
	}
}

func TestDeduplicateSubtreesGolden(t *testing.T) {
	for _, tc := range []struct {
		size int
		hex  string
	}{
		{24, "000001000001000012000014"},
		{28, "0000010000000100001200000014"},
		{32, "00000001000000010000001200000014"},
	} {
		t.Run(strconv.Itoa(tc.size), func(t *testing.T) {
			tree, err := New(
				Options{
					BuildEpoch:              123,
					IPVersion:               4,
					RecordSize:              tc.size,
					IncludeReservedNetworks: true,
				},
			)
			require.NoError(t, err)
			for i, value := range []string{"A", "B", "A", "B"} {
				//nolint:gosec // There are exactly four /2 prefixes.
				ip := netip.AddrFrom4([4]byte{byte(i * 64), 0, 0, 0})
				require.NoError(t, tree.Insert(netip.PrefixFrom(ip, 2), mmdbtype.String(value)))
			}
			output := writeTreeBytes(t, tree)
			require.Equal(t, 2, tree.nodeCount, "equal node children still consume an address bit")
			require.Equal(t, tc.hex, hex.EncodeToString(output[:tc.size/2]))
		})
	}
}

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

// Every level on the leftmost path has an internal right sibling, reaching
// the pending-sibling bound even though those identical right subtrees share.
func TestSubtreeWriteMaximumPendingSiblings(t *testing.T) {
	for _, version := range []int{4, 6} {
		for _, width := range []int{24, 28, 32} {
			t.Run(fmt.Sprintf("IPv%d/%d", version, width), func(t *testing.T) {
				bits := 128
				if version == 4 {
					bits = 32
				}
				tree, err := New(
					Options{
						IPVersion:               version,
						RecordSize:              width,
						BuildEpoch:              123,
						IncludeReservedNetworks: true,
						DisableIPv4Aliasing:     true,
						DatabaseType:            "Maximum pending siblings",
						Description:             map[string]string{"en": "Writer depth test"},
					},
				)
				require.NoError(t, err)
				address := func(bit, next int) netip.Addr {
					var raw [16]byte
					if bit >= 0 {
						raw[bit/8] |= 1 << (7 - bit%8)
					}
					if next >= 0 {
						raw[next/8] |= 1 << (7 - next%8)
					}
					if version == 4 {
						return netip.AddrFrom4([4]byte(raw[:4]))
					}
					return netip.AddrFrom16(raw)
				}
				for bit := range bits - 1 {
					require.NoError(
						t,
						tree.Insert(netip.PrefixFrom(address(bit, -1), bit+1), mmdbtype.Uint32(1)),
					)
					require.NoError(
						t,
						tree.Insert(netip.PrefixFrom(address(bit, -1), bit+2), mmdbtype.Uint32(2)),
					)
				}
				require.NoError(
					t,
					tree.Insert(netip.PrefixFrom(address(-1, -1), bits), mmdbtype.Uint32(3)),
				)
				raw := writeTreeBytes(t, tree)
				require.Equal(t, raw, writeTreeBytes(t, tree))
				reader, err := maxminddb.OpenBytes(raw)
				require.NoError(t, err)
				defer reader.Close()
				require.NoError(t, reader.Verify())
				for bit := range bits - 1 {
					var value uint32
					require.NoError(t, reader.Lookup(address(bit, -1)).Decode(&value))
					require.Equal(t, uint32(2), value)
					require.NoError(t, reader.Lookup(address(bit, bit+1)).Decode(&value))
					require.Equal(t, uint32(1), value)
				}
				var value uint32
				require.NoError(t, reader.Lookup(address(-1, -1)).Decode(&value))
				require.Equal(t, uint32(3), value)
			})
		}
	}
}

// dagCompatibilityFixture encodes four explicit nodes, with node 1 reached
// from the root and again at a different depth via node 2. This checks reader
// compatibility independently of the proposed canonicalizer and writer.
func dagCompatibilityFixture(t *testing.T, recordSize int) []byte {
	t.Helper()
	tree, err := New(Options{
		BuildEpoch:              123,
		DatabaseType:            "Subtree-DAG-Test",
		Description:             map[string]string{"en": "Shared search nodes"},
		IPVersion:               4,
		RecordSize:              recordSize,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)
	data := func(value string) record {
		ref, internErr := tree.valueStore.intern(mmdbtype.String(value))
		require.NoError(t, internErr)
		return record{recordType: recordTypeData, value: ref}
	}
	nodeRecord := func(index nodeIndex) record { return record{recordType: recordTypeNode, nodeIndex: index} }
	nodes := []node{
		{children: [2]record{nodeRecord(1), nodeRecord(2)}},
		{children: [2]record{data("A"), data("B")}},
		{children: [2]record{nodeRecord(1), nodeRecord(3)}},
		{children: [2]record{{}, data("C")}},
	}
	tree.nodeCount = len(nodes)
	tree.nodeNumbers = []uint32{0, 1, 2, 3}
	dw := newDataWriter(tree.valueStore, true)
	var output bytes.Buffer
	buf := make([]byte, recordSize/4)
	for i := range nodes {
		require.NoError(t, tree.copyNode(buf, &nodes[i], dw))
		_, err = output.Write(buf)
		require.NoError(t, err)
	}
	output.Write(dataSectionSeparator)
	output.Write(dw.Bytes())
	output.Write(metadataStartMarker)
	metadata := newDataWriter(newValueStore(), true)
	metadataBytes, err := tree.writeMetadata(metadata)
	require.NoError(t, err)
	require.EqualValues(t, metadata.Len(), metadataBytes)
	output.Write(metadata.Bytes())
	return output.Bytes()
}

func TestSubtreeDAGReaderCompatibility(t *testing.T) {
	for _, recordSize := range []int{24, 28, 32} {
		t.Run(strconv.Itoa(recordSize), func(t *testing.T) {
			fixture := dagCompatibilityFixture(t, recordSize)
			reader, err := maxminddb.OpenBytes(fixture)
			require.NoError(t, err)
			defer reader.Close()
			require.NoError(t, reader.Verify())
			want := []string{
				"0.0.0.0/2=A",
				"64.0.0.0/2=B",
				"128.0.0.0/3=A",
				"160.0.0.0/3=B",
				"224.0.0.0/3=C",
			}
			var got []string
			for result := range reader.Networks() {
				var value string
				require.NoError(t, result.Decode(&value))
				got = append(got, result.Prefix().String()+"="+value)
				var lookup string
				require.NoError(t, reader.Lookup(result.Prefix().Addr()).Decode(&lookup))
				require.Equal(t, value, lookup)
			}
			require.Equal(t, want, got)
			if dir := os.Getenv("MMDBWRITER_SUBTREE_FIXTURES"); dir != "" {
				//nolint:gosec // This opt-in harness writes to the caller's fixture directory.
				require.NoError(
					t,
					os.WriteFile(
						filepath.Join(dir, fmt.Sprintf("dag-%d.mmdb", recordSize)),
						fixture,
						0o600,
					),
				)
			}
		})
	}
}

// The external check is intentionally separate from performance runs: writing
// files, auditing, full enumeration, and decoding must not inflate their
// resident set size (RSS).
func TestSubtreeExternalEquivalence(t *testing.T) {
	if os.Getenv("MMDBWRITER_SUBTREE_VERIFY") == "" {
		t.Skip("MMDBWRITER_SUBTREE_VERIFY is not set")
	}
	path := os.Getenv("MMDBWRITER_BENCHMARK_DB")
	tree, err := Load(path, Options{BuildEpoch: 123, IncludeReservedNetworks: true})
	require.NoError(t, err)
	finalizeUnsharedTree(tree)
	baselinePath := writeTempDB(t, tree)
	tree.nodeCount = 0
	dedupPath := writeTempDB(t, tree)
	left, err := maxminddb.Open(baselinePath)
	require.NoError(t, err)
	defer left.Close()
	right, err := maxminddb.Open(dedupPath)
	require.NoError(t, err)
	defer right.Close()
	require.NoError(t, right.Verify())
	compareSubtreeDataSections(t, baselinePath, dedupPath, left, right)
	compareSubtreeOffsets(t, left, right)
	compareSubtreeOffsets(
		t,
		left,
		right,
		maxminddb.IncludeAliasedNetworks(),
		maxminddb.IncludeNetworksWithoutData(),
	)
}

func compareSubtreeDataSections(
	t *testing.T,
	leftPath, rightPath string,
	left, right *maxminddb.Reader,
) {
	t.Helper()
	readSection := func(path string, reader *maxminddb.Reader) []byte {
		//nolint:gosec // These paths are temporary databases written by this test.
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		end := bytes.LastIndex(data, metadataStartMarker)
		if end < 0 {
			t.Fatal("missing metadata marker")
		}
		start := uint64(reader.Metadata.NodeCount)*uint64(reader.Metadata.RecordSize/4) + 16
		//nolint:gosec // end is checked above; Fatal stops this test on a missing marker.
		require.LessOrEqual(t, start, uint64(end))
		return data[start:end]
	}
	require.True(
		t,
		bytes.Equal(readSection(leftPath, left), readSection(rightPath, right)),
		"data section changed",
	)
}

// Once data bytes are proven identical, equality of every prefix and data
// offset proves equality of every network/value pair, without decoding shared
// values millions of times. Small fixtures separately check decoded values.
func compareSubtreeOffsets(
	t *testing.T,
	left, right *maxminddb.Reader,
	options ...maxminddb.NetworksOption,
) {
	t.Helper()
	leftNext, leftStop := iter.Pull(left.Networks(options...))
	defer leftStop()
	rightNext, rightStop := iter.Pull(right.Networks(options...))
	defer rightStop()
	for {
		l, lok := leftNext()
		r, rok := rightNext()
		if lok != rok {
			t.Fatal("network counts differ")
		}
		if !lok {
			return
		}
		if l.Err() != nil || r.Err() != nil {
			t.Fatalf("network errors: left %v, right %v", l.Err(), r.Err())
		}
		if l.Prefix() != r.Prefix() || l.Offset() != r.Offset() {
			t.Fatalf(
				"network differs: left %s @ %d, right %s @ %d",
				l.Prefix(),
				l.Offset(),
				r.Prefix(),
				r.Offset(),
			)
		}
	}
}
