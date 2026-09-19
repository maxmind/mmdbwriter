package mmdbwriter

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"iter"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// referenceSubtreeCount uses recursive structural strings rather than the
// compact table's keys or IDs, so it can detect incorrect canonicalization.
func referenceSubtreeCount(tree *Tree) (distinct, total int) {
	seen := map[string]bool{}
	protected := subtreeIPv4Root(tree)
	var visit func(nodeIndex) string
	visit = func(index nodeIndex) string {
		total++
		n := tree.nodeAt(index)
		var key strings.Builder
		key.WriteByte('(')
		for _, r := range n.children {
			fmt.Fprintf(&key, "%d:", r.recordType)
			switch r.recordType {
			case recordTypeNode, recordTypeFixedNode:
				key.WriteString(visit(r.nodeIndex))
			case recordTypeData:
				fmt.Fprint(&key, r.value)
			case recordTypeAlias:
				fmt.Fprint(&key, r.nodeIndex)
			case recordTypeEmpty, recordTypeReserved:
			default:
				panic("unexpected reference record")
			}
			key.WriteByte(',')
		}
		key.WriteByte(')')
		if index == protected {
			key.WriteString("ipv4-root")
		}
		seen[key.String()] = true
		return key.String()
	}
	visit(tree.root)
	return len(seen), total
}

func TestSubtreeCounter(t *testing.T) {
	for _, ipVersion := range []int{4, 6} {
		for _, poison := range []bool{false, true} {
			t.Run(fmt.Sprintf("ipv%d/poison=%t", ipVersion, poison), func(t *testing.T) {
				tree, err := New(Options{IPVersion: ipVersion, RefcountAudit: poison})
				require.NoError(t, err)
				for i := range 80 {
					ip := netip.AddrFrom4([4]byte{2, byte(i + 1), 4, 5})
					require.NoError(t, tree.Insert(netip.PrefixFrom(ip, 32), mmdbtype.Uint32(i%3)))
				}
				churnTree(t, tree)
				tree.expandPaths(tree.root, 0)
				wantDistinct, wantTotal := referenceSubtreeCount(tree)
				tree.nodeNumbers = make([]uint32, tree.nodeCountAllocated)
				distinct := tree.canonicalizeSubtrees()
				require.Equal(t, wantDistinct, distinct)
				require.Equal(t, wantTotal, subtreeReachableCount(tree, tree.root))
				require.Less(t, distinct, wantTotal)
				for i := range tree.nodeCountAllocated {
					if tree.rawNodeAt(nodeIndex(i)).children[0].recordType == recordTypeRetired {
						require.Zero(t, tree.nodeNumbers[i])
					}
				}
				require.NoError(t, tree.auditValueStore())
			})
		}
	}
}

func TestSubtreeHashCollisions(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	a := tree.newNode([2]record{{recordType: recordTypeReserved}, {}})
	b := tree.newNode([2]record{{}, {recordType: recordTypeReserved}})
	copyA := tree.newNode(tree.nodeAt(a).children)
	table := subtreeTable{
		tree:  tree,
		ids:   make([]uint32, tree.nodeCountAllocated),
		slots: make([]subtreeSlot, 2),
		seed:  maphash.MakeSeed(),
	}
	for _, index := range []nodeIndex{a, b, copyA} {
		table.ids[index] = table.intern(index, table.key(index), 1)
	}
	require.Equal(t, table.ids[a], table.ids[copyA])
	require.NotEqual(t, table.ids[a], table.ids[b])
	require.Len(t, table.slots, 4, "growth must preserve colliding entries")
}

func TestSubtreeHashCollisionPayloads(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind recordType
	}{
		{"data", recordTypeData},
		{"node", recordTypeNode},
		{"fixed-node", recordTypeFixedNode},
		{"alias", recordTypeAlias},
	} {
		for direction := range 2 {
			t.Run(fmt.Sprintf("%s/child=%d", tc.name, direction), func(t *testing.T) {
				tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
				require.NoError(t, err)
				valueA, err := tree.valueStore.intern(mmdbtype.String("A"))
				require.NoError(t, err)
				valueB, err := tree.valueStore.intern(mmdbtype.String("B"))
				require.NoError(t, err)
				childA := tree.newNode([2]record{{recordType: recordTypeReserved}, {}})
				childB := tree.newNode([2]record{{}, {recordType: recordTypeReserved}})
				copyChildA := tree.newNode(tree.nodeAt(childA).children)
				recordA, recordB := record{recordType: tc.kind}, record{recordType: tc.kind}
				if tc.kind == recordTypeData {
					recordA.value, recordB.value = valueA, valueB
				} else {
					recordA.nodeIndex, recordB.nodeIndex = childA, childB
				}
				children := [2]record{}
				children[direction] = recordA
				a := tree.newNode(children)
				copyA := tree.newNode(children)
				children[direction] = recordB
				b := tree.newNode(children)
				copyB := tree.newNode(children)
				children = tree.nodeAt(a).children
				if tc.kind == recordTypeNode || tc.kind == recordTypeFixedNode {
					children[direction].nodeIndex = copyChildA
				}
				equivalentA := tree.newNode(children)
				table := subtreeTable{
					tree:  tree,
					ids:   make([]uint32, tree.nodeCountAllocated),
					slots: make([]subtreeSlot, 4),
				}
				// These model already-canonicalized children. Equal child IDs
				// must compare equal even when their arena indexes differ.
				table.ids[childA], table.ids[childB], table.ids[copyChildA] = 100, 101, 100
				var keyA, keyB subtreeKey
				keyA[2] = uint32(tc.kind) << (8 * direction)
				keyB[2] = keyA[2]
				switch tc.kind {
				case recordTypeData:
					keyA[direction], keyB[direction] = uint32(valueA), uint32(valueB)
				case recordTypeNode, recordTypeFixedNode:
					keyA[direction], keyB[direction] = 100, 101
				case recordTypeAlias:
					keyA[direction], keyB[direction] = uint32(childA), uint32(childB)
				default:
					t.Fatal("unexpected record type")
				}
				require.Equal(t, keyA, table.key(a))
				require.Equal(t, keyB, table.key(b))
				require.Equal(t, keyA, table.key(equivalentA))
				for _, probe := range []struct {
					index nodeIndex
					want  uint32
				}{
					{a, 1}, {b, 2}, {copyA, 1}, {copyB, 2}, {equivalentA, 1}, {a, 1}, {b, 2},
				} {
					id := table.intern(probe.index, table.key(probe.index), 7)
					table.ids[probe.index] = id
					require.Equal(t, probe.want, id)
				}
				require.EqualValues(t, 2, table.distinct)
				require.Equal(t, 2, table.used)
			})
		}
	}
}

func TestSubtreeHashCollisionGrowth(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	var nodes [4][3]nodeIndex
	for i := range nodes {
		value, internErr := tree.valueStore.intern(mmdbtype.Uint32(i))
		require.NoError(t, internErr)
		for copyIndex := range nodes[i] {
			nodes[i][copyIndex] = tree.newNode(
				[2]record{{recordType: recordTypeData, value: value}, {}},
			)
		}
	}
	table := subtreeTable{
		tree:  tree,
		ids:   make([]uint32, tree.nodeCountAllocated),
		slots: make([]subtreeSlot, 4),
	}
	intern := func(index nodeIndex) uint32 {
		// Hash 7 wraps the cluster around the end of both the old and new tables.
		id := table.intern(index, table.key(index), 7)
		table.ids[index] = id
		return id
	}
	var ids [4]uint32
	for i := range 3 {
		ids[i] = intern(nodes[i][0])
	}
	require.EqualValues(t, 3, table.distinct)
	require.Equal(t, 3, table.used)
	require.Len(t, table.slots, 4)
	for i := range 3 {
		require.Equal(t, ids[i], intern(nodes[i][1]))
	}
	require.Len(t, table.slots, 4, "duplicates at the growth threshold must not grow the table")
	ids[3] = intern(nodes[3][0])
	require.EqualValues(t, 4, table.distinct)
	require.Equal(t, 4, table.used)
	require.Len(t, table.slots, 8, "growth must preserve all three colliding residents")
	for _, copyIndex := range []int{2, 0} {
		for i := range nodes {
			require.Equal(t, ids[i], intern(nodes[i][copyIndex]))
		}
	}
	require.EqualValues(t, 4, table.distinct)
	require.Equal(t, 4, table.used)
	require.Len(t, table.slots, 8)
}

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
				netip.PrefixFrom(netipx.PrefixLastIP(prefix).Next(), 25),
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
						require.NoError(t, right.Verify())
						require.EqualValues(t, tree.nodeCount, right.Metadata.NodeCount)
						compareSubtreeReaders(t, left, right)
						compareSubtreeReaders(
							t,
							left,
							right,
							maxminddb.IncludeAliasedNetworks(),
							maxminddb.IncludeNetworksWithoutData(),
						)
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
			require.NoError(t, right.Verify())
			compareSubtreeReaders(t, left, right)
			compareSubtreeReaders(t, left, right, maxminddb.IncludeAliasedNetworks())
			var value string
			require.NoError(t, right.Lookup(netip.MustParseAddr("2001:db8::1")).Decode(&value))
			require.Equal(t, "A", value)
		})
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
			_, err := tree.WriteTo(subtreeFailWriter{err: failure})
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

type subtreeFailWriter struct{ err error }

func (w subtreeFailWriter) Write([]byte) (int, error) { return 0, w.err }

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

// The external check is intentionally separate from performance runs: writing
// files, auditing, full enumeration, and decoding must not inflate their RSS.
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
