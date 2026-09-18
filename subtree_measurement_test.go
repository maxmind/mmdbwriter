package mmdbwriter

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net/netip"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func subtreeSyntheticTree(tb testing.TB, unique bool) *Tree {
	tb.Helper()
	tree, err := New(Options{BuildEpoch: 123, IncludeReservedNetworks: true})
	require.NoError(tb, err)
	for group := range uint32(8192) {
		for suffix := range uint32(8) {
			ip := [16]byte{0x20, 0x01, 0x0d, 0xb8}
			binary.BigEndian.PutUint32(ip[4:8], group*2654435761)
			binary.BigEndian.PutUint64(ip[8:], uint64(suffix)*0x2000000000000001)
			value := suffix % 4
			if unique {
				value = group*8 + suffix
			}
			require.NoError(
				tb,
				tree.Insert(netip.PrefixFrom(netip.AddrFrom16(ip), 128), mmdbtype.Uint32(value)),
			)
		}
	}
	return tree
}

// TestSubtreeMeasurement is an opt-in, single-process measurement harness.
// Compile once, then run fresh processes under /usr/bin/time -v with
// -test.run='^TestSubtreeMeasurement$' -test.count=1 -test.v.
// Do not run warm-up trees in the resident set size (RSS) process.
// MMDBWRITER_SUBTREE_MODE selects
// write (Load + first WriteTo) or count
// (expansion + production canonicalization without writing).
func TestSubtreeMeasurement(t *testing.T) {
	mode := os.Getenv("MMDBWRITER_SUBTREE_MODE")
	if mode == "" {
		t.Skip("MMDBWRITER_SUBTREE_MODE is not set")
	}
	start := time.Now()
	path := os.Getenv("MMDBWRITER_BENCHMARK_DB")
	var tree *Tree
	if path == "ipv6-repeated" || path == "ipv6-unique" {
		tree = subtreeSyntheticTree(t, path == "ipv6-unique")
	} else {
		var err error
		tree, err = Load(path, Options{BuildEpoch: 123, IncludeReservedNetworks: true})
		require.NoError(t, err)
	}
	loadTime := time.Since(start)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start = time.Now()
	var expandTime time.Duration
	if mode == "count" {
		tree.expandTree()
		expandTime = time.Since(start)
	}
	start = time.Now()
	var distinct uint32
	var size int64
	switch mode {
	case "write":
		var err error
		size, err = tree.WriteTo(io.Discard)
		require.NoError(t, err)
		distinct = uint32(newNodeIndex(tree.nodeCount))
	case "count":
		tree.nodeNumbers = make([]uint32, tree.nodeCountAllocated)
		distinct = uint32(newNodeIndex(tree.canonicalizeSubtrees()))
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	passTime := time.Since(start)
	runtime.ReadMemStats(&after)
	// Count occurrences outside the timed pass. Canonicalization keeps
	// the owning tree intact, so this includes duplicate subtrees.
	total := subtreeReachableCount(tree, tree.root)
	result := map[string]any{
		"fixture":      path,
		"mode":         mode,
		"record_size":  tree.recordSize,
		"allocated":    tree.nodeCountAllocated,
		"total":        total,
		"distinct":     distinct,
		"saved_bytes":  int64(total-int(distinct)) * int64(tree.recordSize) / 4,
		"output_bytes": size,
		"load_ns":      loadTime.Nanoseconds(),
		"expand_ns":    expandTime.Nanoseconds(),
		"pass_ns":      passTime.Nanoseconds(),
		"alloc_bytes":  after.TotalAlloc - before.TotalAlloc,
		"allocs":       after.Mallocs - before.Mallocs,
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	t.Log(string(encoded))
	runtime.KeepAlive(tree)
}
