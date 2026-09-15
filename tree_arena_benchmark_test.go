package mmdbwriter

import (
	"net/netip"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func churnTree(t testing.TB, tree *Tree) {
	t.Helper()
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.String("base")))
	require.NoError(
		t,
		tree.Insert(netip.MustParsePrefix("1.2.3.4/32"), mmdbtype.String("specific")),
	)
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/8"), nil))
}

func BenchmarkTreeArenaFixedFootprintChurn(b *testing.B) {
	b.Setenv("MMDBWRITER_REFCOUNT_AUDIT", "")
	for _, cycles := range []int{1, 8, 64} {
		b.Run(strconv.Itoa(cycles), func(b *testing.B) {
			var tree *Tree
			b.ReportAllocs()
			for b.Loop() {
				var err error
				tree, err = New(Options{IPVersion: 4, IncludeReservedNetworks: true})
				require.NoError(b, err)
				for range cycles {
					churnTree(b, tree)
				}
			}
			b.ReportMetric(float64(tree.nodeCountAllocated), "allocated_nodes/op")
			b.ReportMetric(float64(len(tree.paths)), "allocated_paths/op")
			tree.finalize()
			b.ReportMetric(float64(tree.nodeCount), "live_nodes/op")
		})
	}
}
