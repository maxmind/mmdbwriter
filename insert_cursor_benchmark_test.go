package mmdbwriter

import (
	"math/rand/v2"
	"net/netip"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

var addressOrderBenchmarkDistributions = []struct {
	name       string
	valueShift uint8
}{
	{"distinct-siblings", 0},
	{"merging-siblings", 3},
}

// BenchmarkTreeInsertAddressOrder measures construction, including cursor
// bookkeeping on unordered input. IPv4-in-IPv6 has a long shared leading path;
// the native IPv4 case also measures shorter paths. Use the same seeds and
// prefixes when comparing revisions. Load and overlays have separate benchmarks
// in tree_benchmark_test.go.
func BenchmarkTreeInsertAddressOrder(b *testing.B) {
	for _, ipVersion := range []int{4, 6} {
		b.Run("tree-ipv"+strconv.Itoa(ipVersion), func(b *testing.B) {
			for _, distribution := range addressOrderBenchmarkDistributions {
				b.Run(distribution.name, func(b *testing.B) {
					for _, order := range []string{"sorted", "random", "descending"} {
						b.Run(order, func(b *testing.B) {
							prefixes := addressOrderBenchmarkPrefixes(16384, order)
							b.ReportAllocs()
							for b.Loop() {
								tree, err := New(
									Options{
										BuildEpoch:              123,
										IPVersion:               ipVersion,
										IncludeReservedNetworks: true,
									},
								)
								if err != nil {
									b.Fatal(err)
								}
								if err := insertAddressOrderBenchmark(
									tree,
									prefixes,
									distribution.valueShift,
								); err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				})
			}
		})
	}
}

func addressOrderBenchmarkPrefixes(count int, order string) []netip.Prefix {
	prefixes := make([]netip.Prefix, count)
	for i := range prefixes {
		prefixes[i] = netip.PrefixFrom(netip.AddrFrom4([4]byte{1, byte(i >> 8), byte(i), 0}), 24)
	}
	switch order {
	case "random":
		//nolint:gosec // Reproducible benchmark order.
		random := rand.New(rand.NewPCG(1, 2))
		random.Shuffle(
			len(prefixes),
			func(i, j int) { prefixes[i], prefixes[j] = prefixes[j], prefixes[i] },
		)
	case "descending":
		slices.Reverse(prefixes)
	case "sorted":
	}
	return prefixes
}

func insertAddressOrderBenchmark(tree *Tree, prefixes []netip.Prefix, valueShift uint8) error {
	for _, prefix := range prefixes {
		// Values depend on the prefix, never its position in the insertion order.
		// Shifting by three gives each group of eight /24s the same value,
		// causing three levels of sibling merges into a /21.
		ip := prefix.Addr().As4()
		if err := tree.Insert(prefix, mmdbtype.Uint32((ip[2]>>valueShift)%16)); err != nil {
			return err
		}
	}
	return nil
}

func TestAddressOrderBenchmarkEquivalent(t *testing.T) {
	for _, ipVersion := range []int{4, 6} {
		t.Run(strconv.Itoa(ipVersion), func(t *testing.T) {
			for _, distribution := range addressOrderBenchmarkDistributions {
				t.Run(distribution.name, func(t *testing.T) {
					newTree := func(t *testing.T, order string) *Tree {
						t.Helper()
						tree, err := New(Options{
							BuildEpoch:              123,
							IPVersion:               ipVersion,
							DatabaseType:            "address-order-benchmark-test",
							IncludeReservedNetworks: true,
						})
						require.NoError(t, err)
						require.NoError(
							t,
							insertAddressOrderBenchmark(
								tree,
								addressOrderBenchmarkPrefixes(128, order),
								distribution.valueShift,
							),
						)
						return tree
					}
					// Construct the reference even when only one order subtest is selected.
					expected := writeTreeBytes(t, newTree(t, "sorted"))
					for _, order := range []string{"sorted", "random", "descending"} {
						t.Run(order, func(t *testing.T) {
							tree := newTree(t, order)
							for _, prefix := range addressOrderBenchmarkPrefixes(128, "sorted") {
								actualPrefix, value := tree.Get(prefix.Addr())
								expectedPrefix := netip.PrefixFrom(prefix.Addr(), 24-int(distribution.valueShift)).
									Masked()
								require.Equal(
									t,
									expectedPrefix,
									actualPrefix,
									"sibling merge boundary",
								)
								ip := prefix.Addr().As4()
								require.Equal(
									t,
									mmdbtype.Uint32((ip[2]>>distribution.valueShift)%16),
									value,
								)
							}
							require.NoError(t, tree.auditValueStore())
							actual := writeTreeBytes(t, tree)
							require.Equal(
								t,
								expected,
								actual,
								"insertion order must not change benchmark contents",
							)
						})
					}
				})
			}
		})
	}
}
