package mmdbwriter

import (
	"fmt"
	"io"
	"net"
	"os"
	"testing"

	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

type benchmarkInsertSpec struct {
	network *net.IPNet
	value   mmdbtype.DataType
}

type benchmarkRangeInsertSpec struct {
	start net.IP
	end   net.IP
	value mmdbtype.DataType
}

type benchmarkValueSets struct {
	base     []mmdbtype.DataType
	specific []mmdbtype.DataType
	override []mmdbtype.DataType
	refresh  []mmdbtype.DataType
}

func BenchmarkTreeInsertOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkInsertSpecs()
	reportOverlappingBenchmarkShape(b, specs)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		tree := newBenchmarkTree(b)
		insertBenchmarkSpecs(b, tree, specs)
	}
}

func BenchmarkTreeInsertTopLevelMergeOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkTopLevelMergeSpecs()
	reportOverlappingBenchmarkShape(b, specs)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		tree := newBenchmarkTree(b)
		for _, spec := range specs {
			err := tree.InsertFunc(spec.network, inserter.TopLevelMergeWith(spec.value))
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTreeInsertDeepMergeOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkDeepMergeSpecs()
	reportOverlappingBenchmarkShape(b, specs)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		tree := newBenchmarkTree(b)
		for _, spec := range specs {
			err := tree.InsertFunc(spec.network, inserter.DeepMergeWith(spec.value))
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTreeInsertRangeFragmentedPasses(b *testing.B) {
	specs := fragmentedRangeBenchmarkSpecs()
	b.ReportMetric(float64(len(specs)), "ranges/op")

	tree := newBenchmarkTree(b)
	insertRangeBenchmarkSpecs(b, tree, specs)
	tree.finalize()
	b.ReportMetric(float64(tree.nodeCount), "nodes/op")

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		benchmarkTree := newBenchmarkTree(b)
		insertRangeBenchmarkSpecs(b, benchmarkTree, specs)
	}
}

func BenchmarkTreeInsertChurnRepeatedPasses(b *testing.B) {
	const cycles = 8
	reportChurnBenchmarkShape(b, cycles)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		tree := newBenchmarkTree(b)
		insertChurnBenchmarkSpecs(b, tree, cycles)
	}
}

func BenchmarkTreeWriteToOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkInsertSpecs()
	tree := newBenchmarkTree(b)
	insertBenchmarkSpecs(b, tree, specs)
	tree.finalize()

	b.ReportMetric(float64(len(specs)), "insertions/tree")
	b.ReportMetric(float64(tree.nodeCount), "nodes/tree")
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, err := tree.WriteTo(io.Discard)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTreeLoadOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkInsertSpecs()
	tree := newBenchmarkTree(b)
	insertBenchmarkSpecs(b, tree, specs)

	file, err := os.CreateTemp(b.TempDir(), "mmdbwriter-benchmark-*.mmdb")
	if err != nil {
		b.Fatal(err)
	}

	_, err = tree.WriteTo(file)
	if err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(len(specs)), "insertions/source")
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		loadedTree, err := Load(
			file.Name(),
			Options{
				IncludeReservedNetworks: true,
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		if loadedTree.root == nil {
			b.Fatal("loaded tree has nil root")
		}
	}
}

func reportOverlappingBenchmarkShape(b *testing.B, specs []benchmarkInsertSpec) {
	b.Helper()

	tree := newBenchmarkTree(b)
	insertBenchmarkSpecs(b, tree, specs)
	tree.finalize()

	b.ReportMetric(float64(len(specs)), "insertions/op")
	b.ReportMetric(float64(tree.nodeCount), "nodes/op")
}

func reportChurnBenchmarkShape(b *testing.B, cycles int) {
	b.Helper()

	tree := newBenchmarkTree(b)
	insertChurnBenchmarkSpecs(b, tree, cycles)
	allocatedNodes := tree.nodeCountAllocated
	allocatedPaths := len(tree.paths)
	tree.finalize()

	b.ReportMetric(float64(cycles), "cycles/op")
	b.ReportMetric(float64(tree.nodeCount), "nodes/op")
	b.ReportMetric(float64(allocatedNodes), "allocated_nodes/op")
	b.ReportMetric(float64(allocatedPaths), "allocated_paths/op")
}

func newBenchmarkTree(b *testing.B) *Tree {
	b.Helper()

	tree, err := New(
		Options{
			IPVersion:               4,
			IncludeReservedNetworks: true,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	return tree
}

func insertChurnBenchmarkSpecs(b *testing.B, tree *Tree, cycles int) {
	b.Helper()

	values := benchmarkValueSets{
		base:     benchmarkBaseValues(),
		specific: benchmarkSpecificValues(),
		override: benchmarkOverrideValues(),
		refresh:  benchmarkRefreshValues(),
	}

	for cycle := range cycles {
		firstOctet := 11 + cycle
		base := values.base[cycle%len(values.base)]
		if err := tree.Insert(
			benchmarkCIDR(fmt.Sprintf("%d.0.0.0/16", firstOctet)),
			base,
		); err != nil {
			b.Fatal(err)
		}

		for specific := range 16 {
			secondOctet := specific * 16
			value := values.specific[(cycle+specific)%len(values.specific)]
			prefix := benchmarkCIDR(fmt.Sprintf("%d.%d.0.0/24", firstOctet, secondOctet))
			if err := tree.Insert(prefix, value); err != nil {
				b.Fatal(err)
			}
		}

		for specific := range 16 {
			secondOctet := specific * 16
			removePrefix := benchmarkCIDR(fmt.Sprintf("%d.%d.0.128/25", firstOctet, secondOctet))
			if err := tree.InsertFunc(removePrefix, nil, inserter.Remove); err != nil {
				b.Fatal(err)
			}

			value := values.override[(cycle+specific)%len(values.override)]
			replacePrefix := benchmarkCIDR(fmt.Sprintf("%d.%d.0.0/25", firstOctet, secondOctet))
			if err := tree.Insert(replacePrefix, value); err != nil {
				b.Fatal(err)
			}
		}

		for specific := range 16 {
			secondOctet := specific * 16
			value := values.refresh[(cycle+specific)%len(values.refresh)]
			prefix := benchmarkCIDR(fmt.Sprintf("%d.%d.0.0/24", firstOctet, secondOctet))
			if err := tree.Insert(prefix, value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func insertBenchmarkSpecs(b *testing.B, tree *Tree, specs []benchmarkInsertSpec) {
	b.Helper()

	for _, spec := range specs {
		if err := tree.Insert(spec.network, spec.value); err != nil {
			b.Fatal(err)
		}
	}
}

func insertRangeBenchmarkSpecs(
	b *testing.B,
	tree *Tree,
	specs []benchmarkRangeInsertSpec,
) {
	b.Helper()

	for _, spec := range specs {
		if err := tree.InsertRange(spec.start, spec.end, spec.value); err != nil {
			b.Fatal(err)
		}
	}
}

func overlappingBenchmarkInsertSpecs() []benchmarkInsertSpec {
	return overlappingBenchmarkSpecs(
		benchmarkValueSets{
			base:     benchmarkBaseValues(),
			specific: benchmarkSpecificValues(),
			override: benchmarkOverrideValues(),
			refresh:  benchmarkRefreshValues(),
		},
	)
}

func overlappingBenchmarkTopLevelMergeSpecs() []benchmarkInsertSpec {
	return overlappingBenchmarkSpecs(
		benchmarkValueSets{
			base:     benchmarkTopLevelMergeBaseValues(),
			specific: benchmarkTopLevelMergeSpecificValues(),
			override: benchmarkTopLevelMergeOverrideValues(),
			refresh:  benchmarkTopLevelMergeRefreshValues(),
		},
	)
}

func overlappingBenchmarkDeepMergeSpecs() []benchmarkInsertSpec {
	return overlappingBenchmarkSpecs(
		benchmarkValueSets{
			base:     benchmarkDeepMergeBaseValues(),
			specific: benchmarkDeepMergeSpecificValues(),
			override: benchmarkDeepMergeOverrideValues(),
			refresh:  benchmarkDeepMergeRefreshValues(),
		},
	)
}

func overlappingBenchmarkSpecs(values benchmarkValueSets) []benchmarkInsertSpec {
	const (
		largeNetworks        = 64
		specificsPerNetwork  = 16
		specificOctetSpacing = 16
		firstLargeOctet      = 11
	)

	// Five passes:
	// 1. Insert broad /16 values.
	// 2. Insert /24 values inside each /16, splitting broad data records.
	// 3. Insert upper /25 overrides, splitting /24 data records.
	// 4. Insert matching lower /25 overrides, allowing child records to merge.
	// 5. Refresh the same /24 networks with new values.
	specs := make([]benchmarkInsertSpec, 0, largeNetworks*(1+4*specificsPerNetwork))
	for large := range largeNetworks {
		firstOctet := firstLargeOctet + large
		specs = append(specs, benchmarkInsertSpec{
			network: benchmarkCIDR(fmt.Sprintf("%d.0.0.0/16", firstOctet)),
			value:   values.base[large%len(values.base)],
		})
	}

	for large := range largeNetworks {
		firstOctet := firstLargeOctet + large
		for specific := range specificsPerNetwork {
			secondOctet := specific * specificOctetSpacing
			specs = append(specs, benchmarkInsertSpec{
				network: benchmarkCIDR(fmt.Sprintf("%d.%d.0.0/24", firstOctet, secondOctet)),
				value:   values.specific[(large+specific)%len(values.specific)],
			})
		}
	}

	for large := range largeNetworks {
		firstOctet := firstLargeOctet + large
		for specific := range specificsPerNetwork {
			secondOctet := specific * specificOctetSpacing
			value := values.override[(large+specific)%len(values.override)]
			specs = append(specs, benchmarkInsertSpec{
				network: benchmarkCIDR(fmt.Sprintf("%d.%d.0.128/25", firstOctet, secondOctet)),
				value:   value,
			})
		}
	}

	for large := range largeNetworks {
		firstOctet := firstLargeOctet + large
		for specific := range specificsPerNetwork {
			secondOctet := specific * specificOctetSpacing
			value := values.override[(large+specific)%len(values.override)]
			specs = append(specs, benchmarkInsertSpec{
				network: benchmarkCIDR(fmt.Sprintf("%d.%d.0.0/25", firstOctet, secondOctet)),
				value:   value,
			})
		}
	}

	for large := range largeNetworks {
		firstOctet := firstLargeOctet + large
		for specific := range specificsPerNetwork {
			secondOctet := specific * specificOctetSpacing
			specs = append(specs, benchmarkInsertSpec{
				network: benchmarkCIDR(fmt.Sprintf("%d.%d.0.0/24", firstOctet, secondOctet)),
				value:   values.refresh[(large+specific)%len(values.refresh)],
			})
		}
	}

	return specs
}

func fragmentedRangeBenchmarkSpecs() []benchmarkRangeInsertSpec {
	const (
		rangeGroups     = 96
		firstLargeOctet = 81
	)

	values := benchmarkSpecificValues()
	specs := make([]benchmarkRangeInsertSpec, 0, 3*rangeGroups)

	for group := range rangeGroups {
		firstOctet := firstLargeOctet + group/4
		secondOctet := (group % 4) * 48
		specs = append(specs,
			benchmarkRangeInsertSpec{
				start: benchmarkIP(fmt.Sprintf("%d.%d.0.3", firstOctet, secondOctet)),
				end:   benchmarkIP(fmt.Sprintf("%d.%d.3.197", firstOctet, secondOctet)),
				value: values[group%len(values)],
			},
			benchmarkRangeInsertSpec{
				start: benchmarkIP(fmt.Sprintf("%d.%d.4.63", firstOctet, secondOctet)),
				end:   benchmarkIP(fmt.Sprintf("%d.%d.7.241", firstOctet, secondOctet)),
				value: values[(group+1)%len(values)],
			},
			benchmarkRangeInsertSpec{
				start: benchmarkIP(fmt.Sprintf("%d.%d.8.15", firstOctet, secondOctet)),
				end:   benchmarkIP(fmt.Sprintf("%d.%d.15.239", firstOctet, secondOctet)),
				value: values[(group+2)%len(values)],
			},
		)
	}

	return specs
}

func benchmarkCIDR(cidr string) *net.IPNet {
	//nolint:forbidigo // benchmarks exercise the existing net.IPNet API.
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	return network
}

func benchmarkIP(ip string) net.IP {
	//nolint:forbidigo // benchmarks exercise the existing net.IP API.
	parsed := net.ParseIP(ip)
	if parsed == nil {
		panic("invalid benchmark IP: " + ip)
	}
	return parsed
}

func benchmarkBaseValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		benchmarkRecord("base-a", 1, "en"),
		benchmarkRecord("base-b", 2, "en"),
		benchmarkRecord("base-c", 3, "fr"),
		benchmarkRecord("base-d", 4, "es"),
	}
}

func benchmarkSpecificValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		benchmarkRecord("specific-a", 101, "en"),
		benchmarkRecord("specific-b", 102, "de"),
		benchmarkRecord("specific-c", 103, "ja"),
		benchmarkRecord("specific-d", 104, "pt-BR"),
		benchmarkRecord("specific-e", 105, "zh-CN"),
		benchmarkRecord("specific-f", 106, "en"),
	}
}

func benchmarkOverrideValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		benchmarkRecord("override-a", 201, "en"),
		benchmarkRecord("override-b", 202, "fr"),
		benchmarkRecord("override-c", 203, "es"),
		benchmarkRecord("override-d", 204, "de"),
	}
}

func benchmarkRefreshValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		benchmarkRecord("refresh-a", 301, "en"),
		benchmarkRecord("refresh-b", 302, "fr"),
		benchmarkRecord("refresh-c", 303, "es"),
		benchmarkRecord("refresh-d", 304, "de"),
		benchmarkRecord("refresh-e", 305, "ja"),
	}
}

func benchmarkRecord(label string, id uint32, locale string) mmdbtype.Map {
	return mmdbtype.Map{
		"id":     mmdbtype.Uint32(id),
		"label":  mmdbtype.String(label),
		"locale": mmdbtype.String(locale),
		"names": mmdbtype.Map{
			mmdbtype.String(locale): mmdbtype.String(label),
			"en":                    mmdbtype.String(label + "-en"),
		},
		"traits": mmdbtype.Map{
			"rank":        mmdbtype.Uint16(uint16(id % 100)),
			"represented": mmdbtype.Bool(id%2 == 0),
		},
	}
}

func benchmarkTopLevelMergeBaseValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{"country": mmdbtype.String("US"), "source": mmdbtype.String("base-a")},
		mmdbtype.Map{"country": mmdbtype.String("CA"), "source": mmdbtype.String("base-b")},
		mmdbtype.Map{"country": mmdbtype.String("GB"), "source": mmdbtype.String("base-c")},
		mmdbtype.Map{"country": mmdbtype.String("DE"), "source": mmdbtype.String("base-d")},
	}
}

func benchmarkTopLevelMergeSpecificValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{"region": mmdbtype.String("north"), "confidence": mmdbtype.Uint16(80)},
		mmdbtype.Map{"region": mmdbtype.String("south"), "confidence": mmdbtype.Uint16(81)},
		mmdbtype.Map{"region": mmdbtype.String("east"), "confidence": mmdbtype.Uint16(82)},
		mmdbtype.Map{"region": mmdbtype.String("west"), "confidence": mmdbtype.Uint16(83)},
	}
}

func benchmarkTopLevelMergeOverrideValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{"isp": mmdbtype.String("isp-a"), "network_type": mmdbtype.String("business")},
		mmdbtype.Map{"isp": mmdbtype.String("isp-b"), "network_type": mmdbtype.String("hosting")},
		mmdbtype.Map{"isp": mmdbtype.String("isp-c"), "network_type": mmdbtype.String("mobile")},
		mmdbtype.Map{
			"isp":          mmdbtype.String("isp-d"),
			"network_type": mmdbtype.String("residential"),
		},
	}
}

func benchmarkTopLevelMergeRefreshValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{"accuracy_radius": mmdbtype.Uint16(5), "source": mmdbtype.String("refresh-a")},
		mmdbtype.Map{
			"accuracy_radius": mmdbtype.Uint16(10),
			"source":          mmdbtype.String("refresh-b"),
		},
		mmdbtype.Map{
			"accuracy_radius": mmdbtype.Uint16(20),
			"source":          mmdbtype.String("refresh-c"),
		},
		mmdbtype.Map{
			"accuracy_radius": mmdbtype.Uint16(50),
			"source":          mmdbtype.String("refresh-d"),
		},
	}
}

func benchmarkDeepMergeBaseValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		benchmarkDeepMergeValue("US", "base-a", 10),
		benchmarkDeepMergeValue("CA", "base-b", 20),
		benchmarkDeepMergeValue("GB", "base-c", 30),
		benchmarkDeepMergeValue("DE", "base-d", 40),
	}
}

func benchmarkDeepMergeSpecificValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{
			"geo": mmdbtype.Map{
				"region": mmdbtype.String("north"),
				"city":   mmdbtype.String("specific-a"),
			},
			"traits": mmdbtype.Map{"confidence": mmdbtype.Uint16(80)},
		},
		mmdbtype.Map{
			"geo": mmdbtype.Map{
				"region": mmdbtype.String("south"),
				"city":   mmdbtype.String("specific-b"),
			},
			"traits": mmdbtype.Map{"confidence": mmdbtype.Uint16(81)},
		},
		mmdbtype.Map{
			"geo": mmdbtype.Map{
				"region": mmdbtype.String("east"),
				"city":   mmdbtype.String("specific-c"),
			},
			"traits": mmdbtype.Map{"confidence": mmdbtype.Uint16(82)},
		},
		mmdbtype.Map{
			"geo": mmdbtype.Map{
				"region": mmdbtype.String("west"),
				"city":   mmdbtype.String("specific-d"),
			},
			"traits": mmdbtype.Map{"confidence": mmdbtype.Uint16(83)},
		},
	}
}

func benchmarkDeepMergeOverrideValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{
			"traits": mmdbtype.Map{
				"isp":          mmdbtype.String("isp-a"),
				"network_type": mmdbtype.String("business"),
			},
		},
		mmdbtype.Map{
			"traits": mmdbtype.Map{
				"isp":          mmdbtype.String("isp-b"),
				"network_type": mmdbtype.String("hosting"),
			},
		},
		mmdbtype.Map{
			"traits": mmdbtype.Map{
				"isp":          mmdbtype.String("isp-c"),
				"network_type": mmdbtype.String("mobile"),
			},
		},
		mmdbtype.Map{
			"traits": mmdbtype.Map{
				"isp":          mmdbtype.String("isp-d"),
				"network_type": mmdbtype.String("residential"),
			},
		},
	}
}

func benchmarkDeepMergeRefreshValues() []mmdbtype.DataType {
	return []mmdbtype.DataType{
		mmdbtype.Map{
			"geo":    mmdbtype.Map{"accuracy_radius": mmdbtype.Uint16(5)},
			"source": mmdbtype.String("refresh-a"),
		},
		mmdbtype.Map{
			"geo":    mmdbtype.Map{"accuracy_radius": mmdbtype.Uint16(10)},
			"source": mmdbtype.String("refresh-b"),
		},
		mmdbtype.Map{
			"geo":    mmdbtype.Map{"accuracy_radius": mmdbtype.Uint16(20)},
			"source": mmdbtype.String("refresh-c"),
		},
		mmdbtype.Map{
			"geo":    mmdbtype.Map{"accuracy_radius": mmdbtype.Uint16(50)},
			"source": mmdbtype.String("refresh-d"),
		},
	}
}

func benchmarkDeepMergeValue(country, source string, rank uint16) mmdbtype.Map {
	return mmdbtype.Map{
		"geo": mmdbtype.Map{
			"country": mmdbtype.String(country),
			"names": mmdbtype.Map{
				"en": mmdbtype.String(country),
			},
		},
		"source": mmdbtype.String(source),
		"traits": mmdbtype.Map{
			"rank":        mmdbtype.Uint16(rank),
			"represented": mmdbtype.Bool(rank%20 == 0),
		},
	}
}
