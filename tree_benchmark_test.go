package mmdbwriter

import (
	"bytes"
	"fmt"
	"io"
	"net/netip"
	"os"
	"slices"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type benchmarkInsertSpec struct {
	network netip.Prefix
	value   mmdbtype.DataType
}

type benchmarkRangeInsertSpec struct {
	start netip.Addr
	end   netip.Addr
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
	b.ReportAllocs()
	for b.Loop() {
		tree := newBenchmarkTree(b)
		insertBenchmarkSpecs(b, tree, specs)
	}

	// Loop's first call resets the timer, which deletes user-reported
	// metrics, so every custom metric is reported after the loop.
	reportOverlappingBenchmarkShape(b, specs, nil)
}

func BenchmarkTreeInsertTopLevelMergeOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkTopLevelMergeSpecs()
	b.ReportAllocs()
	for b.Loop() {
		tree := newBenchmarkTree(b)
		for _, spec := range specs {
			err := tree.InsertPureFunc(spec.network, spec.value, inserter.TopLevelMerge)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	reportOverlappingBenchmarkShape(b, specs, inserter.TopLevelMerge)
}

func BenchmarkTreeInsertDeepMergeOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkDeepMergeSpecs()
	b.ReportAllocs()
	for b.Loop() {
		tree := newBenchmarkTree(b)
		for _, spec := range specs {
			err := tree.InsertPureFunc(spec.network, spec.value, inserter.DeepMerge)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	reportOverlappingBenchmarkShape(b, specs, inserter.DeepMerge)
}

func BenchmarkTreeInsertDeepMergeFragmentedNetwork(b *testing.B) {
	const networkCount = 4_096
	values := benchmarkUniqueValues(benchmarkEnterpriseValue(), 16)
	overlay := mmdbtype.Map{
		"traits": mmdbtype.Map{"new_field": mmdbtype.String("overlay")},
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		tree := newBenchmarkTree(b)
		for index := range networkCount {
			address := netip.AddrFrom4(
				[4]byte{1, byte(index >> 16), byte(index >> 8), byte(index)},
			)
			if err := tree.Insert(
				netip.PrefixFrom(address, 32),
				values[index%len(values)],
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := tree.InsertPureFunc(
			netip.MustParsePrefix("1.0.0.0/20"),
			overlay,
			inserter.DeepMerge,
		); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(networkCount, "records/op")
}

func BenchmarkTreeInsertRangeFragmentedPasses(b *testing.B) {
	specs := fragmentedRangeBenchmarkSpecs()
	tree := newBenchmarkTree(b)
	insertRangeBenchmarkSpecs(b, tree, specs)
	tree.finalize()

	b.ReportAllocs()
	for b.Loop() {
		benchmarkTree := newBenchmarkTree(b)
		insertRangeBenchmarkSpecs(b, benchmarkTree, specs)
	}

	b.ReportMetric(float64(len(specs)), "ranges/op")
	b.ReportMetric(float64(tree.nodeCount), "nodes/op")
}

func BenchmarkTreeInsertChurnRepeatedPasses(b *testing.B) {
	const cycles = 8
	b.ReportAllocs()
	for b.Loop() {
		tree := newBenchmarkTree(b)
		insertChurnBenchmarkSpecs(b, tree, cycles)
	}

	reportChurnBenchmarkShape(b, cycles)
}

func BenchmarkTreeWriteToOverlappingPasses(b *testing.B) {
	specs := overlappingBenchmarkInsertSpecs()
	tree := newBenchmarkTree(b)
	insertBenchmarkSpecs(b, tree, specs)
	tree.finalize()

	b.ReportAllocs()
	for b.Loop() {
		_, err := tree.WriteTo(io.Discard)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(len(specs)), "insertions/tree")
	b.ReportMetric(float64(tree.nodeCount), "nodes/tree")
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

	b.ReportAllocs()
	for b.Loop() {
		loadedTree, err := Load(
			file.Name(),
			Options{
				IncludeReservedNetworks: true,
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		if loadedTree.nodeCountAllocated == 0 {
			b.Fatal("loaded tree has no nodes")
		}
	}

	b.ReportMetric(float64(len(specs)), "insertions/source")
}

// BenchmarkEnterpriseLoadThenOverlay models the production Enterprise build:
// load a City-scale source database and rewrite every record through several
// merge overlay passes. Overlay values are copied per insert because the
// production passes decode a fresh value for every source network; presenting
// one long-lived value per layer would overstate identity-cache hits. The
// network count must stay large enough to exercise the caches at realistic
// occupancy.
func BenchmarkEnterpriseLoadThenOverlay(b *testing.B) {
	base, overlays := enterpriseBenchmarkLayers(8_192)
	source := newBenchmarkTree(b)
	for _, spec := range base {
		if err := source.Insert(spec.network, spec.value); err != nil {
			b.Fatal(err)
		}
	}
	file, err := os.CreateTemp(b.TempDir(), "mmdbwriter-enterprise-*.mmdb")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := source.WriteTo(file); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		tree, err := Load(file.Name(), Options{IncludeReservedNetworks: true})
		if err != nil {
			b.Fatal(err)
		}
		for _, layer := range overlays {
			for _, spec := range layer {
				if err := tree.InsertPureFunc(
					spec.network,
					spec.value.Copy(),
					inserter.DeepMerge,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	b.ReportMetric(float64(len(base)), "networks/op")
	b.ReportMetric(float64(len(overlays)), "overlays/op")
}

// BenchmarkTreeInsertMetadataOverlayPasses compares a single unsorted metadata
// overlay with the equivalent sort, provenance comparison, and strip pass.
// The fixture has disjoint overlays that exercise both comparator outcomes.
func BenchmarkTreeInsertMetadataOverlayPasses(b *testing.B) {
	plainBase, provenanceBase, plainOverlays, provenanceOverlays := metadataOverlayBenchmarkSpecs(
		2_048,
	)
	plainSource := writeBenchmarkSource(b, plainBase)
	provenanceSource := writeBenchmarkSource(b, provenanceBase)

	metadataTree := loadMetadataFixture(b, plainSource)
	applyMetadataOverlays(b, metadataTree, plainOverlays)
	provenanceTree := loadMetadataFixture(b, provenanceSource)
	applyProvenanceOverlays(b, provenanceTree, provenanceOverlays)
	if !bytes.Equal(writeTreeBytes(b, metadataTree), writeTreeBytes(b, provenanceTree)) {
		b.Fatal("metadata and provenance benchmark fixtures produced different databases")
	}

	b.Run("metadata", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			tree := loadMetadataFixture(b, plainSource)
			b.StartTimer()
			applyMetadataOverlays(b, tree, plainOverlays)
		}
		b.ReportMetric(float64(len(plainOverlays)), "overlays/op")
	})

	b.Run("sort-provenance-strip", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			tree := loadMetadataFixture(b, provenanceSource)
			b.StartTimer()
			applyProvenanceOverlays(b, tree, provenanceOverlays)
		}
		b.ReportMetric(float64(len(provenanceOverlays)), "overlays/op")
	})
}

func metadataOverlayBenchmarkSpecs(groupCount int) (
	plainBase,
	provenanceBase,
	plainOverlays,
	provenanceOverlays []benchmarkInsertSpec,
) {
	plainBase = make([]benchmarkInsertSpec, 0, groupCount*3)
	provenanceBase = make([]benchmarkInsertSpec, 0, groupCount*3)
	plainOverlays = make([]benchmarkInsertSpec, 0, groupCount*2)
	provenanceOverlays = make([]benchmarkInsertSpec, 0, groupCount*2)
	for group := range groupCount {
		start := uint32(0x01000000 + group*4) // 1.0.0.0 and upward.
		for child := range 2 {
			prefix := netip.PrefixFrom(ipv4Addr(start+uint32(child)), 32)
			name := mmdbtype.String(fmt.Sprintf("base-specific-%d-%d", group, child))
			plainBase = append(plainBase, benchmarkInsertSpec{
				network: prefix,
				value:   mmdbtype.Map{"name": name},
			})
			provenanceBase = append(provenanceBase, benchmarkInsertSpec{
				network: prefix,
				value:   provenanceValue(name, 32),
			})
		}

		wideBasePrefix := netip.PrefixFrom(ipv4Addr(start+2), 31)
		wideBaseName := mmdbtype.String(fmt.Sprintf("base-wide-%d", group))
		plainBase = append(plainBase, benchmarkInsertSpec{
			network: wideBasePrefix,
			value:   mmdbtype.Map{"name": wideBaseName},
		})
		provenanceBase = append(provenanceBase, benchmarkInsertSpec{
			network: wideBasePrefix,
			value:   provenanceValue(wideBaseName, 31),
		})

		wideOverlayPrefix := netip.PrefixFrom(ipv4Addr(start), 31)
		wideOverlayName := mmdbtype.String(fmt.Sprintf("overlay-wide-%d", group))
		plainOverlays = append(plainOverlays, benchmarkInsertSpec{
			network: wideOverlayPrefix,
			value:   mmdbtype.Map{"name": wideOverlayName},
		})
		provenanceOverlays = append(provenanceOverlays, benchmarkInsertSpec{
			network: wideOverlayPrefix,
			value:   provenanceValue(wideOverlayName, 31),
		})

		specificOverlayPrefix := netip.PrefixFrom(ipv4Addr(start+2), 32)
		specificOverlayName := mmdbtype.String(fmt.Sprintf("overlay-specific-%d", group))
		plainOverlays = append(plainOverlays, benchmarkInsertSpec{
			network: specificOverlayPrefix,
			value:   mmdbtype.Map{"name": specificOverlayName},
		})
		provenanceOverlays = append(provenanceOverlays, benchmarkInsertSpec{
			network: specificOverlayPrefix,
			value:   provenanceValue(specificOverlayName, 32),
		})
	}
	return plainBase, provenanceBase, plainOverlays, provenanceOverlays
}

func writeBenchmarkSource(
	b *testing.B,
	specs []benchmarkInsertSpec,
) string {
	b.Helper()
	tree, err := New(Options{
		BuildEpoch:              1,
		DatabaseType:            "metadata-overlay",
		Description:             map[string]string{"en": "Metadata overlay benchmark"},
		IPVersion:               4,
		IncludeReservedNetworks: true,
		RecordSize:              24,
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, spec := range specs {
		if err := tree.Insert(spec.network, spec.value); err != nil {
			b.Fatal(err)
		}
	}
	file, err := os.CreateTemp(b.TempDir(), "mmdbwriter-metadata-overlay-*.mmdb")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := tree.WriteTo(file); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	return file.Name()
}

// applyMetadataOverlays inserts the overlays in one unsorted pass, letting the
// inserter compare the inserted network with the existing record.
func applyMetadataOverlays(
	tb testing.TB,
	tree *Tree,
	overlays []benchmarkInsertSpec,
) {
	tb.Helper()
	for _, spec := range overlays {
		if err := tree.InsertFunc(
			spec.network,
			spec.value,
			metadataSpecificityInserter,
		); err != nil {
			tb.Fatal(err)
		}
	}
}

// applyProvenanceOverlays is the pre-metadata equivalent: sort by prefix
// length, compare against a prefix length carried in the value, then strip it.
func applyProvenanceOverlays(
	tb testing.TB,
	tree *Tree,
	overlays []benchmarkInsertSpec,
) {
	tb.Helper()
	sorted := slices.Clone(overlays)
	slices.SortFunc(sorted, func(left, right benchmarkInsertSpec) int {
		return right.network.Bits() - left.network.Bits()
	})
	for _, spec := range sorted {
		if err := tree.InsertFunc(
			spec.network,
			spec.value,
			provenanceSpecificityInserter,
		); err != nil {
			tb.Fatal(err)
		}
	}
	stripProvenance(tb, tree)
}

func enterpriseBenchmarkLayers(
	networkCount int,
) ([]benchmarkInsertSpec, [][]benchmarkInsertSpec) {
	base := make([]benchmarkInsertSpec, networkCount)
	overlays := make([][]benchmarkInsertSpec, 4)
	for index := range overlays {
		overlays[index] = make([]benchmarkInsertSpec, networkCount)
	}
	for index := range networkCount {
		address := netip.AddrFrom4([4]byte{1, byte(index >> 16), byte(index >> 8), byte(index)})
		prefix := netip.PrefixFrom(address, 32)
		base[index] = benchmarkInsertSpec{
			network: prefix,
			value: benchmarkDeepMergeValue(
				[]string{"GB", "US", "DE", "JP"}[index%4],
				"city",
				uint16(index%100),
			),
		}
		overlays[0][index] = benchmarkInsertSpec{network: prefix, value: mmdbtype.Map{
			"traits": mmdbtype.Map{
				"confidence": mmdbtype.Uint16(index % 100),
			},
		}}
		overlays[1][index] = benchmarkInsertSpec{network: prefix, value: mmdbtype.Map{
			"traits": mmdbtype.Map{"user_type": mmdbtype.String("business")},
		}}
		overlays[2][index] = benchmarkInsertSpec{network: prefix, value: mmdbtype.Map{
			"traits": mmdbtype.Map{"isp": mmdbtype.String("Example ISP")},
		}}
		overlays[3][index] = benchmarkInsertSpec{network: prefix, value: mmdbtype.Map{
			"traits": mmdbtype.Map{"domain": mmdbtype.String("example.test")},
		}}
	}
	return base, overlays
}

func reportOverlappingBenchmarkShape(
	b *testing.B,
	specs []benchmarkInsertSpec,
	pureFunc inserter.PureFunc,
) {
	b.Helper()

	tree := newBenchmarkTree(b)
	if pureFunc == nil {
		insertBenchmarkSpecs(b, tree, specs)
	} else {
		for _, spec := range specs {
			if err := tree.InsertPureFunc(spec.network, spec.value, pureFunc); err != nil {
				b.Fatal(err)
			}
		}
	}
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
			if err := tree.InsertPureFunc(removePrefix, nil, inserter.Remove); err != nil {
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

func benchmarkCIDR(cidr string) netip.Prefix {
	return netip.MustParsePrefix(cidr)
}

func benchmarkIP(ip string) netip.Addr {
	return netip.MustParseAddr(ip)
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
