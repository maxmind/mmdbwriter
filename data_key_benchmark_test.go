package mmdbwriter

import (
	"maps"
	"net/netip"
	"strconv"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func BenchmarkDataHasherEnterpriseValue(b *testing.B) {
	value := benchmarkEnterpriseValue()
	b.Run("sha256", func(b *testing.B) {
		writer := newKeyWriter()
		b.ReportAllocs()
		for range b.N {
			if _, err := writer.Key(value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("structural", func(b *testing.B) {
		hasher := newDataHasher()
		b.ReportAllocs()
		for range b.N {
			if _, err := hasher.Hash(value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDataHasherEnterpriseDeepCopies(b *testing.B) {
	const valueCount = 512
	value := benchmarkEnterpriseValue()
	values := make([]mmdbtype.DataType, valueCount)
	for index := range values {
		values[index] = value.Copy()
	}
	b.Run("sha256", func(b *testing.B) {
		writer := newKeyWriter()
		b.ReportAllocs()
		for index := range b.N {
			if _, err := writer.Key(values[index%len(values)]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("structural", func(b *testing.B) {
		hasher := newDataHasher()
		b.ReportAllocs()
		for index := range b.N {
			if _, err := hasher.Hash(values[index%len(values)]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEnterpriseKeyPipeline(b *testing.B) {
	const networkCount = 2_048
	base := benchmarkUniqueValues(benchmarkEnterpriseValue(), networkCount)
	overlays := []mmdbtype.DataType{
		mmdbtype.Map{"traits": mmdbtype.Map{"connection_type": mmdbtype.String("Corporate")}},
		mmdbtype.Map{"traits": mmdbtype.Map{"user_type": mmdbtype.String("business")}},
		mmdbtype.Map{"traits": mmdbtype.Map{"isp": mmdbtype.String("Example ISP")}},
		mmdbtype.Map{"traits": mmdbtype.Map{"domain": mmdbtype.String("overlay.test")}},
	}
	b.ReportMetric(float64(networkCount*(1+len(overlays))), "insertions/op")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
		if err != nil {
			b.Fatal(err)
		}
		for index, value := range base {
			address := netip.AddrFrom4([4]byte{1, byte(index >> 16), byte(index >> 8), byte(index)})
			prefix := netip.PrefixFrom(address, 32)
			if err := tree.Insert(prefix, value); err != nil {
				b.Fatal(err)
			}
			for _, overlay := range overlays {
				if err := tree.InsertFunc(prefix, overlay, inserter.DeepMerge); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func BenchmarkDataMapEnterpriseValue(b *testing.B) {
	value := benchmarkEnterpriseValue()

	b.Run("equal-shared-nested", func(b *testing.B) {
		values := benchmarkShallowCopies(value, 8_192)
		dataMap := newDataMap()
		canonical, err := dataMap.storeWithIdentity(value)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { dataMap.remove(canonical) })
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			stored, err := dataMap.storeWithIdentity(values[i%len(values)])
			if err != nil {
				b.Fatal(err)
			}
			dataMap.remove(stored)
		}
	})

	b.Run("equal-deep-copy", func(b *testing.B) {
		const valueCount = 512
		values := make([]mmdbtype.DataType, valueCount)
		for index := range values {
			values[index] = value.Copy()
		}
		dataMap := newDataMap()
		canonical, err := dataMap.storeWithIdentity(value)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { dataMap.remove(canonical) })
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			stored, err := dataMap.storeWithIdentity(values[i%len(values)])
			if err != nil {
				b.Fatal(err)
			}
			dataMap.remove(stored)
		}
	})

	b.Run("unique-miss", func(b *testing.B) {
		values := benchmarkUniqueValues(value, 8_192)
		dataMap := newDataMap()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			stored, err := dataMap.storeWithIdentity(values[i%len(values)])
			if err != nil {
				b.Fatal(err)
			}
			dataMap.remove(stored)
		}
	})
}

func BenchmarkWireDataEqualEnterpriseValue(b *testing.B) {
	value := benchmarkEnterpriseValue()
	b.Run("shared-nested", func(b *testing.B) {
		clone := benchmarkShallowCopies(value, 1)[0]
		b.ReportAllocs()
		for range b.N {
			if !wireDataEqual(value, clone) {
				b.Fatal("values do not compare equal")
			}
		}
	})
	b.Run("deep-copy", func(b *testing.B) {
		clone := value.Copy()
		b.ReportAllocs()
		for range b.N {
			if !wireDataEqual(value, clone) {
				b.Fatal("values do not compare equal")
			}
		}
	})
}

func BenchmarkTreeInsertEnterpriseDedupRates(b *testing.B) {
	const networkCount = 4_608
	for _, hitRate := range []int{0, 50, 90, 99} {
		b.Run(strconv.Itoa(hitRate)+"%-hits", func(b *testing.B) {
			values := benchmarkEnterpriseValuesAtHitRate(networkCount, hitRate)
			b.ReportMetric(networkCount, "insertions/op")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
				if err != nil {
					b.Fatal(err)
				}
				for index, value := range values {
					address := netip.AddrFrom4(
						[4]byte{1, byte(index >> 16), byte(index >> 8), byte(index)},
					)
					if err := tree.Insert(netip.PrefixFrom(address, 32), value); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func benchmarkEnterpriseValuesAtHitRate(count, hitRate int) []mmdbtype.DataType {
	uniqueCount := max(1, count*(100-hitRate)/100)
	uniqueValues := benchmarkUniqueValues(benchmarkEnterpriseValue(), uniqueCount)
	values := make([]mmdbtype.DataType, count)
	for index := range values {
		values[index] = uniqueValues[index%uniqueCount].Copy()
	}
	return values
}

func benchmarkShallowCopies(value mmdbtype.Map, count int) []mmdbtype.DataType {
	values := make([]mmdbtype.DataType, count)
	for i := range values {
		clone := make(mmdbtype.Map, len(value))
		maps.Copy(clone, value)
		values[i] = clone
	}
	return values
}

func benchmarkUniqueValues(value mmdbtype.Map, count int) []mmdbtype.DataType {
	values := benchmarkShallowCopies(value, count)
	baseTraits := value["traits"].(mmdbtype.Map)
	for i, data := range values {
		traits := make(mmdbtype.Map, len(baseTraits))
		maps.Copy(traits, baseTraits)
		traits["domain"] = mmdbtype.String("example-" + strconv.Itoa(i) + ".test")
		data.(mmdbtype.Map)["traits"] = traits
	}
	return values
}

func benchmarkEnterpriseValue() mmdbtype.Map {
	names := mmdbtype.Map{
		"de":    mmdbtype.String("Vereinigtes Koenigreich"),
		"en":    mmdbtype.String("United Kingdom"),
		"es":    mmdbtype.String("Reino Unido"),
		"fr":    mmdbtype.String("Royaume-Uni"),
		"ja":    mmdbtype.String("United Kingdom"),
		"pt-BR": mmdbtype.String("Reino Unido"),
		"ru":    mmdbtype.String("United Kingdom"),
		"zh-CN": mmdbtype.String("United Kingdom"),
	}
	return mmdbtype.Map{
		"city": mmdbtype.Map{
			"confidence": mmdbtype.Uint16(90),
			"geoname_id": mmdbtype.Uint32(2643743),
			"names":      names,
		},
		"continent": mmdbtype.Map{
			"code":       mmdbtype.String("EU"),
			"geoname_id": mmdbtype.Uint32(6255148),
			"names":      names,
		},
		"country": mmdbtype.Map{
			"confidence": mmdbtype.Uint16(95),
			"geoname_id": mmdbtype.Uint32(2635167),
			"iso_code":   mmdbtype.String("GB"),
			"names":      names,
		},
		"location": mmdbtype.Map{
			"accuracy_radius": mmdbtype.Uint16(10),
			"latitude":        mmdbtype.Float64(51.5074),
			"longitude":       mmdbtype.Float64(-0.1278),
			"metro_code":      mmdbtype.Uint16(0),
			"time_zone":       mmdbtype.String("Europe/London"),
		},
		"location_prefix_length": mmdbtype.Uint16(24),
		"postal": mmdbtype.Map{
			"code":       mmdbtype.String("EC1A"),
			"confidence": mmdbtype.Uint16(80),
		},
		"registered_country": mmdbtype.Map{
			"geoname_id": mmdbtype.Uint32(2635167),
			"iso_code":   mmdbtype.String("GB"),
			"names":      names,
		},
		"represented_country": mmdbtype.Map{
			"geoname_id": mmdbtype.Uint32(2635167),
			"iso_code":   mmdbtype.String("GB"),
			"names":      names,
		},
		"subdivisions": mmdbtype.Slice{
			mmdbtype.Map{
				"confidence": mmdbtype.Uint16(75),
				"geoname_id": mmdbtype.Uint32(6269131),
				"iso_code":   mmdbtype.String("ENG"),
				"names":      names,
			},
			mmdbtype.Map{
				"geoname_id": mmdbtype.Uint32(3333121),
				"iso_code":   mmdbtype.String("LND"),
				"names":      names,
			},
		},
		"traits": mmdbtype.Map{
			"autonomous_system_number":       mmdbtype.Uint32(64512),
			"autonomous_system_organization": mmdbtype.String("Example AS"),
			"connection_type":                mmdbtype.String("Cable/DSL"),
			"domain":                         mmdbtype.String("example.test"),
			"is_anonymous_proxy":             mmdbtype.Bool(false),
			"is_anycast":                     mmdbtype.Bool(false),
			"is_legitimate_proxy":            mmdbtype.Bool(false),
			"isp":                            mmdbtype.String("Example ISP"),
			"mobile_country_code":            mmdbtype.String("234"),
			"mobile_network_code":            mmdbtype.String("15"),
			"organization":                   mmdbtype.String("Example Organization"),
			"user_type":                      mmdbtype.String("business"),
		},
	}
}
