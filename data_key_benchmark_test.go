package mmdbwriter

import (
	"testing"

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
	b.Run("maphash", func(b *testing.B) {
		hasher := newDataHasher()
		b.ReportAllocs()
		for range b.N {
			if _, err := hasher.Hash(value); err != nil {
				b.Fatal(err)
			}
		}
	})
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
		"postal": mmdbtype.Map{
			"code":       mmdbtype.String("EC1A"),
			"confidence": mmdbtype.Uint16(80),
		},
		"registered_country": mmdbtype.Map{
			"geoname_id": mmdbtype.Uint32(2635167),
			"iso_code":   mmdbtype.String("GB"),
			"names":      names,
		},
		"subdivisions": mmdbtype.Slice{mmdbtype.Map{
			"confidence": mmdbtype.Uint16(75),
			"geoname_id": mmdbtype.Uint32(6269131),
			"iso_code":   mmdbtype.String("ENG"),
			"names":      names,
		}},
		"traits": mmdbtype.Map{
			"autonomous_system_number":       mmdbtype.Uint32(64512),
			"autonomous_system_organization": mmdbtype.String("Example AS"),
			"connection_type":                mmdbtype.String("Cable/DSL"),
			"domain":                         mmdbtype.String("example.test"),
			"is_anonymous_proxy":             mmdbtype.Bool(false),
			"is_anycast":                     mmdbtype.Bool(false),
			"is_legitimate_proxy":            mmdbtype.Bool(false),
			"isp":                            mmdbtype.String("Example ISP"),
			"organization":                   mmdbtype.String("Example Organization"),
			"user_type":                      mmdbtype.String("business"),
		},
	}
}
