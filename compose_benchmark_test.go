package mmdbwriter

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func BenchmarkComposeLayers(b *testing.B) {
	specs := make([][]benchmarkInsertSpec, 5)
	for layer := range 5 {
		for i := range 1024 {
			specs[layer] = append(specs[layer], benchmarkInsertSpec{
				network: netip.PrefixFrom(
					netip.AddrFrom4([4]byte{1, byte(i >> 8), byte(i), 0}),
					24+layer,
				),
				value: mmdbtype.Map{
					mmdbtype.String(fmt.Sprintf("layer%d", layer)): mmdbtype.String(
						fmt.Sprintf("value%d", i%128),
					),
				},
			})
		}
	}
	benchmarkComposition(
		b,
		specs,
		inserter.TopLevelMerge,
		func(_ netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
			result := mmdbtype.Map{}
			for _, value := range values {
				if value != nil {
					maps.Copy(result, value.(mmdbtype.Map))
				}
			}
			return result, nil
		},
	)
}

func BenchmarkComposeEnterpriseLayers(b *testing.B) {
	base, overlays := enterpriseBenchmarkLayers(8192)
	specs := append([][]benchmarkInsertSpec{base}, overlays...)
	benchmarkComposition(
		b,
		specs,
		inserter.DeepMerge,
		func(_ netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
			// Only traits overlap in this fixture. Construct the final record once.
			result := mmdbtype.Map{}
			traits := mmdbtype.Map{}
			for _, value := range values {
				if value == nil {
					continue
				}
				for key, item := range value.(mmdbtype.Map) {
					if key == "traits" {
						maps.Copy(traits, item.(mmdbtype.Map))
					} else {
						result[key] = item
					}
				}
			}
			result["traits"] = traits
			return result, nil
		},
	)
}

func benchmarkComposition(
	b *testing.B,
	specs [][]benchmarkInsertSpec,
	resolve inserter.PureFunc,
	merge MergeFunc,
) {
	b.Helper()
	opts := Options{
		IPVersion:    4,
		BuildEpoch:   1,
		DatabaseType: "Compose-Benchmark",
		Description:  map[string]string{"en": "Composition benchmark"},
	}
	var sources []NetworkSource
	var baseFile string
	for layer, records := range specs {
		tree, err := New(opts)
		require.NoError(b, err)
		for _, record := range records {
			require.NoError(b, tree.Insert(record.network, record.value))
		}
		var output bytes.Buffer
		_, err = tree.WriteTo(&output)
		require.NoError(b, err)
		reader, err := maxminddb.OpenBytes(output.Bytes())
		require.NoError(b, err)
		b.Cleanup(func() { require.NoError(b, reader.Close()) })
		sources = append(sources, MMDBSource(reader))
		if layer == 0 {
			baseFile = filepath.Join(b.TempDir(), "base.mmdb")
			require.NoError(b, os.WriteFile(baseFile, output.Bytes(), 0o600))
		}
	}
	build := func(compose bool) (*Tree, error) {
		if compose {
			return Compose(opts, sources, merge)
		}
		tree, err := Load(baseFile, opts)
		if err != nil {
			return nil, err
		}
		for _, source := range sources[1:] {
			for value, err := range source.Networks() {
				if err != nil {
					return nil, err
				}
				if err := tree.InsertPureFunc(
					value.Prefix,
					value.Value,
					resolve,
				); err != nil {
					return nil, err
				}
			}
		}
		return tree, nil
	}
	legacy, err := build(false)
	require.NoError(b, err)
	composed, err := build(true)
	require.NoError(b, err)
	for _, source := range []*Tree{legacy, composed} {
		for value, err := range source.Networks() {
			require.NoError(b, err)
			interval := netipx.RangeOfPrefix(value.Prefix)
			for _, addr := range []netip.Addr{interval.From(), interval.To(), interval.From().Prev(), interval.To().Next()} {
				if !addr.IsValid() {
					continue
				}
				_, want := legacy.Get(addr)
				_, got := composed.Get(addr)
				require.Equal(b, want, got)
			}
		}
		var output bytes.Buffer
		_, err := source.WriteTo(&output)
		require.NoError(b, err)
		reader, err := maxminddb.OpenBytes(output.Bytes())
		require.NoError(b, err)
		require.NoError(b, reader.Verify())
		require.NoError(b, reader.Close())
	}
	for _, serialize := range []bool{false, true} {
		for _, compose := range []bool{false, true} {
			name := "Sequential"
			if compose {
				name = "Compose"
			}
			if serialize {
				name += "Write"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					tree, err := build(compose)
					if err != nil {
						b.Fatal(err)
					}
					if serialize {
						if _, err := tree.WriteTo(io.Discard); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
