package mmdbwriter

import (
	"net/netip"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// BenchmarkComposePR239Inputs reproduces PR #239's in-memory input and merge
// policy on the current implementation. It does not include MMDB decoding.
func BenchmarkComposePR239Inputs(b *testing.B) {
	base, overlays := enterpriseBenchmarkLayers(2_048)
	specs := append([][]benchmarkInsertSpec{base}, overlays...)
	layerValues := make([][]NetworkValue, len(specs))
	for i, layer := range specs {
		for _, spec := range layer {
			layerValues[i] = append(
				layerValues[i],
				NetworkValue{Prefix: spec.network, Value: spec.value},
			)
		}
	}
	layers := make([]NetworkSource, len(layerValues))
	for index, values := range layerValues {
		layers[index] = SourceFunc(func(yield func(NetworkValue, error) bool) {
			for _, value := range values {
				if !yield(value, nil) {
					return
				}
			}
		})
	}
	merge := func(_ netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
		var merged mmdbtype.DataType
		var err error
		for _, value := range values {
			merged, err = inserter.DeepMerge(merged, value)
			if err != nil {
				return nil, err
			}
		}
		return merged, nil
	}

	for _, compose := range []bool{false, true} {
		name := "sequential-passes"
		if compose {
			name = "compose"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportMetric(float64(len(base)), "networks/op")
			b.ReportAllocs()
			for range b.N {
				if compose {
					if _, err := Compose(
						Options{IPVersion: 4, IncludeReservedNetworks: true},
						layers,
						merge,
					); err != nil {
						b.Fatal(err)
					}
					continue
				}
				tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
				if err != nil {
					b.Fatal(err)
				}
				for _, value := range layerValues[0] {
					if err := tree.Insert(value.Prefix, value.Value); err != nil {
						b.Fatal(err)
					}
				}
				for _, layer := range layerValues[1:] {
					for _, value := range layer {
						if err := tree.InsertPureFunc(
							value.Prefix,
							value.Value,
							inserter.DeepMerge,
						); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}

// BenchmarkComposePR239MMDBInputs keeps the old record count and merge policy,
// but reads both paths' inputs from serialized MMDBs.
func BenchmarkComposePR239MMDBInputs(b *testing.B) {
	base, overlays := enterpriseBenchmarkLayers(2048)
	benchmarkComposition(b, append([][]benchmarkInsertSpec{base}, overlays...), inserter.DeepMerge,
		func(_ netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
			var result mmdbtype.DataType
			for _, value := range values {
				var err error
				result, err = inserter.DeepMerge(result, value)
				if err != nil {
					return nil, err
				}
			}
			return result, nil
		})
}
