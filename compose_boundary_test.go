package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"net/netip"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestComposeInputValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prefixes []string
		version  int
		message  string
	}{
		{"unmasked", []string{"1.2.3.4/24"}, 6, "not masked"},
		{"mapped short", []string{"::ffff:1.2.3.4/80"}, 6, "shorter than /96"},
		{"mapped overlap", []string{"1.2.3.0/24", "::ffff:1.2.3.0/120"}, 6, "not sorted and disjoint"},
		{"subtree overlap", []string{"1.2.3.0/24", "::1.2.3.0/120"}, 6, "not sorted and disjoint"},
		{"v6 dropped", []string{"2001:db8::/32"}, 4, "IPv6 prefixes"},
		{"maximum duplicate", []string{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"}, 6, "not sorted and disjoint"},
		{"after full space", []string{"::/0", "1.2.3.0/24"}, 6, "not sorted and disjoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var values []NetworkValue
			for _, prefix := range tc.prefixes {
				values = append(
					values,
					NetworkValue{netip.MustParsePrefix(prefix), mmdbtype.String("value")},
				)
			}
			tree, err := Compose(
				Options{
					IPVersion:               tc.version,
					DisableIPv4Aliasing:     true,
					IncludeReservedNetworks: true,
				},
				[]NetworkSource{networkSource(values...)},
				func(netip.Prefix, []mmdbtype.DataType) (mmdbtype.DataType, error) { return nil, nil },
			)
			require.ErrorContains(t, err, tc.message)
			assert.Nil(t, tree)
		})
	}
	_, err := Compose(Options{Inserter: inserter.Replace}, nil, nil)
	require.ErrorContains(t, err, "Options.Inserter")
	_, err = Compose(Options{}, []NetworkSource{networkSource(NetworkValue{})}, nil)
	require.ErrorContains(t, err, "prefix is invalid")
}

func TestComposeStopsSources(t *testing.T) {
	failure := errors.New("source failed")
	for _, mode := range []string{"success", "source", "merge", "insert", "maximum"} {
		t.Run(mode, func(t *testing.T) {
			stopped := 0
			source := func(prefix string, fail bool) NetworkSource {
				return SourceFunc(func(yield func(NetworkValue, error) bool) {
					defer func() { stopped++ }()
					if !yield(
						NetworkValue{netip.MustParsePrefix(prefix), mmdbtype.String("value")},
						nil,
					) {
						return
					}
					if fail {
						yield(NetworkValue{}, failure)
					}
				})
			}
			prefix := "1.2.3.0/24"
			if mode == "insert" {
				prefix = "127.0.0.0/8"
			}
			if mode == "maximum" {
				prefix = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"
			}
			var merge MergeFunc
			if mode == "merge" {
				merge = func(netip.Prefix, []mmdbtype.DataType) (mmdbtype.DataType, error) { return nil, failure }
			}
			tree, err := Compose(
				Options{IncludeReservedNetworks: mode != "insert"},
				[]NetworkSource{
					source(prefix, mode == "source" || mode == "maximum"),
					source(prefix, false),
				},
				merge,
			)
			if mode == "success" {
				require.NoError(t, err)
				require.NotNil(t, tree)
			} else {
				require.Error(t, err)
				assert.Nil(t, tree)
				if mode != "insert" {
					require.ErrorIs(t, err, failure)
				}
			}
			assert.Equal(t, 2, stopped)
		})
	}
}

func TestComposeCallbackRefinement(t *testing.T) {
	var calls []netip.Prefix
	tree, err := Compose(Options{}, []NetworkSource{networkSource(
		NetworkValue{netip.MustParsePrefix("1.0.0.0/25"), mmdbtype.String("a")},
		NetworkValue{netip.MustParsePrefix("1.0.0.128/25"), mmdbtype.String("b")},
		NetworkValue{netip.MustParsePrefix("1.0.2.0/24"), nil},
	)}, func(prefix netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
		calls = append(calls, prefix)
		if values[0] == nil {
			return nil, nil
		}
		return mmdbtype.String("same"), nil
	})
	require.NoError(t, err)
	require.Len(t, calls, 3)
	values := collectNetworks(t, tree)
	require.Len(t, values, 1)
	assert.Equal(t, netip.MustParsePrefix("1.0.0.0/24"), values[0].Prefix)
}

func TestComposeAddressSpaceAndRoundTrip(t *testing.T) {
	for _, prefixes := range [][]string{
		{"0.0.0.0/0"},
		{"::/0"},
		{"255.255.255.255/32"},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"},
		{"::ffff:1.2.3.0/120", "::1:0:0/96", "2001:db8::/32"},
	} {
		t.Run(fmt.Sprint(prefixes), func(t *testing.T) {
			var inputs []NetworkValue
			for i, p := range prefixes {
				inputs = append(
					inputs,
					NetworkValue{netip.MustParsePrefix(p), mmdbtype.Uint32(i + 1)},
				)
			}
			opts := Options{
				DatabaseType:            "Compose-Test",
				Description:             map[string]string{"en": "Composition test"},
				BuildEpoch:              1,
				IncludeReservedNetworks: true,
				DisableIPv4Aliasing:     true,
			}
			tree, err := Compose(opts, []NetworkSource{networkSource(inputs...)}, nil)
			require.NoError(t, err)
			before := collectNetworks(t, tree)
			var output bytes.Buffer
			_, err = tree.WriteTo(&output)
			require.NoError(t, err)
			assert.Equal(t, before, collectNetworks(t, tree))
			reader, err := maxminddb.OpenBytes(output.Bytes())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			require.NoError(t, reader.Verify())
			copyTree, err := Compose(
				opts,
				[]NetworkSource{MMDBSource(reader, maxminddb.IncludeAliasedNetworks())},
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, before, collectNetworks(t, copyTree))
		})
	}
}

type nilSequenceSource struct{}

func (nilSequenceSource) Networks() iter.Seq2[NetworkValue, error] { return nil }

func TestSourcesNilAndEarlyStop(t *testing.T) {
	for _, source := range []NetworkSource{SourceFunc(nil), MMDBSource(nil), (*Tree)(nil), nilSequenceSource{}} {
		_, err := Compose(Options{}, []NetworkSource{source}, nil)
		require.Error(t, err)
	}
	tree, err := New(Options{})
	require.NoError(t, err)
	for _, prefix := range []string{"1.0.0.0/24", "2.0.0.0/24"} {
		require.NoError(t, tree.Insert(netip.MustParsePrefix(prefix), mmdbtype.String(prefix)))
	}
	calls := 0
	tree.Networks()(func(NetworkValue, error) bool { calls++; return false })
	assert.Equal(t, 1, calls)
	var output bytes.Buffer
	_, err = tree.WriteTo(&output)
	require.NoError(t, err)
	reader, err := maxminddb.OpenBytes(output.Bytes())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	calls = 0
	MMDBSource(reader).Networks()(func(NetworkValue, error) bool { calls++; return false })
	assert.Equal(t, 1, calls)
	assert.Equal(t, collectNetworks(t, tree), collectNetworks(t, MMDBSource(reader)))
}

func FuzzComposeSequential(f *testing.F) {
	f.Add([]byte{0, 4, 1, 8, 3, 2, 16, 4, 5})
	f.Add([]byte{255, 0, 0, 0, 0, 7})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 96 {
			data = data[:96]
		}
		opts := Options{IPVersion: 4, IncludeReservedNetworks: true, BuildEpoch: 1}
		reference, err := New(opts)
		require.NoError(t, err)
		var layers []NetworkSource
		for layer := range 3 {
			source := NewSortingSource(nil)
			for i := layer * 3; i+2 < len(data); i += 9 {
				prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{1, 0, 0, data[i]}), 24+int(data[i+1]%9)).
					Masked()
				require.NoError(t, source.Insert(prefix, mmdbtype.Uint32(data[i+2])))
			}
			layers = append(layers, source)
			for value, err := range source.Networks() {
				require.NoError(t, err)
				require.NoError(t, reference.Insert(value.Prefix, value.Value))
			}
		}
		composed, err := Compose(opts, layers, nil)
		require.NoError(t, err)
		for i := range 256 {
			addr := netip.AddrFrom4([4]byte{1, 0, 0, byte(i)})
			_, want := reference.Get(addr)
			_, got := composed.Get(addr)
			require.Equal(t, want, got)
		}
	})
}

func TestComposeStopsOpenedSourcesOnInitializationError(t *testing.T) {
	for _, invalid := range []NetworkSource{nil, nilSequenceSource{}, SourceFunc(nil)} {
		stopped := false
		source := SourceFunc(func(yield func(NetworkValue, error) bool) {
			defer func() { stopped = true }()
			yield(NetworkValue{netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.String("base")}, nil)
		})
		_, err := Compose(Options{}, []NetworkSource{source, invalid}, nil)
		require.Error(t, err)
		assert.True(t, stopped)
	}
}

func TestComposePreservesInputs(t *testing.T) {
	value := mmdbtype.Map{"names": mmdbtype.Map{"en": mmdbtype.String("name")}}
	before := value.Copy()
	source := networkSource(NetworkValue{netip.MustParsePrefix("1.0.0.0/8"), value})
	tree, err := Compose(
		Options{},
		[]NetworkSource{source},
		func(_ netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
			result := values[0].Copy().(mmdbtype.Map)
			result["extra"] = mmdbtype.Bool(true)
			return result, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, before, value)
	_, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Contains(t, got, mmdbtype.String("extra"))
}
