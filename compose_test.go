package mmdbwriter

import (
	"bytes"
	"errors"
	"net/netip"
	"reflect"
	"slices"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func networkSource(values ...NetworkValue) NetworkSource {
	return SourceFunc(func(yield func(NetworkValue, error) bool) {
		for _, value := range values {
			if !yield(value, nil) {
				return
			}
		}
	})
}

func collectNetworks(t *testing.T, source NetworkSource) []NetworkValue {
	t.Helper()
	var values []NetworkValue
	for value, err := range source.Networks() {
		require.NoError(t, err)
		values = append(values, value)
	}
	return values
}

func TestTreeNetworksEnumeratesDisjointDataWithoutAliases(t *testing.T) {
	tree, err := New(Options{IncludeReservedNetworks: true})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("v4"),
	))
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("2001:db8::/32"),
		mmdbtype.String("v6"),
	))

	values := collectNetworks(t, tree)
	require.Len(t, values, 2)
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), values[0].Prefix)
	assert.Equal(t, mmdbtype.String("v4"), values[0].Value)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::/32"), values[1].Prefix)
	assert.Equal(t, mmdbtype.String("v6"), values[1].Value)
}

func TestMMDBSourceCachesValuesByOffset(t *testing.T) {
	tree, err := New(Options{
		BuildEpoch:              1,
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)
	shared := mmdbtype.Map{"name": mmdbtype.String("shared")}
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.0.0/24"), shared))
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.0.2.0/24"), shared))

	var database bytes.Buffer
	_, err = tree.WriteTo(&database)
	require.NoError(t, err)
	reader, err := maxminddb.OpenBytes(database.Bytes())
	require.NoError(t, err)
	defer reader.Close()

	values := collectNetworks(t, MMDBSource(reader))
	require.Len(t, values, 2)
	first := values[0].Value.(mmdbtype.Map)
	second := values[1].Value.(mmdbtype.Map)
	assert.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(second).Pointer())
}

func TestComposeRefinesLayersAndCallsMergeOncePerOutput(t *testing.T) {
	base := networkSource(NetworkValue{
		Prefix: netip.MustParsePrefix("1.0.0.0/8"),
		Value:  mmdbtype.Map{"base": mmdbtype.String("yes")},
	})
	overlay := networkSource(NetworkValue{
		Prefix: netip.MustParsePrefix("1.2.0.0/16"),
		Value:  mmdbtype.Map{"overlay": mmdbtype.String("yes")},
	})
	calls := map[netip.Prefix]int{}
	tree, err := Compose(
		Options{IPVersion: 4, IncludeReservedNetworks: true},
		[]NetworkSource{base, overlay},
		func(prefix netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
			calls[prefix]++
			var merged mmdbtype.DataType
			var mergeErr error
			for _, value := range values {
				merged, mergeErr = inserter.DeepMerge(merged, value)
				if mergeErr != nil {
					return nil, mergeErr
				}
			}
			return merged, nil
		},
	)
	require.NoError(t, err)

	_, outside := tree.Get(netip.MustParseAddr("1.1.1.1"))
	assert.Equal(t, mmdbtype.Map{"base": mmdbtype.String("yes")}, outside)
	_, inside := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, mmdbtype.Map{
		"base": mmdbtype.String("yes"), "overlay": mmdbtype.String("yes"),
	}, inside)
	_, missing := tree.Get(netip.MustParseAddr("2.0.0.1"))
	assert.Nil(t, missing)

	outputs := collectNetworks(t, tree)
	assert.Len(t, calls, len(outputs))
	for _, output := range outputs {
		assert.Equal(t, 1, calls[output.Prefix], output.Prefix.String())
	}
}

func TestComposeNilMergeUsesHighestLayer(t *testing.T) {
	tree, err := Compose(
		Options{IPVersion: 4, IncludeReservedNetworks: true},
		[]NetworkSource{
			networkSource(NetworkValue{
				Prefix: netip.MustParsePrefix("10.0.0.0/8"),
				Value:  mmdbtype.String("base"),
			}),
			networkSource(NetworkValue{
				Prefix: netip.MustParsePrefix("10.1.0.0/16"),
				Value:  mmdbtype.String("top"),
			}),
		},
		nil,
	)
	require.NoError(t, err)
	_, base := tree.Get(netip.MustParseAddr("10.2.0.1"))
	_, top := tree.Get(netip.MustParseAddr("10.1.0.1"))
	assert.Equal(t, mmdbtype.String("base"), base)
	assert.Equal(t, mmdbtype.String("top"), top)
}

func TestComposeRefinesIPv6Layers(t *testing.T) {
	tree, err := Compose(
		Options{IncludeReservedNetworks: true},
		[]NetworkSource{
			networkSource(NetworkValue{
				Prefix: netip.MustParsePrefix("2001:db8::/32"),
				Value:  mmdbtype.String("base"),
			}),
			networkSource(NetworkValue{
				Prefix: netip.MustParsePrefix("2001:db8:8000::/33"),
				Value:  mmdbtype.String("top"),
			}),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, []netip.Prefix{
		netip.MustParsePrefix("2001:db8::/33"),
		netip.MustParsePrefix("2001:db8:8000::/33"),
	}, prefixesFromValues(collectNetworks(t, tree)))
}

func TestComposeHandlesEmptyAndNilLayers(t *testing.T) {
	options := Options{IPVersion: 4, IncludeReservedNetworks: true}
	tree, err := Compose(options, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, collectNetworks(t, tree))

	_, err = Compose(options, []NetworkSource{nil}, nil)
	require.ErrorContains(t, err, "composition layer 0 is nil")
}

func TestPrefixesForRangeRejectsOversizedIPv4Range(t *testing.T) {
	require.Panics(t, func() {
		prefixesForRange(uint128{}, uint128{hi: 1}, true)
	})
}

func TestComposeRejectsUnsortedOrOverlappingLayer(t *testing.T) {
	_, err := Compose(
		Options{IPVersion: 4, IncludeReservedNetworks: true},
		[]NetworkSource{networkSource(
			NetworkValue{
				Prefix: netip.MustParsePrefix("2.0.0.0/8"),
				Value:  mmdbtype.String("later"),
			},
			NetworkValue{
				Prefix: netip.MustParsePrefix("1.0.0.0/8"),
				Value:  mmdbtype.String("earlier"),
			},
		)},
		nil,
	)
	require.ErrorContains(t, err, "not sorted and disjoint")
}

func TestComposePropagatesSourceAndMergeErrors(t *testing.T) {
	sourceErr := errors.New("source failed")
	source := SourceFunc(func(yield func(NetworkValue, error) bool) {
		yield(NetworkValue{}, sourceErr)
	})
	_, err := Compose(Options{}, []NetworkSource{source}, nil)
	require.ErrorIs(t, err, sourceErr)

	mergeErr := errors.New("merge failed")
	_, err = Compose(Options{IPVersion: 4, IncludeReservedNetworks: true}, []NetworkSource{
		networkSource(NetworkValue{
			Prefix: netip.MustParsePrefix("1.0.0.0/8"),
			Value:  mmdbtype.String("value"),
		}),
	}, func(netip.Prefix, []mmdbtype.DataType) (mmdbtype.DataType, error) {
		return nil, mergeErr
	})
	require.ErrorIs(t, err, mergeErr)
}

func TestSortingSourceResolvesUnsortedOverlaps(t *testing.T) {
	source := NewSortingSource(inserter.TopLevelMerge)
	require.NoError(t, source.Insert(netip.MustParsePrefix("1.2.0.0/16"), mmdbtype.Map{
		"specific": mmdbtype.Bool(true),
	}))
	require.NoError(t, source.Insert(netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.Map{
		"base": mmdbtype.Bool(true),
	}))

	values := collectNetworks(t, source)
	assert.NotEmpty(t, values)
	composed, err := Compose(
		Options{IPVersion: 4, IncludeReservedNetworks: true},
		[]NetworkSource{source},
		nil,
	)
	require.NoError(t, err)
	_, got := composed.Get(netip.MustParseAddr("1.2.3.4"))
	// The broad value was inserted second and TopLevelMerge preserves both.
	assert.Equal(t, mmdbtype.Map{
		"specific": mmdbtype.Bool(true), "base": mmdbtype.Bool(true),
	}, got)
}

func TestComposeOrdersIPv4WithinLowIPv6Region(t *testing.T) {
	var mergedPrefixes []netip.Prefix
	tree, err := Compose(
		Options{DisableIPv4Aliasing: true, IncludeReservedNetworks: true},
		[]NetworkSource{
			networkSource(NetworkValue{
				Prefix: netip.MustParsePrefix("::/96"),
				Value:  mmdbtype.String("base"),
			}),
			networkSource(NetworkValue{
				Prefix: netip.MustParsePrefix("128.0.0.0/1"),
				Value:  mmdbtype.String("top"),
			}),
		},
		func(prefix netip.Prefix, values []mmdbtype.DataType) (mmdbtype.DataType, error) {
			mergedPrefixes = append(mergedPrefixes, prefix)
			for _, value := range slices.Backward(values) {
				if value != nil {
					return value, nil
				}
			}
			return nil, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/1"),
		netip.MustParsePrefix("128.0.0.0/1"),
	}, mergedPrefixes)
	assert.Equal(t, []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/1"),
		netip.MustParsePrefix("128.0.0.0/1"),
	}, prefixesFromValues(collectNetworks(t, tree)))
}

func prefixesFromValues(values []NetworkValue) []netip.Prefix {
	prefixes := make([]netip.Prefix, len(values))
	for index, value := range values {
		prefixes[index] = value.Prefix
	}
	return prefixes
}
