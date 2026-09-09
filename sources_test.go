package mmdbwriter

import (
	"bytes"
	"errors"
	"net/netip"
	"reflect"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

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
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	values := collectNetworks(t, MMDBSource(reader))
	require.Len(t, values, 2)
	first := values[0].Value.(mmdbtype.Map)
	second := values[1].Value.(mmdbtype.Map)
	assert.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(second).Pointer())
}

func TestSortingSourceResolvesUnsortedOverlaps(t *testing.T) {
	source := NewSortingSource(
		func(existing, value mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
			return inserter.TopLevelMerge(existing, value)
		},
	)
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

func TestSortingSourceDefaultsToReplace(t *testing.T) {
	source := NewSortingSource(nil)
	require.NoError(t, source.Insert(
		netip.MustParsePrefix("1.2.0.0/16"),
		mmdbtype.String("specific"),
	))
	require.NoError(t, source.Insert(
		netip.MustParsePrefix("1.0.0.0/8"),
		mmdbtype.String("replacement"),
	))

	values := collectNetworks(t, source)
	require.Len(t, values, 1)
	assert.Equal(t, netip.MustParsePrefix("1.0.0.0/8"), values[0].Prefix)
	assert.Equal(t, mmdbtype.String("replacement"), values[0].Value)
}

func TestSortingSourceAddSourceRollsBackOnError(t *testing.T) {
	source := NewSortingSource(nil)
	require.NoError(t, source.Insert(
		netip.MustParsePrefix("1.0.0.0/8"),
		mmdbtype.String("existing"),
	))
	sourceErr := errors.New("source failed")
	failingSource := SourceFunc(func(yield func(NetworkValue, error) bool) {
		if !yield(NetworkValue{
			Prefix: netip.MustParsePrefix("2.0.0.0/8"),
			Value:  mmdbtype.String("partial"),
		}, nil) {
			return
		}
		yield(NetworkValue{}, sourceErr)
	})

	err := source.AddSource(failingSource)
	require.ErrorIs(t, err, sourceErr)
	require.Len(t, source.values, 1)
	assert.Equal(t, netip.MustParsePrefix("1.0.0.0/8"), source.values[0].Prefix)
}

func TestSortingSourceMetadataAndRollback(t *testing.T) {
	var calls []inserter.Metadata
	source := NewSortingSource(
		func(_, value mmdbtype.DataType, metadata inserter.Metadata) (mmdbtype.DataType, error) {
			calls = append(calls, metadata)
			return value, nil
		},
	)
	require.NoError(t, source.Insert(netip.MustParsePrefix("1.0.0.0/8"), mmdbtype.String("base")))
	require.NoError(
		t,
		source.Insert(netip.MustParsePrefix("1.2.0.0/16"), mmdbtype.String("overlay")),
	)
	first := collectNetworks(t, source)
	require.Len(t, calls, 2)
	assert.Equal(t, netip.MustParsePrefix("1.2.0.0/16"), calls[1].InsertedNetwork)
	assert.Equal(t, netip.MustParsePrefix("1.0.0.0/8"), calls[1].ExistingNetwork())
	assert.Equal(t, first, collectNetworks(t, source))
	require.Error(t, source.AddSource(nilSequenceSource{}))
	require.Error(t, source.AddSource(networkSource(NetworkValue{Prefix: netip.Prefix{}})))
	assert.Equal(t, first, collectNetworks(t, source))
}
