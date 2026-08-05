package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type testInsert struct {
	network string
	start   string
	end     string
	value   mmdbtype.DataType
}

type testInsertError struct {
	network          string
	start            string
	end              string
	value            mmdbtype.DataType
	expectedErrorMsg string
}

type testGet struct {
	ip                  string
	expectedNetwork     string
	expectedGetValue    mmdbtype.DataType
	expectedLookupValue *any
}

func TestTreeInsert(t *testing.T) {
	tree, err := New(Options{
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	value := mmdbtype.Map{"name": mmdbtype.String("test")}
	err = tree.Insert(netip.MustParsePrefix("1.2.3.0/24"), value)
	require.NoError(t, err)

	network, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, "1.2.3.0/24", network.String())
	assert.Equal(t, value, got)
}

func TestTreeInsertSplittingDataRecordMaintainsRefCounts(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	initialValue := mmdbtype.String("initial")
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.1.0.0/24"), initialValue))

	hash, err := tree.dataMap.hasher.Hash(initialValue)
	require.NoError(t, err)
	key := hash
	initialMapValue := tree.dataMap.data[key]
	require.NotNil(t, initialMapValue)
	require.Equal(t, uint32(1), initialMapValue.refCount)

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.1.0.128/25"),
		mmdbtype.String("upper"),
	))
	assert.Equal(t, uint32(1), initialMapValue.refCount)
	assert.Same(t, initialMapValue, tree.dataMap.data[key])

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.1.0.0/25"),
		mmdbtype.String("lower"),
	))
	assert.Zero(t, initialMapValue.refCount)
	assert.NotContains(t, tree.dataMap.data, key)
}

func TestTreeNodeBlocksGrowAndWrite(t *testing.T) {
	tree, err := New(Options{
		DatabaseType:            "Test",
		Description:             map[string]string{"en": "Test database"},
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	addresses := make([]netip.Addr, nodeBlockSize+1)
	for i := range addresses {
		address := netip.AddrFrom4([4]byte{
			1,
			byte(i >> 16),
			byte(i >> 8),
			byte(i),
		})
		addresses[i] = address
		require.NoError(t, tree.Insert(
			netip.PrefixFrom(address, address.BitLen()),
			mmdbtype.Uint32(i),
		))
	}

	require.Greater(t, tree.nodeCountAllocated, nodeBlockSize)
	require.Greater(t, len(tree.nodeBlocks), 1)

	var buf bytes.Buffer
	_, writeErr := tree.WriteTo(&buf)
	require.NoError(t, writeErr)

	reader, err := maxminddb.OpenBytes(buf.Bytes())
	require.NoError(t, err)
	defer reader.Close()
	require.NoError(t, reader.Verify())

	for i, address := range addresses {
		var got uint32
		result := reader.Lookup(address)
		require.True(t, result.Found(), "record for %s", address)
		require.NoError(t, result.Decode(&got), "decode record for %s", address)
		assert.Equal(t, uint32(i), got, "record for %s", address)
	}
}

func TestTreeInsertFunc(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	err = tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.Map{"base": mmdbtype.String("value")},
	)
	require.NoError(t, err)

	err = tree.InsertFunc(
		netip.MustParsePrefix("1.2.3.0/25"),
		mmdbtype.Map{"extra": mmdbtype.String("value")},
		inserter.TopLevelMerge,
	)
	require.NoError(t, err)

	network, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, "1.2.3.0/25", network.String())
	assert.Equal(t, mmdbtype.Map{
		"base":  mmdbtype.String("value"),
		"extra": mmdbtype.String("value"),
	}, got)
}

func TestTreeInsertPureFuncMemoizesDistinctExistingValues(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	for index, value := range []mmdbtype.String{"a", "b", "a", "b"} {
		//nolint:gosec // index is bounded by the four-element literal
		prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{1, 2, 3, byte(index * 64)}), 26)
		require.NoError(t, tree.Insert(prefix, value))
	}

	calls := map[mmdbtype.String]int{}
	err = tree.InsertPureFunc(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("new"),
		func(existing, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
			calls[existing.(mmdbtype.String)]++
			return newValue, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, map[mmdbtype.String]int{"a": 1, "b": 1}, calls)
	for index := range 4 {
		//nolint:gosec // index is bounded by the four-iteration loop
		_, value := tree.Get(netip.AddrFrom4([4]byte{1, 2, 3, byte(index * 64)}))
		assert.Equal(t, mmdbtype.String("new"), value)
	}

	// Two distinct existing values promote the memo to its map form, so this
	// also pins that releaseResolved drains every entry rather than the first.
	// The four covered records now share one value and merge back into a single
	// record, so exactly one reference should remain.
	require.Len(t, tree.dataMap.data, 1, "the memo retained replaced values")
	for _, dmv := range tree.dataMap.data {
		assert.EqualValues(t, 1, dmv.refCount,
			"the memo held references after the insert finished")
	}
}

func TestTreeInsertPureFuncReleasesMemoAfterPartialError(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/25"),
		mmdbtype.String("first"),
	))
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.128/25"),
		mmdbtype.String("second"),
	))

	insertErr := errors.New("insert failed")
	result := mmdbtype.String("resolved")
	err = tree.InsertPureFunc(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("new"),
		func(existing, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			if existing == mmdbtype.String("second") {
				return nil, insertErr
			}
			return result, nil
		},
	)
	require.ErrorIs(t, err, insertErr)

	hash, err := tree.dataMap.hasher.Hash(result)
	require.NoError(t, err)
	stored := tree.dataMap.data[hash]
	require.NotNil(t, stored)
	assert.Equal(
		t,
		uint32(1),
		stored.refCount,
		"only the inserted record should hold a reference after the memo is released",
	)

	_, got := tree.Get(netip.MustParseAddr("1.2.3.1"))
	assert.Equal(t, result, got)
	_, got = tree.Get(netip.MustParseAddr("1.2.3.129"))
	assert.Equal(t, mmdbtype.String("second"), got)
}

func TestTreeInsertFuncEvaluatesOrdinaryFuncForEveryRecord(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	for index, value := range []mmdbtype.String{"a", "b", "a", "b"} {
		//nolint:gosec // index is bounded by the four-iteration loop
		prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{1, 2, 3, byte(index * 64)}), 26)
		require.NoError(t, tree.Insert(prefix, value))
	}

	calls := 0
	err = tree.InsertFunc(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("new"),
		inserter.Func(func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			calls++
			//nolint:gosec // calls is bounded by the four covered records
			return mmdbtype.Uint32(calls), nil
		}),
	)
	require.NoError(t, err)
	assert.Equal(t, 4, calls)

	values := map[mmdbtype.DataType]struct{}{}
	for index := range 4 {
		//nolint:gosec // index is bounded by the four-iteration loop
		_, value := tree.Get(netip.AddrFrom4([4]byte{1, 2, 3, byte(index * 64)}))
		values[value] = struct{}{}
	}
	assert.Len(t, values, 4)
}

func TestTreeOptionsInserter(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
		Inserter:                inserter.TopLevelMerge,
	})
	require.NoError(t, err)

	err = tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.Map{"base": mmdbtype.String("value")},
	)
	require.NoError(t, err)

	err = tree.Insert(
		netip.MustParsePrefix("1.2.3.0/25"),
		mmdbtype.Map{"extra": mmdbtype.String("value")},
	)
	require.NoError(t, err)

	network, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, "1.2.3.0/25", network.String())
	assert.Equal(t, mmdbtype.Map{
		"base":  mmdbtype.String("value"),
		"extra": mmdbtype.String("value"),
	}, got)

	network, got = tree.Get(netip.MustParseAddr("1.2.3.200"))
	assert.Equal(t, "1.2.3.128/25", network.String())
	assert.Equal(t, mmdbtype.Map{"base": mmdbtype.String("value")}, got)
}

func TestTreeRejectsNilInserterFunc(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)

	var insertFunc inserter.Func
	prefix := netip.MustParsePrefix("1.2.3.0/24")
	start := netip.MustParseAddr("1.2.3.1")
	end := netip.MustParseAddr("1.2.3.2")
	value := mmdbtype.String("value")

	require.ErrorIs(t, tree.InsertFunc(prefix, value, insertFunc), errNilInserterFunc)
	require.ErrorIs(t, tree.InsertPureFunc(prefix, value, insertFunc), errNilInserterFunc)
	require.ErrorIs(
		t,
		tree.InsertRangeFunc(start, end, value, insertFunc),
		errNilInserterFunc,
	)
	require.ErrorIs(
		t,
		tree.InsertRangePureFunc(start, end, value, insertFunc),
		errNilInserterFunc,
	)
}

func TestTreeInsertFuncErrorDoesNotMutateEmptySiblingRecord(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("neighbor"),
	))

	insertErr := errors.New("insert failed")
	err = tree.InsertFunc(
		netip.MustParsePrefix("1.2.2.0/24"),
		mmdbtype.String("value"),
		inserter.Func(func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return nil, insertErr
		}),
	)
	require.ErrorIs(t, err, insertErr)

	require.NotPanics(t, func() {
		_, got := tree.Get(netip.MustParseAddr("1.2.2.4"))
		assert.Nil(t, got)
	})

	buf := &bytes.Buffer{}
	_, err = tree.WriteTo(buf)
	require.NoError(t, err)
}

func TestTreeInsertFuncErrorLeavesExistingRecordUnchanged(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("1.2.3.0/24")
	base := mmdbtype.Map{"base": mmdbtype.String("value")}
	require.NoError(t, tree.Insert(prefix, base))

	insertErr := errors.New("insert failed")
	err = tree.InsertFunc(
		prefix,
		mmdbtype.Map{"extra": mmdbtype.String("value")},
		inserter.Func(func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return nil, insertErr
		}),
	)
	require.ErrorIs(t, err, insertErr)

	network, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, prefix, network)
	assert.Equal(t, base, got)
}

func TestTreeInsertFuncReturnsErrorForNilUint128Result(t *testing.T) {
	tests := []struct {
		name          string
		existing      func(*mmdbtype.Uint128) mmdbtype.DataType
		result        func(*mmdbtype.Uint128) mmdbtype.DataType
		expectedError string
	}{
		{
			name: "direct",
			existing: func(value *mmdbtype.Uint128) mmdbtype.DataType {
				return value
			},
			result: func(value *mmdbtype.Uint128) mmdbtype.DataType {
				return value
			},
			expectedError: "cannot hash a nil *mmdbtype.Uint128",
		},
		{
			name: "nested",
			existing: func(value *mmdbtype.Uint128) mmdbtype.DataType {
				return mmdbtype.Map{"value": value}
			},
			result: func(value *mmdbtype.Uint128) mmdbtype.DataType {
				return mmdbtype.Map{"value": value}
			},
			expectedError: `hashing map key "value": cannot hash a nil *mmdbtype.Uint128`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
			require.NoError(t, err)

			value := mmdbtype.Uint128(*big.NewInt(42))
			existing := test.existing(&value)
			prefix := netip.MustParsePrefix("1.2.3.0/24")
			require.NoError(t, tree.Insert(prefix, existing))

			var nilUint128 *mmdbtype.Uint128
			err = tree.InsertFunc(
				prefix,
				nil,
				inserter.Func(func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
					return test.result(nilUint128), nil
				}),
			)
			require.EqualError(t, err, test.expectedError)

			_, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
			assert.Equal(t, existing, got)
		})
	}
}

func TestTreeInsertCompressedPathBeforeFinalize(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	base := mmdbtype.Map{"name": mmdbtype.String("base")}
	require.NoError(t, tree.Insert(netip.MustParsePrefix("11.0.0.0/8"), base))
	assert.Equal(t, 1, tree.nodeCountAllocated)

	network, got := tree.Get(netip.MustParseAddr("11.1.2.3"))
	assert.Equal(t, "11.0.0.0/8", network.String())
	assert.Equal(t, base, got)

	missAddress := netip.MustParseAddr("12.1.2.3")
	expectedMissPrefix := netip.MustParsePrefix("12.0.0.0/6")
	network, got = tree.Get(missAddress)
	assert.Equal(t, expectedMissPrefix, network)
	assert.Nil(t, got)

	tree.finalize()
	network, got = tree.Get(missAddress)
	assert.Equal(t, expectedMissPrefix, network)
	assert.Nil(t, got)

	specific := mmdbtype.Map{"name": mmdbtype.String("specific")}
	require.NoError(t, tree.Insert(netip.MustParsePrefix("11.2.0.0/16"), specific))

	network, got = tree.Get(netip.MustParseAddr("11.2.3.4"))
	assert.Equal(t, "11.2.0.0/16", network.String())
	assert.Equal(t, specific, got)

	_, got = tree.Get(netip.MustParseAddr("11.3.3.4"))
	assert.Equal(t, base, got)
}

func TestTreeInsertInvalid(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	err = tree.Insert(netip.Prefix{}, mmdbtype.Map{})
	require.EqualError(t, err, "prefix is invalid")
}

func TestTreeInsertMasksPrefix(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	value := mmdbtype.String("value")
	err = tree.Insert(netip.PrefixFrom(netip.MustParseAddr("1.2.3.4"), 24), value)
	require.NoError(t, err)

	network, got := tree.Get(netip.MustParseAddr("1.2.3.5"))
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), network)
	assert.Equal(t, value, got)
}

func TestTreeInsertIPv4MappedPrefix(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "IPv6 tree",
			opts: Options{IncludeReservedNetworks: true},
		},
		{
			name: "IPv4 tree",
			opts: Options{
				IPVersion:               4,
				IncludeReservedNetworks: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := New(test.opts)
			require.NoError(t, err)

			value := mmdbtype.String("value")
			err = tree.Insert(netip.MustParsePrefix("::ffff:1.2.3.0/120"), value)
			require.NoError(t, err)

			network, got := tree.Get(netip.MustParseAddr("1.2.3.1"))
			assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), network)
			assert.Equal(t, value, got)
		})
	}
}

func TestTreeInsertIPv4MappedPrefixShorterThan96(t *testing.T) {
	tree, err := New(Options{IncludeReservedNetworks: true})
	require.NoError(t, err)

	err = tree.Insert(
		netip.PrefixFrom(netip.MustParseAddr("::ffff:1.2.3.4"), 95),
		mmdbtype.String("value"),
	)
	require.EqualError(t, err, "IPv4-mapped prefixes shorter than /96 cannot be inserted")
}

func TestTreeNormalizeLoadPrefixIPv4Mapped(t *testing.T) {
	tree, err := New(Options{IncludeReservedNetworks: true})
	require.NoError(t, err)

	prefix, err := tree.normalizeLoadPrefix(netip.MustParsePrefix("::ffff:1.2.3.0/120"))
	require.NoError(t, err)
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), prefix)

	_, err = tree.normalizeLoadPrefix(
		netip.PrefixFrom(netip.MustParseAddr("::ffff:1.2.3.4"), 95),
	)
	require.EqualError(
		t,
		err,
		"normalizing loaded network ::ffff:1.2.3.4/95: IPv4-mapped prefixes shorter than /96 cannot be inserted",
	)

	_, err = tree.normalizeLoadPrefix(netip.Prefix{})
	require.EqualError(t, err, "loaded prefix is invalid")
}

func TestTreeInsertIPv6IntoIPv4Tree(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	err = tree.Insert(
		netip.MustParsePrefix("2001:db8::/32"),
		mmdbtype.String("value"),
	)
	require.EqualError(t, err, "IPv6 prefixes cannot be inserted into an IPv4 tree")

	err = tree.InsertRange(
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("2001:db8::1"),
		mmdbtype.String("value"),
	)
	require.EqualError(t, err, "IPv6 ranges cannot be inserted into an IPv4 tree")

	err = tree.InsertRange(netip.Addr{}, netip.MustParseAddr("1.2.3.4"), mmdbtype.String("value"))
	require.EqualError(t, err, "start IP is invalid")
}

func TestTreeInsertRangeInvalidBounds(t *testing.T) {
	tree, err := New(Options{IncludeReservedNetworks: true})
	require.NoError(t, err)

	tests := []struct {
		name          string
		start         netip.Addr
		end           netip.Addr
		expectedError string
	}{
		{
			name:          "invalid end",
			start:         netip.MustParseAddr("1.2.3.4"),
			expectedError: "end IP is invalid",
		},
		{
			name:          "reversed range",
			start:         netip.MustParseAddr("1.2.3.5"),
			end:           netip.MustParseAddr("1.2.3.4"),
			expectedError: "start & end IPs did not give valid range",
		},
		{
			name:          "mixed address families",
			start:         netip.MustParseAddr("1.2.3.4"),
			end:           netip.MustParseAddr("2001:db8::1"),
			expectedError: "start & end IPs did not give valid range",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := tree.InsertRange(test.start, test.end, mmdbtype.String("value"))
			require.EqualError(t, err, test.expectedError)
		})
	}
}

func TestTreeInsertRangeFuncNonPrefixAligned(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)

	base := mmdbtype.Map{"base": mmdbtype.String("value")}
	merged := mmdbtype.Map{
		"base":  mmdbtype.String("value"),
		"extra": mmdbtype.String("value"),
	}
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/24"), base))
	require.NoError(t, tree.InsertRangeFunc(
		netip.MustParseAddr("1.2.3.10"),
		netip.MustParseAddr("1.2.3.20"),
		mmdbtype.Map{"extra": mmdbtype.String("value")},
		inserter.TopLevelMerge,
	))

	for _, test := range []struct {
		address  string
		expected mmdbtype.Map
	}{
		{address: "1.2.3.9", expected: base},
		{address: "1.2.3.10", expected: merged},
		{address: "1.2.3.20", expected: merged},
		{address: "1.2.3.21", expected: base},
	} {
		_, got := tree.Get(netip.MustParseAddr(test.address))
		assert.Equal(t, test.expected, got, test.address)
	}
}

func TestTreeInsertRangePureFuncMemoizesAcrossSubnets(t *testing.T) {
	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)

	oldValue := mmdbtype.String("old")
	newValue := mmdbtype.String("new")
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/24"), oldValue))

	calls := 0
	err = tree.InsertRangePureFunc(
		netip.MustParseAddr("1.2.3.1"),
		netip.MustParseAddr("1.2.3.254"),
		newValue,
		func(existing, replacement mmdbtype.DataType) (mmdbtype.DataType, error) {
			calls++
			assert.Equal(t, oldValue, existing)
			return replacement, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)

	for _, address := range []string{"1.2.3.0", "1.2.3.255"} {
		_, got := tree.Get(netip.MustParseAddr(address))
		assert.Equal(t, oldValue, got, address)
	}
	for _, address := range []string{"1.2.3.1", "1.2.3.128", "1.2.3.254"} {
		_, got := tree.Get(netip.MustParseAddr(address))
		assert.Equal(t, newValue, got, address)
	}
}

func TestTreeInsertIPv4TreeReservedNetworkError(t *testing.T) {
	tree, err := New(Options{IPVersion: 4})
	require.NoError(t, err)

	err = tree.Insert(netip.MustParsePrefix("10.0.0.1/32"), mmdbtype.String("value"))
	require.EqualError(
		t,
		err,
		"attempt to insert 10.0.0.1/32 into 10.0.0.0/8, which is a reserved network",
	)

	var reservedErr *ReservedNetworkError
	require.ErrorAs(t, err, &reservedErr)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.1/32"), reservedErr.InsertedNetwork)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/8"), reservedErr.ReservedNetwork)
}

func TestTreeGetIPv4AddressForShortIPv6Prefix(t *testing.T) {
	tree, err := New(Options{
		DisableIPv4Aliasing:     true,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	value := mmdbtype.String("value")
	require.NoError(t, tree.Insert(netip.MustParsePrefix("::/90"), value))

	network, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, netip.MustParsePrefix("::/90"), network)
	assert.Equal(t, value, got)
}

func TestTreeGetInvalidOrWrongFamilyReturnsZeroPrefix(t *testing.T) {
	tree, err := New(Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	network, got := tree.Get(netip.Addr{})
	assert.Equal(t, netip.Prefix{}, network)
	assert.Nil(t, got)

	network, got = tree.Get(netip.MustParseAddr("2001:db8::1"))
	assert.Equal(t, netip.Prefix{}, network)
	assert.Nil(t, got)
}

func TestLoadWrapsInsertErrorWithNetwork(t *testing.T) {
	tree, err := New(Options{
		DisableIPv4Aliasing:     true,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("2001:db8::/32"),
		mmdbtype.String("value"),
	))

	f, err := os.CreateTemp(t.TempDir(), "mmdbwriter-load-error-*.mmdb")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(f.Name())) }()

	_, err = tree.WriteTo(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Load(f.Name(), Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading network 2001:db8::/32")
	assert.Contains(t, err.Error(), "IPv6 prefixes cannot be inserted into an IPv4 tree")
}

func TestLoadChecksIteratorErrorBeforeOffsetCache(t *testing.T) {
	tree, err := New(Options{
		DatabaseType:            "mmdbwriter-load-corrupt",
		IncludeReservedNetworks: true,
		IPVersion:               4,
		RecordSize:              24,
	})
	require.NoError(t, err)

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("0.0.0.0/1"),
		mmdbtype.String("first"),
	))
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("128.0.0.0/1"),
		mmdbtype.String("second"),
	))

	buf := &bytes.Buffer{}
	_, err = tree.WriteTo(buf)
	require.NoError(t, err)

	dbBytes := append([]byte(nil), buf.Bytes()...)
	// Record size 24 stores the root node as three bytes per child. Corrupt
	// the right child pointer so its iterator result has Err set and Offset 0.
	dbBytes[3], dbBytes[4], dbBytes[5] = 0xFF, 0xFF, 0xFF

	f, err := os.CreateTemp(t.TempDir(), "mmdbwriter-load-corrupt-*.mmdb")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(f.Name())) }()

	_, err = f.Write(dbBytes)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Load(f.Name(), Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading network 128.0.0.0/1")
	assert.Contains(t, err.Error(), "search tree is corrupt")
}

func TestLoadDecodeErrorIncludesNetwork(t *testing.T) {
	tree, err := New(Options{
		DatabaseType:            "mmdbwriter-load-corrupt-data",
		Description:             map[string]string{"en": "Test database"},
		IncludeReservedNetworks: true,
		IPVersion:               4,
		RecordSize:              24,
	})
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("1.2.3.0/24")
	require.NoError(t, tree.Insert(prefix, mmdbtype.String("value")))

	var buf bytes.Buffer
	_, writeErr := tree.WriteTo(&buf)
	require.NoError(t, writeErr)

	dbBytes := append([]byte(nil), buf.Bytes()...)
	nodeSize := 2 * tree.recordSize / 8
	dataStart := tree.nodeCount*nodeSize + len(dataSectionSeparator)
	// Extended type 16 is validly encoded but unsupported by the unmarshaler.
	dbBytes[dataStart], dbBytes[dataStart+1] = 0, 9

	f, err := os.CreateTemp(t.TempDir(), "mmdbwriter-load-corrupt-data-*.mmdb")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(f.Name())) }()

	_, err = f.Write(dbBytes)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Load(f.Name(), Options{
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshaling record for network 1.2.3.0/24")
}

func TestTreeInsertAndGet(t *testing.T) {
	bigInt := big.Int{}
	bigInt.SetString("1329227995784915872903807060280344576", 10)
	uint128 := mmdbtype.Uint128(bigInt)
	var allTypesGetSubmap mmdbtype.DataType = mmdbtype.Map{
		"mapX": mmdbtype.Map{
			"arrayX": mmdbtype.Slice{
				mmdbtype.Uint64(0x7),
				mmdbtype.Uint64(0x8),
				mmdbtype.Uint64(0x9),
			},
			"utf8_stringX": mmdbtype.String("hello"),
		},
	}
	var allTypesGetRecord mmdbtype.DataType = mmdbtype.Map{
		"array": mmdbtype.Slice{
			mmdbtype.Uint64(1),
			mmdbtype.Uint64(2),
			mmdbtype.Uint64(3),
		},
		"boolean": mmdbtype.Bool(true),
		"bytes": mmdbtype.Bytes{
			0x0,
			0x0,
			0x0,
			0x2a,
		},
		"double":      mmdbtype.Float64(42.123456),
		"float":       mmdbtype.Float32(1.1),
		"int32":       mmdbtype.Int32(-268435456),
		"map":         allTypesGetSubmap,
		"uint128":     &uint128,
		"uint16":      mmdbtype.Uint64(0x64),
		"uint32":      mmdbtype.Uint64(0x10000000),
		"uint64":      mmdbtype.Uint64(0x1000000000000000),
		"utf8_string": mmdbtype.String("unicode! ☯ - ♫"),
	}

	var allTypesLookupSubmap any = map[string]any{
		"mapX": map[string]any{
			"arrayX": []any{
				uint64(0x7),
				uint64(0x8),
				uint64(0x9),
			},
			"utf8_stringX": "hello",
		},
	}
	var allTypesLookupRecord any = map[string]any{
		"array": []any{
			uint64(1),
			uint64(2),
			uint64(3),
		},
		"boolean": true,
		"bytes": []uint8{
			0x0,
			0x0,
			0x0,
			0x2a,
		},
		"double":      42.123456,
		"float":       float32(1.1),
		"int32":       int32(-268435456),
		"map":         allTypesLookupSubmap,
		"uint128":     &bigInt,
		"uint16":      uint64(0x64),
		"uint32":      uint64(0x10000000),
		"uint64":      uint64(0x1000000000000000),
		"utf8_string": "unicode! ☯ - ♫",
	}

	stringsGetRecord := mmdbtype.Map{
		// firstSize
		"size28": mmdbtype.String(strings.Repeat("*", 28)),
		"size29": mmdbtype.String(strings.Repeat("*", 29)),
		"size30": mmdbtype.String(strings.Repeat("*", 30)),
		// secondSize
		"size284": mmdbtype.String(strings.Repeat("*", 284)),
		"size285": mmdbtype.String(strings.Repeat("*", 285)),
		"size286": mmdbtype.String(strings.Repeat("*", 286)),
		// thirdSize
		"size65820": mmdbtype.String(strings.Repeat("*", 65820)),
		"size65821": mmdbtype.String(strings.Repeat("*", 65821)),
		"size65822": mmdbtype.String(strings.Repeat("*", 65822)),
		// maxSize
		"maxSizeMinus1": mmdbtype.String(strings.Repeat("*", 16843036)),
	}

	var stringsLookupRecord any = map[string]any{
		"size28":        strings.Repeat("*", 28),
		"size29":        strings.Repeat("*", 29),
		"size30":        strings.Repeat("*", 30),
		"size284":       strings.Repeat("*", 284),
		"size285":       strings.Repeat("*", 285),
		"size286":       strings.Repeat("*", 286),
		"size65820":     strings.Repeat("*", 65820),
		"size65821":     strings.Repeat("*", 65821),
		"size65822":     strings.Repeat("*", 65822),
		"maxSizeMinus1": strings.Repeat("*", 16843036),
	}

	tests := []struct {
		name                    string
		disableIPv4Aliasing     bool
		includeReservedNetworks bool
		insertType              string // "net" or "range or "" for both.
		inserts                 []testInsert
		insertErrors            []testInsertError
		gets                    []testGet
		expectedNodeCount       int
	}{
		{
			name:                    "::/0 insert",
			disableIPv4Aliasing:     true,
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "::/0",
					start:   "::",
					end:     "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "8.1.1.0",
					expectedNetwork:     "::/1",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "8000::",
					expectedNetwork:     "8000::/1",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
			},
			expectedNodeCount: 1,
		},
		{
			name:                    "::/1 insert, IPv4 lookup",
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "::/1",
					start:   "::",
					end:     "7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "1.1.1.1",
					expectedNetwork:     "0.0.0.0/1",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
			},
			expectedNodeCount: 142,
		},
		{
			name:                    "8000::/1 insert",
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "8000::/1",
					start:   "8000::",
					end:     "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "8000::",
					expectedNetwork:     "8000::/1",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
			},
			expectedNodeCount: 142,
		},
		{
			name:                    "overwriting smaller network with bigger network",
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "2003:1000::/32",
					start:   "2003:1000::",
					end:     "2003:1000:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("string"),
				},
				{
					network: "2003::/16",
					start:   "2003::",
					end:     "2003:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("new string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "2003::",
					expectedNetwork:     "2003::/16",
					expectedGetValue:    mmdbtype.String("new string"),
					expectedLookupValue: s2ip("new string"),
				},
				{
					ip:                  "2003:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					expectedNetwork:     "2003::/16",
					expectedGetValue:    mmdbtype.String("new string"),
					expectedLookupValue: s2ip("new string"),
				},
			},
			expectedNodeCount: 142,
		},
		{
			name:                    "insert smaller network into bigger network",
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "2003::/16",
					start:   "2003::",
					end:     "2003:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("string"),
				},
				{
					network: "2003:1000::/32",
					start:   "2003:1000::",
					end:     "2003:1000:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("new string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "2003::",
					expectedNetwork:     "2003::/20",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "2003:1000::",
					expectedNetwork:     "2003:1000::/32",
					expectedGetValue:    mmdbtype.String("new string"),
					expectedLookupValue: s2ip("new string"),
				},
				{
					ip:                  "2003:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					expectedNetwork:     "2003:8000::/17",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
			},
			expectedNodeCount: 158,
		},
		{
			name:                    "inserting IPv4 address in IPv6 tree, without aliasing",
			disableIPv4Aliasing:     true,
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "1.1.1.1/32",
					start:   "1.1.1.1",
					end:     "1.1.1.1",
					value:   mmdbtype.String("string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "1.1.1.1",
					expectedNetwork:     "1.1.1.1/32",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "::1.1.1.1",
					expectedNetwork:     "::101:101/128",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					// The IPv4 network should not be aliased
					ip:              "2002:100:100::",
					expectedNetwork: "2000::/3",
				},
			},
			expectedNodeCount: 128,
		},
		{
			name: "reserved and aliased networks",
			inserts: []testInsert{
				{
					network: "::/1",
					start:   "::",
					end:     "7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					value:   mmdbtype.String("string"),
				},
			},
			insertErrors: []testInsertError{
				{
					network:          "10.0.0.0/8",
					start:            "10.0.0.0",
					end:              "10.255.255.255",
					expectedErrorMsg: "attempt to insert 10.0.0.0/8 into 10.0.0.0/8, which is a reserved network",
				},
				{
					network:          "10.0.0.1/32",
					start:            "10.0.0.1",
					end:              "10.0.0.1",
					expectedErrorMsg: "attempt to insert 10.0.0.1/32 into 10.0.0.0/8, which is a reserved network",
				},
				{
					network:          "2002:100::/24",
					start:            "2002:100::",
					end:              "2002:1ff:ffff:ffff:ffff:ffff:ffff:ffff",
					expectedErrorMsg: "attempt to insert 2002:100::/24 into 2002::/16, which is an aliased network",
				},
			},
			gets: []testGet{
				{
					ip:                  "1.1.1.1",
					expectedNetwork:     "1.0.0.0/8",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					// This is within a reserved network
					ip:              "203.0.113.0",
					expectedNetwork: "203.0.113.0/24",
				},
				{
					// This is in an aliased network
					ip:                  "2002:100:100::",
					expectedNetwork:     "2002:100::/24",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
			},
			expectedNodeCount: 351,
		},
		{
			name: "all types and pointers",
			inserts: []testInsert{
				{
					network: "1.1.1.0/24",
					start:   "1.1.1.0",
					end:     "1.1.1.255",
					value:   allTypesGetSubmap,
				},
				{
					network: "1.1.2.0/24",
					start:   "1.1.2.0",
					end:     "1.1.2.255",
					value:   allTypesGetRecord,
				},
			},
			gets: []testGet{
				{
					ip:                  "1.1.1.0",
					expectedNetwork:     "1.1.1.0/24",
					expectedGetValue:    allTypesGetSubmap,
					expectedLookupValue: &allTypesLookupSubmap,
				},
				{
					ip:                  "1.1.2.128",
					expectedNetwork:     "1.1.2.0/24",
					expectedGetValue:    allTypesGetRecord,
					expectedLookupValue: &allTypesLookupRecord,
				},
			},
			expectedNodeCount: 368,
		},
		{
			name: "node pruning - adjacent",
			inserts: []testInsert{
				{
					network: "1.1.0.0/24",
					start:   "1.1.0.0",
					end:     "1.1.0.255",
					value: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
				},
				{
					network: "1.1.1.0/24",
					start:   "1.1.1.0",
					end:     "1.1.1.255",
					// We intentionally don't use the same variable for
					// here and above as we want them to be different instances.
					value: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
				},
			},
			gets: []testGet{
				{
					ip:              "1.1.0.0",
					expectedNetwork: "1.1.0.0/23",
					expectedGetValue: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
					expectedLookupValue: func() *any {
						v := any(map[string]any{"a": []any{uint64(1), []byte{1, 2}}})
						return &v
					}(),
				},
			},
			expectedNodeCount: 366,
		},
		{
			name: "node pruning - inserting smaller duplicate into larger",
			inserts: []testInsert{
				{
					network: "1.1.0.0/24",
					start:   "1.1.0.0",
					end:     "1.1.0.255",
					value: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
				},
				{
					network: "1.1.0.128/26",
					start:   "1.1.0.128",
					end:     "1.1.0.191",
					// We intentionally don't use the same variable for
					// here and above as we want them to be different instances.
					value: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
				},
			},
			gets: []testGet{
				{
					ip:              "1.1.0.0",
					expectedNetwork: "1.1.0.0/24",
					expectedGetValue: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
					expectedLookupValue: func() *any {
						v := any(map[string]any{"a": []any{uint64(1), []byte{1, 2}}})
						return &v
					}(),
				},
			},
			expectedNodeCount: 367,
		},
		{
			name: "node pruning - inserting smaller non-duplicate and then duplicate into larger",
			inserts: []testInsert{
				{
					network: "1.1.0.0/24",
					start:   "1.1.0.0",
					end:     "1.1.0.255",
					value: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
				},
				{
					network: "1.1.0.128/26",
					start:   "1.1.0.128",
					end:     "1.1.0.191",
					// We intentionally don't use the same variable for
					// here and above as we want them to be different instances.
					value: mmdbtype.Map{"a": mmdbtype.Int32(1)},
				},
				{
					network: "1.1.0.128/26",
					start:   "1.1.0.128",
					end:     "1.1.0.191",
					// We intentionally don't use the same variable for
					// here and above as we want them to be different instances.
					value: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
				},
			},
			gets: []testGet{
				{
					ip:              "1.1.0.0",
					expectedNetwork: "1.1.0.0/24",
					expectedGetValue: mmdbtype.Map{
						"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}},
					},
					expectedLookupValue: func() *any {
						v := any(map[string]any{"a": []any{uint64(1), []byte{1, 2}}})
						return &v
					}(),
				},
			},
			expectedNodeCount: 367,
		},
		{
			name:       "insertion of range with multiple subnets",
			insertType: "range",
			inserts: []testInsert{
				{
					start: "1.1.1.0",
					end:   "1.1.1.6",
					value: mmdbtype.String("string"),
				},
			},
			gets: []testGet{
				{
					ip:                  "1.1.1.0",
					expectedNetwork:     "1.1.1.0/30",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "1.1.1.1",
					expectedNetwork:     "1.1.1.0/30",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "1.1.1.2",
					expectedNetwork:     "1.1.1.0/30",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "1.1.1.3",
					expectedNetwork:     "1.1.1.0/30",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "1.1.1.4",
					expectedNetwork:     "1.1.1.4/31",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "1.1.1.5",
					expectedNetwork:     "1.1.1.4/31",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
				{
					ip:                  "1.1.1.6",
					expectedNetwork:     "1.1.1.6/32",
					expectedGetValue:    mmdbtype.String("string"),
					expectedLookupValue: s2ip("string"),
				},
			},
			expectedNodeCount: 375,
		},
		{
			name: "insertion of strings at boundary control byte size",
			inserts: []testInsert{
				{
					network: "1.1.1.1/32",
					start:   "1.1.1.1",
					end:     "1.1.1.1",
					value:   stringsGetRecord,
				},
			},
			gets: []testGet{
				{
					ip:                  "1.1.1.1",
					expectedNetwork:     "1.1.1.1/32",
					expectedGetValue:    stringsGetRecord,
					expectedLookupValue: &stringsLookupRecord,
				},
			},
			expectedNodeCount: 375,
		},
	}

	for _, recordSize := range []int{24, 28, 32} {
		t.Run(fmt.Sprintf("Record Size: %d", recordSize), func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					epochSec := time.Now().Unix()
					tree, err := New(
						Options{
							BuildEpoch:              epochSec,
							DatabaseType:            "mmdbwriter-test",
							Description:             map[string]string{"en": "Test database"},
							DisableIPv4Aliasing:     test.disableIPv4Aliasing,
							IncludeReservedNetworks: test.includeReservedNetworks,
							RecordSize:              recordSize,
						},
					)
					require.NoError(t, err)
					switch test.insertType {
					case "", "net":
						for _, insert := range test.inserts {
							network, err := netip.ParsePrefix(insert.network)
							require.NoError(t, err)

							require.NoError(t, tree.Insert(network, insert.value))
						}
						for _, insert := range test.insertErrors {
							network, err := netip.ParsePrefix(insert.network)
							require.NoError(t, err)

							err = tree.Insert(network, insert.value)

							require.EqualError(t, err, insert.expectedErrorMsg)
						}
					case "range":
						for _, insert := range test.inserts {
							start, err := netip.ParseAddr(insert.start)
							require.NoError(t, err)
							end, err := netip.ParseAddr(insert.end)
							require.NoError(t, err)

							require.NoError(t, tree.InsertRange(start, end, insert.value))
						}
						for _, insert := range test.insertErrors {
							start, err := netip.ParseAddr(insert.start)
							require.NoError(t, err)
							end, err := netip.ParseAddr(insert.end)
							require.NoError(t, err)

							err = tree.InsertRange(start, end, insert.value)
							require.EqualError(t, err, insert.expectedErrorMsg)
						}
					}

					tree.finalize()

					for _, get := range test.gets {
						network, value := tree.Get(netip.MustParseAddr(get.ip))

						assert.Equal(
							t,
							get.expectedNetwork,
							network.String(),
							"network for %s",
							get.ip,
						)
						assert.Equal(t, get.expectedGetValue, value, "value for %s", get.ip)
					}

					assert.Equal(t, test.expectedNodeCount, tree.nodeCount)

					buf := &bytes.Buffer{}
					numBytes, err := tree.WriteTo(buf)
					require.NoError(t, err)

					checkMMDB(t, buf, test.gets, "MMDB lookups on New tree")

					assert.Equal(t, int64(buf.Len()), numBytes, "number of bytes")

					f, err := os.CreateTemp(t.TempDir(), "mmdbwriter")
					require.NoError(t, err)
					defer func() { require.NoError(t, os.Remove(f.Name())) }()

					bufBytes := buf.Bytes()

					_, err = f.Write(bufBytes)
					require.NoError(t, err)
					require.NoError(t, f.Close())

					loadBuf := &bytes.Buffer{}
					tree, err = Load(f.Name(),
						Options{
							BuildEpoch:              epochSec,
							DisableIPv4Aliasing:     test.disableIPv4Aliasing,
							IncludeReservedNetworks: test.includeReservedNetworks,
						},
					)
					require.NoError(t, err)

					_, err = tree.WriteTo(loadBuf)
					require.NoError(t, err)

					checkMMDB(t, loadBuf, test.gets, "MMDB lookups on Load tree")

					assert.Equal(
						t,
						bufBytes,
						loadBuf.Bytes(),
						"Load + WriteTo generates an identical database",
					)
				})
			}
		})
	}
}

func checkMMDB(t *testing.T, buf *bytes.Buffer, gets []testGet, name string) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		reader, err := maxminddb.OpenBytes(buf.Bytes())
		require.NoError(t, err)

		defer reader.Close()

		for _, get := range gets {
			var v any

			res := reader.Lookup(netip.MustParseAddr(get.ip))
			err := res.Decode(&v)
			require.NoError(t, err)

			assert.Equal(
				t,
				get.expectedNetwork,
				res.Prefix().String(),
				"network for %s in database",
				get.ip,
			)

			if get.expectedLookupValue == nil {
				assert.False(t, res.Found(), "%s is not in the database", get.ip)
			} else {
				assert.Equal(t, *get.expectedLookupValue, v, "value for %s in database", get.ip)
			}
		}
		require.NoError(t, reader.Verify(), "verify database format")
	})
}

// This test case exists to test a bug that we experienced where a value
// could reappear on a later insert after being removed from the record.
// This happened as we were only changing the record type and not
// removing the underlying data.
func TestInsertFunc_RemovalAndLaterInsert(t *testing.T) {
	tree, err := New(
		Options{},
	)
	require.NoError(t, err)

	network := netip.MustParsePrefix("::1.1.1.0/120")

	value := mmdbtype.String("value")
	require.NoError(t, tree.Insert(network, value))

	ip := netip.MustParseAddr("::1.1.1.1")

	recNetwork, recValue := tree.Get(ip)

	assert.Equal(t, network, recNetwork)
	assert.Equal(t, value, recValue)

	removedNetwork := netip.MustParsePrefix("::1.1.1.1/128")

	err = tree.InsertFunc(
		removedNetwork,
		nil,
		inserter.Remove,
	)
	require.NoError(t, err)

	recNetwork, recValue = tree.Get(ip)

	assert.Equal(t, removedNetwork, recNetwork)
	assert.Nil(t, recValue)

	err = tree.InsertFunc(
		removedNetwork,
		nil,
		inserter.Func(func(v, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return v, nil
		}),
	)
	require.NoError(t, err)

	recNetwork, recValue = tree.Get(ip)

	assert.Equal(t, removedNetwork, recNetwork)
	assert.Nil(t, recValue)
}

// See GitHub #62.
func TestGet_IPv4MappedIn128BitTree(t *testing.T) {
	writer, err := New(Options{DatabaseType: "GitHub #62"})
	require.NoError(t, err)

	network := netip.MustParsePrefix("1.0.0.0/24")

	err = writer.Insert(network, mmdbtype.Map{"country_code": mmdbtype.String("AU")})
	require.NoError(t, err)

	getNetwork, _ := writer.Get(netip.MustParseAddr("1.0.0.1"))

	assert.Equal(t, network.String(), getNetwork.String(), "IPv4 lookup")

	getNetwork, _ = writer.Get(netip.MustParseAddr("::ffff:1.0.0.1"))

	assert.Equal(t, network.String(), getNetwork.String(), "IPv4-mapped lookup")
}

func s2ip(v string) *any {
	i := any(v)
	return &i
}

// TestInsertPureFuncEqualResultKeepsReference pins the addRef inside the branch
// where a pure inserter returns a value equal to the existing one. Without it,
// releaseResolved drops the record's own reference and unlinks a value a record
// still points at. It does not pin the branch itself: falling through to
// dataMap.store instead is behaviorally equivalent.
func TestInsertPureFuncEqualResultKeepsReference(t *testing.T) {
	tree := newTestTree(t, "mmdbwriter-pure-equal")

	prefix := netip.MustParsePrefix("1.0.0.0/8")
	require.NoError(t, tree.Insert(prefix, mmdbtype.String("value")))

	require.NoError(t, tree.InsertPureFunc(
		prefix,
		mmdbtype.String("value"),
		func(existingValue, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return existingValue, nil
		},
	))

	require.Len(t, tree.dataMap.data, 1,
		"the retained value was unlinked while a record still referenced it")
	for _, dmv := range tree.dataMap.data {
		assert.NotZero(t, dmv.refCount,
			"the retained value has no references but is still in the tree")
	}

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("2.0.0.0/8"), mmdbtype.String("value")))
	assert.Len(t, tree.dataMap.data, 1, "an equal value failed to deduplicate")
}

// TestInsertFuncReleasesRedundantStoredReference covers the other uncovered
// ownership arm: an ordinary inserter whose result is not Equal to the existing
// value, but which deduplicates back onto that same value once stored. The
// reference the store took must be released.
func TestInsertFuncReleasesRedundantStoredReference(t *testing.T) {
	tree := newTestTree(t, "mmdbwriter-owned-release")

	prefix := netip.MustParsePrefix("1.0.0.0/8")
	require.NoError(t, tree.Insert(prefix, mmdbtype.String("shared")))

	require.Len(t, tree.dataMap.data, 1)
	var stored *dataMapValue
	for _, value := range tree.dataMap.data {
		stored = value
	}
	require.EqualValues(t, 1, stored.refCount)

	// A pointer form is not Equal to the value form, so resolve stores it. The
	// store deduplicates back onto the record's own value, leaving a reference
	// that must be released.
	shared := mmdbtype.String("shared")
	require.NoError(t, tree.InsertFunc(
		prefix,
		mmdbtype.String("ignored"),
		func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return &shared, nil
		},
	))

	assert.Len(t, tree.dataMap.data, 1)
	assert.EqualValues(t, 1, stored.refCount, "a redundant stored reference leaked")
}

// TestInsertPureFuncMatchesInsertFuncOutput is the differential that most
// directly encodes the contract of the pure insert methods: memoizing a pure
// function must not change the database.
func TestInsertPureFuncMatchesInsertFuncOutput(t *testing.T) {
	build := func(pure bool) []byte {
		tree, err := New(Options{
			DatabaseType:            "mmdbwriter-pure-differential",
			Description:             map[string]string{"en": "Test database"},
			IPVersion:               4,
			RecordSize:              24,
			IncludeReservedNetworks: true,
			BuildEpoch:              1,
		})
		require.NoError(t, err)

		for i := range 8 {
			require.NoError(t, tree.Insert(
				netip.MustParsePrefix(
					netip.AddrFrom4([4]byte{1, byte(i), 0, 0}).String()+"/16"),
				mmdbtype.Map{"n": mmdbtype.Uint32(uint32(i % 3))},
			))
		}

		prefix := netip.MustParsePrefix("1.0.0.0/8")
		value := mmdbtype.Map{"added": mmdbtype.String("x")}
		if pure {
			require.NoError(t, tree.InsertPureFunc(prefix, value, inserter.DeepMerge))
		} else {
			require.NoError(t, tree.InsertFunc(prefix, value, inserter.DeepMerge))
		}

		var buf bytes.Buffer
		_, writeErr := tree.WriteTo(&buf)
		require.NoError(t, writeErr)
		return buf.Bytes()
	}

	assert.Equal(t, build(false), build(true))
}

// TestInsertPureFuncNilResultRemovesRecords covers a memoized nil result, which
// is the common outcome for inserter.Remove.
func TestInsertPureFuncNilResultRemovesRecords(t *testing.T) {
	tree := newTestTree(t, "mmdbwriter-pure-nil")

	for i := range 4 {
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix(
				netip.AddrFrom4([4]byte{1, byte(i), 0, 0}).String()+"/16"),
			mmdbtype.String("value"),
		))
	}

	require.NoError(t, tree.InsertPureFunc(
		netip.MustParsePrefix("1.0.0.0/8"),
		mmdbtype.String("ignored"),
		inserter.Remove,
	))

	assert.Empty(t, tree.dataMap.data, "removed values were not released")
}

// TestInsertPureFuncCreatesPathFromEmptySpace covers the compressed-path branch
// a pure inserter takes when nothing covers the network yet.
func TestInsertPureFuncCreatesPathFromEmptySpace(t *testing.T) {
	tree := newTestTree(t, "mmdbwriter-pure-empty")

	prefix := netip.MustParsePrefix("9.9.9.0/24")
	require.NoError(t, tree.InsertPureFunc(
		prefix, mmdbtype.String("value"), inserter.Replace))

	gotPrefix, value := tree.Get(netip.MustParseAddr("9.9.9.1"))
	assert.Equal(t, prefix, gotPrefix)
	assert.Equal(t, mmdbtype.String("value"), value)

	var buf bytes.Buffer
	_, writeErr := tree.WriteTo(&buf)
	require.NoError(t, writeErr)
	assert.NotZero(t, buf.Len())
}

// writeMetadataPatchedDB writes a valid database with one metadata value
// replaced. Both ip_version and record_size are encoded as a uint16 with a
// one-byte payload, so the byte after the key and its control byte is the
// value.
func writeMetadataPatchedDB(t *testing.T, key string, value byte) string {
	t.Helper()

	tree := newTestTree(t, "mmdbwriter-metadata")
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("value"),
	))

	var buf bytes.Buffer
	_, writeErr := tree.WriteTo(&buf)
	require.NoError(t, writeErr)

	dbBytes := append([]byte(nil), buf.Bytes()...)
	i := bytes.LastIndex(dbBytes, []byte(key))
	require.GreaterOrEqual(t, i, 0)
	dbBytes[i+len(key)+1] = value

	f, err := os.CreateTemp(t.TempDir(), "mmdbwriter-metadata-*.mmdb")
	require.NoError(t, err)
	_, err = f.Write(dbBytes)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return f.Name()
}

// TestLoadRejectsUnsupportedMetadataDimensions covers the metadata validation
// added for loaded databases.
func TestLoadRejectsUnsupportedMetadataDimensions(t *testing.T) {
	t.Run("unsupported ip version", func(t *testing.T) {
		path := writeMetadataPatchedDB(t, "ip_version", 5)

		_, err := Load(path, Options{IncludeReservedNetworks: true})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported IPVersion: 5")
		assert.Contains(t, err.Error(), path,
			"the error does not say which database failed")
	})

	t.Run("unsupported record size", func(t *testing.T) {
		path := writeMetadataPatchedDB(t, "record_size", 20)

		_, err := Load(path, Options{IncludeReservedNetworks: true})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported RecordSize: 20")
	})

	t.Run("absent record size", func(t *testing.T) {
		path := writeMetadataPatchedDB(t, "record_size", 0)

		_, err := Load(path, Options{IncludeReservedNetworks: true})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported record_size in metadata: 0")
		assert.Contains(t, err.Error(), path,
			"the error does not say which database failed")
	})

	t.Run("absent ip version", func(t *testing.T) {
		path := writeMetadataPatchedDB(t, "ip_version", 0)

		_, err := Load(path, Options{IncludeReservedNetworks: true})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported ip_version in metadata: 0")
	})

	t.Run("explicit options bypass the metadata values", func(t *testing.T) {
		path := writeMetadataPatchedDB(t, "ip_version", 5)

		_, err := Load(path, Options{
			IPVersion:               4,
			RecordSize:              24,
			IncludeReservedNetworks: true,
		})

		require.NoError(t, err)
	})
}

// TestSignedZeroIsNeverSilentlyDropped pins that an inserted signed zero
// replaces an existing one of the opposite sign. The two encodings differ on
// the wire, so keeping the old value would silently discard the new sign. This
// must not depend on whether an unrelated sibling key also changed.
func TestSignedZeroIsNeverSilentlyDropped(t *testing.T) {
	negativeZero := mmdbtype.Float64(math.Copysign(0, -1))

	tests := []struct {
		name     string
		newValue mmdbtype.Map
	}{
		{
			name: "only the signed zero changes",
			newValue: mmdbtype.Map{
				"zero":    negativeZero,
				"sibling": mmdbtype.String("same"),
			},
		},
		{
			name: "the signed zero and a sibling change",
			newValue: mmdbtype.Map{
				"zero":    negativeZero,
				"sibling": mmdbtype.String("changed"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := newTestTree(t, "mmdbwriter-signed-zero")

			prefix := netip.MustParsePrefix("1.0.0.0/8")
			require.NoError(t, tree.Insert(prefix, mmdbtype.Map{
				"zero":    mmdbtype.Float64(0),
				"sibling": mmdbtype.String("same"),
			}))
			require.NoError(t, tree.InsertFunc(prefix, test.newValue, inserter.DeepMerge))

			_, value := tree.Get(netip.MustParseAddr("1.2.3.4"))
			stored := float64(value.(mmdbtype.Map)["zero"].(mmdbtype.Float64))
			assert.True(t, math.Signbit(stored),
				"the inserted negative zero was replaced by the existing positive zero")
		})
	}

	t.Run("the sign survives encoding and a reader", func(t *testing.T) {
		tree := newTestTree(t, "mmdbwriter-signed-zero")

		prefix := netip.MustParsePrefix("1.0.0.0/8")
		require.NoError(t, tree.Insert(prefix, mmdbtype.Float64(0)))
		require.NoError(t, tree.Insert(prefix, negativeZero))

		var buf bytes.Buffer
		_, writeErr := tree.WriteTo(&buf)
		require.NoError(t, writeErr)

		reader, err := maxminddb.OpenBytes(buf.Bytes())
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()

		var stored float64
		require.NoError(t, reader.Lookup(netip.MustParseAddr("1.2.3.4")).Decode(&stored))
		assert.True(t, math.Signbit(stored),
			"the negative zero did not survive encoding")
	})

	t.Run("a plain insert also keeps the new sign", func(t *testing.T) {
		tree := newTestTree(t, "mmdbwriter-signed-zero")

		prefix := netip.MustParsePrefix("1.0.0.0/8")
		require.NoError(t, tree.Insert(prefix, mmdbtype.Float64(0)))
		require.NoError(t, tree.Insert(prefix, negativeZero))

		_, value := tree.Get(netip.MustParseAddr("1.2.3.4"))
		assert.True(t, math.Signbit(float64(value.(mmdbtype.Float64))))
	})
}

// TestNewRejectsUnsupportedOptions covers the Options validation in New. An
// unsupported record size previously reached serialization, where a negative
// value panicked in make and other values failed only after bytes had already
// been written.
func TestNewRejectsUnsupportedOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		errText string
	}{
		{
			name:    "record size below the supported range",
			options: Options{RecordSize: 20},
			errText: "unsupported RecordSize: 20",
		},
		{
			name:    "record size between supported values",
			options: Options{RecordSize: 30},
			errText: "unsupported RecordSize: 30",
		},
		{
			name:    "negative record size",
			options: Options{RecordSize: -8},
			errText: "unsupported RecordSize: -8",
		},
		{
			name:    "negative build epoch",
			options: Options{BuildEpoch: -1},
			errText: "BuildEpoch must not be negative: -1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := test.options
			options.DatabaseType = "mmdbwriter-options"
			options.Description = map[string]string{"en": "Test database"}

			tree, err := New(options)

			require.Error(t, err)
			assert.Nil(t, tree)
			assert.Contains(t, err.Error(), test.errText)
		})
	}
}

// TestNewAcceptsSupportedRecordSizes is the control for
// TestNewRejectsUnsupportedOptions.
func TestNewAcceptsSupportedRecordSizes(t *testing.T) {
	for _, recordSize := range []int{24, 28, 32} {
		t.Run(strconv.Itoa(recordSize), func(t *testing.T) {
			_, err := New(Options{
				DatabaseType: "mmdbwriter-options",
				Description:  map[string]string{"en": "Test database"},
				RecordSize:   recordSize,
			})
			require.NoError(t, err)
		})
	}
}

// TestInsertNilValueRemovesRecord covers the direct-value removal path. The
// inserter path has its own removal implementation in replaceDataRecord, so
// this one is only reached by a plain Insert of a nil value.
func TestInsertNilValueRemovesRecord(t *testing.T) {
	tree := newTestTree(t, "mmdbwriter-insert-nil")

	prefix := netip.MustParsePrefix("1.0.0.0/8")
	require.NoError(t, tree.Insert(prefix, mmdbtype.String("value")))
	require.Len(t, tree.dataMap.data, 1)

	require.NoError(t, tree.Insert(prefix, nil))

	_, value := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Nil(t, value)
	assert.Empty(t, tree.dataMap.data, "the removed value was not released")
}

// TestInserterNilResultOverEmptySpaceIsNoOp covers the guard for an inserter
// that returns nil for a network nothing covers. Without it the tree gains a
// data record with no value, which nil-dereferences when written.
func TestInserterNilResultOverEmptySpaceIsNoOp(t *testing.T) {
	tests := []struct {
		name   string
		insert func(*Tree) error
	}{
		{
			name: "InsertFunc",
			insert: func(tree *Tree) error {
				return tree.InsertFunc(
					netip.MustParsePrefix("9.9.9.0/24"),
					mmdbtype.String("ignored"),
					inserter.Remove,
				)
			},
		},
		{
			name: "InsertPureFunc",
			insert: func(tree *Tree) error {
				return tree.InsertPureFunc(
					netip.MustParsePrefix("9.9.9.0/24"),
					mmdbtype.String("ignored"),
					inserter.Remove,
				)
			},
		},
		{
			name: "InsertRangePureFunc",
			insert: func(tree *Tree) error {
				return tree.InsertRangePureFunc(
					netip.MustParseAddr("9.9.9.0"),
					netip.MustParseAddr("9.9.9.255"),
					mmdbtype.String("ignored"),
					inserter.Remove,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := newTestTree(t, "mmdbwriter-empty-space")

			require.NoError(t, test.insert(tree))

			_, value := tree.Get(netip.MustParseAddr("9.9.9.1"))
			assert.Nil(t, value)
			assert.Empty(t, tree.dataMap.data)

			var buf bytes.Buffer
			_, writeErr := tree.WriteTo(&buf)
			require.NoError(t, writeErr, "the tree could not be written")
		})
	}
}

// TestInsertReportsHashErrorFromIdentityPath covers the hash failure returned
// through storeWithIdentity, which is the path a plain Insert takes.
func TestInsertReportsHashErrorFromIdentityPath(t *testing.T) {
	tree := newTestTree(t, "mmdbwriter-identity-error")

	err := tree.Insert(
		netip.MustParsePrefix("1.0.0.0/8"),
		mmdbtype.Map{"u": (*mmdbtype.Uint128)(nil)},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `hashing map key "u"`)
	assert.Contains(t, err.Error(), "cannot hash a nil *mmdbtype.Uint128")
	// A Map has an identity key, so the failure happens after the identity
	// lookup and must not leave anything cached.
	assert.Empty(t, tree.dataMap.valueByDataIdentity)
	assert.Empty(t, tree.dataMap.data)
}

// newTestTree builds the tree shape most tests want. Tests that depend on a
// particular option set it explicitly instead.
func newTestTree(t *testing.T, databaseType string) *Tree {
	t.Helper()

	tree, err := New(Options{
		DatabaseType:            databaseType,
		Description:             map[string]string{"en": "Test database"},
		IPVersion:               4,
		RecordSize:              24,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)
	return tree
}
