package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"os"
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

	keyBytes, err := tree.dataMap.keyWriter.Key(initialValue)
	require.NoError(t, err)
	key := dataMapKey(keyBytes)
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
	_, err = tree.WriteTo(&buf)
	require.NoError(t, err)

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
		func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return nil, insertErr
		},
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
		func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return nil, insertErr
		},
	)
	require.ErrorIs(t, err, insertErr)

	network, got := tree.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, prefix, network)
	assert.Equal(t, base, got)
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
	_, err = tree.WriteTo(&buf)
	require.NoError(t, err)

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
		func(v, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
			return v, nil
		},
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
