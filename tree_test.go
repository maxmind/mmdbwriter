package mmdbwriter

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/oschwald/maxminddb-golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testInsert struct {
	network string
	value   mmdbtype.DataType
}

type testInsertError struct {
	network          string
	value            mmdbtype.DataType
	expectedErrorMsg string
}

type testGet struct {
	ip                  string
	expectedNetwork     string
	expectedGetValue    mmdbtype.DataType
	expectedLookupValue *interface{}
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

	var allTypesLookupSubmap interface{} = map[string]interface{}{
		"mapX": map[string]interface{}{
			"arrayX": []interface{}{
				uint64(0x7),
				uint64(0x8),
				uint64(0x9),
			},
			"utf8_stringX": "hello",
		},
	}
	var allTypesLookupRecord interface{} = map[string]interface{}{
		"array": []interface{}{
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
		"int32":       -268435456,
		"map":         allTypesLookupSubmap,
		"uint128":     &bigInt,
		"uint16":      uint64(0x64),
		"uint32":      uint64(0x10000000),
		"uint64":      uint64(0x1000000000000000),
		"utf8_string": "unicode! ☯ - ♫",
	}

	tests := []struct {
		name                    string
		disableIPv4Aliasing     bool
		includeReservedNetworks bool
		inserts                 []testInsert
		insertErrors            []testInsertError
		gets                    []testGet
		expectedNodeCount       int
	}{
		{
			name:                    "::/1 insert, IPv4 lookup",
			includeReservedNetworks: true,
			inserts: []testInsert{
				{
					network: "::/1",
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
					value:   mmdbtype.String("string"),
				},
				{
					network: "2003::/16",
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
					value:   mmdbtype.String("string"),
				},
				{
					network: "2003:1000::/32",
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
					value:   mmdbtype.String("string"),
				},
			},
			insertErrors: []testInsertError{
				{
					network:          "10.0.0.0/8",
					expectedErrorMsg: "attempt to insert ::a00:0/104, which is in a reserved network",
				},
				{
					network:          "10.0.0.1/32",
					expectedErrorMsg: "attempt to insert ::a00:1/128, which is in a reserved network",
				},
				{
					network:          "2002:100::/24",
					expectedErrorMsg: "attempt to insert 2002:100::/24, which is in an aliased network",
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
			expectedNodeCount: 352,
		},
		{
			name: "all types and pointers",
			inserts: []testInsert{
				{
					network: "1.1.1.0/24",
					value:   allTypesGetSubmap,
				},
				{
					network: "1.1.2.0/24",
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
			expectedNodeCount: 369,
		},
		{
			name: "node pruning",
			inserts: []testInsert{
				{
					network: "1.1.0.0/24",
					value:   mmdbtype.Map{"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}}},
				},
				{
					network: "1.1.1.0/24",
					// We intentionally don't use the same variable for
					// here and above as we want them to be different instances.
					value: mmdbtype.Map{"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}}},
				},
			},
			gets: []testGet{
				{
					ip:               "1.1.0.0",
					expectedNetwork:  "1.1.0.0/23",
					expectedGetValue: mmdbtype.Map{"a": mmdbtype.Slice{mmdbtype.Uint64(1), mmdbtype.Bytes{1, 2}}},
					expectedLookupValue: func() *interface{} {
						v := interface{}(map[string]interface{}{"a": []interface{}{uint64(1), []byte{1, 2}}})
						return &v
					}(),
				},
			},
			expectedNodeCount: 367,
		},
	}

	for _, recordSize := range []int{24, 28, 32} {
		t.Run(fmt.Sprintf("Record Size: %d", recordSize), func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					epoch := time.Now().Unix()
					tree, err := New(
						Options{
							BuildEpoch:              epoch,
							DatabaseType:            "mmdbwriter-test",
							Description:             map[string]string{"en": "Test database"},
							DisableIPv4Aliasing:     test.disableIPv4Aliasing,
							IncludeReservedNetworks: test.includeReservedNetworks,
							RecordSize:              recordSize,
						},
					)
					require.NoError(t, err)
					for _, insert := range test.inserts {
						_, network, err := net.ParseCIDR(insert.network)
						require.NoError(t, err)

						require.NoError(t, tree.Insert(network, insert.value))
					}

					for _, insert := range test.insertErrors {
						_, network, err := net.ParseCIDR(insert.network)
						require.NoError(t, err)

						err = tree.Insert(network, insert.value)

						assert.EqualError(t, err, insert.expectedErrorMsg)
					}

					tree.finalize()

					for _, get := range test.gets {
						network, value := tree.Get(net.ParseIP(get.ip))

						assert.Equal(t, get.expectedNetwork, network.String(), "network for %s", get.ip)
						assert.Equal(t, get.expectedGetValue, value, "value for %s", get.ip)
					}

					assert.Equal(t, test.expectedNodeCount, tree.nodeCount)

					buf := &bytes.Buffer{}
					numBytes, err := tree.WriteTo(buf)
					require.NoError(t, err)

					reader, err := maxminddb.FromBytes(buf.Bytes())
					require.NoError(t, err)

					for _, get := range test.gets {
						var v interface{}
						network, ok, err := reader.LookupNetwork(net.ParseIP(get.ip), &v)
						require.NoError(t, err)

						assert.Equal(t, get.expectedNetwork, network.String(), "network for %s in database", get.ip)

						if get.expectedLookupValue == nil {
							assert.False(t, ok, "%s is not in the database", get.ip)
						} else {
							assert.Equal(t, *get.expectedLookupValue, v, "value for %s in database", get.ip)
						}
					}

					assert.NoError(t, reader.Verify(), "verify database format")

					assert.Equal(t, int64(buf.Len()), numBytes, "number of bytes")

					f, err := ioutil.TempFile("", "mmdbwriter")
					require.NoError(t, err)
					defer func() { require.NoError(t, os.Remove(f.Name())) }()

					bufBytes := buf.Bytes()

					_, err = f.Write(bufBytes)
					require.NoError(t, err)
					require.NoError(t, f.Close())

					loadBuf := &bytes.Buffer{}
					tree, err = Load(f.Name(),
						Options{
							BuildEpoch:              epoch,
							DisableIPv4Aliasing:     test.disableIPv4Aliasing,
							IncludeReservedNetworks: test.includeReservedNetworks,
						},
					)
					require.NoError(t, err)

					_, err = tree.WriteTo(loadBuf)
					require.NoError(t, err)

					assert.Equal(t, bufBytes, loadBuf.Bytes(), "Load + WriteTo generates an identical database")
				})
			}
		})
	}
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

	_, network, err := net.ParseCIDR("::1.1.1.0/120")
	require.NoError(t, err)

	value := mmdbtype.String("value")
	require.NoError(t, tree.Insert(network, value))

	ip := net.ParseIP("::1.1.1.1")

	recNetwork, recValue := tree.Get(ip)

	assert.Equal(t, network, recNetwork)
	assert.Equal(t, value, recValue)

	_, removedNetwork, err := net.ParseCIDR("::1.1.1.1/128")
	require.NoError(t, err)

	err = tree.InsertFunc(
		removedNetwork,
		inserter.Remove,
	)
	require.NoError(t, err)

	recNetwork, recValue = tree.Get(ip)

	assert.Equal(t, removedNetwork, recNetwork)
	assert.Nil(t, recValue)

	err = tree.InsertFunc(
		removedNetwork,
		func(v mmdbtype.DataType) (mmdbtype.DataType, error) {
			return v, nil
		},
	)
	require.NoError(t, err)

	recNetwork, recValue = tree.Get(ip)

	assert.Equal(t, removedNetwork, recNetwork)
	assert.Nil(t, recValue)
}

func s2ip(v string) *interface{} {
	i := interface{}(v)
	return &i
}
