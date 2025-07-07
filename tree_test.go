package mmdbwriter

import (
	"bytes"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/inserter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
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
					switch test.insertType {
					case "", "net":
						for _, insert := range test.inserts {
							//nolint:forbidigo // code predates netip
							_, network, err := net.ParseCIDR(insert.network)
							require.NoError(t, err)

							require.NoError(t, tree.Insert(network, insert.value))
						}
						for _, insert := range test.insertErrors {
							//nolint:forbidigo // code predates netip
							_, network, err := net.ParseCIDR(insert.network)
							require.NoError(t, err)

							err = tree.Insert(network, insert.value)

							require.EqualError(t, err, insert.expectedErrorMsg)
						}
					case "range":
						for _, insert := range test.inserts {
							//nolint:forbidigo // code predates netip
							start := net.ParseIP(insert.start)
							require.NotNil(t, start)
							//nolint:forbidigo // code predates netip
							end := net.ParseIP(insert.end)
							require.NotNil(t, end)

							require.NoError(t, tree.InsertRange(start, end, insert.value))
						}
						for _, insert := range test.insertErrors {
							//nolint:forbidigo // code predates netip
							start := net.ParseIP(insert.start)
							require.NotNil(t, start)
							//nolint:forbidigo // code predates netip
							end := net.ParseIP(insert.end)
							require.NotNil(t, end)

							err = tree.InsertRange(start, end, insert.value)
							require.EqualError(t, err, insert.expectedErrorMsg)
						}
					}

					tree.finalize()

					for _, get := range test.gets {
						//nolint:forbidigo // code predates netip
						network, value := tree.Get(net.ParseIP(get.ip))

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
							BuildEpoch:              epoch,
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
		reader, err := maxminddb.FromBytes(buf.Bytes())
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

	//nolint:forbidigo // code predates netip
	_, network, err := net.ParseCIDR("::1.1.1.0/120")
	require.NoError(t, err)

	value := mmdbtype.String("value")
	require.NoError(t, tree.Insert(network, value))

	//nolint:forbidigo // code predates netip
	ip := net.ParseIP("::1.1.1.1")

	recNetwork, recValue := tree.Get(ip)

	assert.Equal(t, network, recNetwork)
	assert.Equal(t, value, recValue)

	//nolint:forbidigo // code predates netip
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

// See GitHub #62.
func TestGet_4ByteIPIn128BitTree(t *testing.T) {
	writer, err := New(Options{DatabaseType: "GitHub #62"})
	require.NoError(t, err)

	//nolint:forbidigo // code predates netip
	ip, network, err := net.ParseCIDR("1.0.0.0/24")
	require.NoError(t, err)

	err = writer.Insert(network, mmdbtype.Map{"country_code": mmdbtype.String("AU")})
	require.NoError(t, err)

	getNetwork, _ := writer.Get(ip.To4())

	assert.Equal(t, network.String(), getNetwork.String(), "4-byte lookup")

	getNetwork, _ = writer.Get(ip.To16())

	assert.Equal(t, network.String(), getNetwork.String(), "16-byte lookup")
}

func s2ip(v string) *any {
	i := any(v)
	return &i
}
