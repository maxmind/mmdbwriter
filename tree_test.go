package mmdbwriter

import (
	"bytes"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testInsert struct {
	network string
	value   string
}

type testGet struct {
	ip              string
	expectedNetwork string
	expectedValue   *string
}

func TestTreeInsertAndGet(t *testing.T) {
	tests := []struct {
		name              string
		inserts           []testInsert
		gets              []testGet
		expectedNodeCount int
	}{
		{
			name: "::/1 insert, IPv4 lookup",
			inserts: []testInsert{
				{
					network: "::/1",
					value:   "string",
				},
			},
			gets: []testGet{
				{
					ip:              "1.1.1.1",
					expectedNetwork: "::/1",
					expectedValue:   s2sp("string"),
				},
			},
			expectedNodeCount: 1,
		},
		{
			name: "overwriting smaller network with bigger network",
			inserts: []testInsert{
				{
					network: "2002:1000::/32",
					value:   "string",
				},
				{
					network: "2002::/16",
					value:   "new string",
				},
			},
			gets: []testGet{
				{
					ip:              "2002::",
					expectedNetwork: "2002::/16",
					expectedValue:   s2sp("new string"),
				},
				{
					ip:              "2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					expectedNetwork: "2002::/16",
					expectedValue:   s2sp("new string"),
				},
			},
			expectedNodeCount: 16,
		},
		{
			name: "insert smaller network into bigger network",
			inserts: []testInsert{
				{
					network: "2002::/16",
					value:   "string",
				},
				{
					network: "2002:1000::/32",
					value:   "new string",
				},
			},
			gets: []testGet{
				{
					ip:              "2002::",
					expectedNetwork: "2002::/20",
					expectedValue:   s2sp("string"),
				},
				{
					ip:              "2002:1000::",
					expectedNetwork: "2002:1000::/32",
					expectedValue:   s2sp("new string"),
				},
				{
					ip:              "2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					expectedNetwork: "2002:8000::/17",
					expectedValue:   s2sp("string"),
				},
			},
			expectedNodeCount: 32,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := New()
			for _, insert := range test.inserts {
				_, network, err := net.ParseCIDR(insert.network)
				require.NoError(t, err)

				require.NoError(t, tree.Insert(network, insert.value))
			}

			for _, get := range test.gets {
				network, value := tree.Get(net.ParseIP(get.ip))

				assert.Equal(t, get.expectedNetwork, network.String(), "network for %s", get.ip)
				assert.Equal(t, get.expectedValue, value, "value for %s", get.ip)
			}

			tree.Finalize()

			assert.Equal(t, test.expectedNodeCount, tree.nodeCount)

			buf := &bytes.Buffer{}
			numBytes, err := tree.WriteTo(buf)
			require.NoError(t, err)

			assert.Equal(t, int64(buf.Len()), numBytes)
		})
	}
}

func s2sp(v string) *string { return &v }
