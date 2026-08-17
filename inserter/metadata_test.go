package inserter

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadataDerivedMethods(t *testing.T) {
	v4Addr := [16]byte{1, 2, 3}
	v4InV6Addr := [16]byte{12: 1}
	v6Addr := netip.MustParseAddr("2001:db8:1234::").As16()
	tests := []struct {
		name                    string
		metadata                Metadata
		expectedInsertedDepth   int
		expectedExistingNetwork netip.Prefix
	}{
		{
			name:     "zero value",
			metadata: Metadata{},
		},
		{
			name: "invalid inserted network",
			metadata: Metadata{
				ExistingDepth: 24,
				ExistingAddr:  v4Addr,
				TreeDepth:     32,
			},
		},
		{
			name: "invalid tree depth",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("1.2.3.0/24"),
				ExistingDepth:   24,
				ExistingAddr:    v4Addr,
				TreeDepth:       64,
			},
		},
		{
			name: "IPv6 insertion in IPv4 tree",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("2001:db8::/32"),
				ExistingDepth:   24,
				ExistingAddr:    v4Addr,
				TreeDepth:       32,
			},
		},
		{
			name: "out of range existing depth",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("1.2.3.0/24"),
				ExistingDepth:   33,
				ExistingAddr:    v4Addr,
				TreeDepth:       32,
			},
			expectedInsertedDepth: 24,
		},
		{
			name: "negative existing depth",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("1.2.3.0/24"),
				ExistingDepth:   -1,
				ExistingAddr:    v4Addr,
				TreeDepth:       32,
			},
			expectedInsertedDepth: 24,
		},
		{
			name: "IPv4 tree",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("1.2.3.0/25"),
				ExistingDepth:   24,
				ExistingAddr:    v4Addr,
				TreeDepth:       32,
			},
			expectedInsertedDepth:   25,
			expectedExistingNetwork: netip.MustParsePrefix("1.2.3.0/24"),
		},
		{
			name: "IPv4 insertion in IPv6 tree",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("1.0.0.0/16"),
				ExistingDepth:   104,
				ExistingAddr:    v4InV6Addr,
				TreeDepth:       128,
			},
			expectedInsertedDepth:   112,
			expectedExistingNetwork: netip.MustParsePrefix("1.0.0.0/8"),
		},
		{
			name: "IPv4 insertion above IPv4 subtree",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("1.0.0.0/16"),
				ExistingDepth:   1,
				TreeDepth:       128,
			},
			expectedInsertedDepth:   112,
			expectedExistingNetwork: netip.MustParsePrefix("::/1"),
		},
		{
			name: "IPv6 tree",
			metadata: Metadata{
				InsertedNetwork: netip.MustParsePrefix("2001:db8::/48"),
				ExistingDepth:   32,
				ExistingAddr:    v6Addr,
				TreeDepth:       128,
			},
			expectedInsertedDepth:   48,
			expectedExistingNetwork: netip.MustParsePrefix("2001:db8::/32"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedInsertedDepth, test.metadata.InsertedDepth())
			assert.Equal(t, test.expectedExistingNetwork, test.metadata.ExistingNetwork())
		})
	}
}
