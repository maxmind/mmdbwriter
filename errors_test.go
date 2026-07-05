package mmdbwriter

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefixFromInsertIPLayouts(t *testing.T) {
	ip := netip.MustParseAddr("2001:db8::1234").As16()
	prefix, err := prefixFromInsertIP(ip, 48, 128)
	require.NoError(t, err)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::/48"), prefix)

	var ipv4SubtreeIP [16]byte
	copy(ipv4SubtreeIP[12:], netip.MustParseAddr("1.2.3.4").AsSlice())
	prefix, err = prefixFromInsertIP(ipv4SubtreeIP, 96, 128)
	require.NoError(t, err)
	assert.Equal(t, netip.MustParsePrefix("0.0.0.0/0"), prefix)
}

func TestAliasedNetworkErrorFormatsIPv4SubtreePrefixes(t *testing.T) {
	var ip [16]byte
	copy(ip[12:], netip.MustParseAddr("1.2.3.4").AsSlice())

	err := newAliasedNetworkError(ip, 104, 120, 128)

	var aliasedErr *AliasedNetworkError
	require.ErrorAs(t, err, &aliasedErr)
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), aliasedErr.InsertedNetwork)
	assert.Equal(t, netip.MustParsePrefix("1.0.0.0/8"), aliasedErr.AliasedNetwork)
	assert.EqualError(
		t,
		err,
		"attempt to insert 1.2.3.0/24 into 1.0.0.0/8, which is an aliased network",
	)
}
