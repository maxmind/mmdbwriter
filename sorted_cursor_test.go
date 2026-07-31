package mmdbwriter

import (
	"bytes"
	"net/netip"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestSortedInsertCursorResumesBelowRoot(t *testing.T) {
	tree, err := New(Options{
		BuildEpoch:              1,
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(t, err)
	for index := range 4 {
		address := netip.AddrFrom4([4]byte{1, 2, 3, byte(index)})
		require.NoError(t, tree.Insert(
			netip.PrefixFrom(address, 32),
			mmdbtype.Uint32(index+1),
		))
	}
	assert.Greater(t, tree.cursor.lastResumeDepth, 0)
}

func FuzzSortedInsertCursorMatchesRootWalk(f *testing.F) {
	f.Add([]byte{9, 1, 7, 3, 2})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Fuzz(func(t *testing.T, octets []byte) {
		if len(octets) > 32 {
			octets = octets[:32]
		}
		unique := map[byte]struct{}{}
		for _, octet := range octets {
			unique[octet] = struct{}{}
		}
		ordered := make([]byte, 0, len(unique))
		for octet := range unique {
			ordered = append(ordered, octet)
		}
		slices.Sort(ordered)

		opts := Options{
			BuildEpoch:              1,
			DatabaseType:            "cursor-fuzz",
			IPVersion:               4,
			IncludeReservedNetworks: true,
		}
		withCursor, err := New(opts)
		require.NoError(t, err)
		rootWalk, err := New(opts)
		require.NoError(t, err)
		rootWalk.disableInsertCursor = true

		for _, octet := range ordered {
			address := netip.AddrFrom4([4]byte{octet, octet ^ 0x55, octet, 1})
			prefix := netip.PrefixFrom(address, 8+int(octet%25)).Masked()
			value := mmdbtype.Map{
				"octet": mmdbtype.Uint16(octet),
				"group": mmdbtype.Uint16(octet % 3),
			}
			require.NoError(t, withCursor.Insert(prefix, value))
			require.NoError(t, rootWalk.Insert(prefix, value.Copy()))
		}

		var cursorBytes, rootBytes bytes.Buffer
		_, err = withCursor.WriteTo(&cursorBytes)
		require.NoError(t, err)
		_, err = rootWalk.WriteTo(&rootBytes)
		require.NoError(t, err)
		assert.Equal(t, rootBytes.Bytes(), cursorBytes.Bytes())
	})
}
