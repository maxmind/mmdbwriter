package mmdbwriter

import (
	"bytes"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// TestRecordEncodingHighBits writes databases whose record values need the
// high bits of each record size: above 16 bits for 24-bit records, and
// above 24 bits for 28-bit and 32-bit records, which puts data in the
// shared nibble of a 28-bit node. Large values in the first networks push
// the offsets of the later values up, in both the left and the right record
// of a node. Each large value stays below the 2 MiB per-record payload limit
// of maxminddb-golang.
func TestRecordEncodingHighBits(t *testing.T) {
	tests := []struct {
		recordSize int
		// count large values of size bytes each must make the later record
		// values exceed 1<<(recordSize-8).
		count int
		size  int
	}{
		{recordSize: 24, count: 1, size: 1 << 17},
		{recordSize: 28, count: 17, size: 1 << 20},
		{recordSize: 32, count: 17, size: 1 << 20},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d-bit", test.recordSize), func(t *testing.T) {
			tree, err := New(Options{
				BuildEpoch:              1,
				DatabaseType:            "record-encoding-test",
				Description:             map[string]string{"en": "Record encoding test"},
				IPVersion:               4,
				IncludeReservedNetworks: true,
				RecordSize:              test.recordSize,
			})
			require.NoError(t, err)
			values := map[string]mmdbtype.DataType{
				"128.0.0.0/2": mmdbtype.Uint32(1),
				"192.0.0.0/2": mmdbtype.Uint32(2),
			}
			large := map[string]bool{}
			for i := range test.count {
				network := fmt.Sprintf("%d.0.0.0/8", i)
				large[network] = true
				values[network] = mmdbtype.String(strings.Repeat(string(rune('a'+i)), test.size))
			}
			for network, value := range values {
				require.NoError(t, tree.Insert(netip.MustParsePrefix(network), value))
			}
			var buf bytes.Buffer
			_, err = tree.WriteTo(&buf)
			require.NoError(t, err)

			reader, err := maxminddb.OpenBytes(buf.Bytes())
			require.NoError(t, err)
			defer reader.Close()
			require.NoError(t, reader.Verify())
			require.Equal(t, uint(test.recordSize), reader.Metadata.RecordSize)

			nodeCount := int(reader.Metadata.NodeCount)
			for network, want := range values {
				res := reader.Lookup(netip.MustParsePrefix(network).Addr())
				require.NoError(t, res.Err())
				require.Equal(t, network, res.Prefix().String())
				if !large[network] {
					// The record value is nodeCount + 16 + offset.
					require.Greater(t, nodeCount+16+int(res.Offset()), 1<<(test.recordSize-8))
				}
				var u mmdbtype.Unmarshaler
				require.NoError(t, res.Decode(&u))
				require.True(t, want.Equal(u.Result()))
			}
		})
	}
}
