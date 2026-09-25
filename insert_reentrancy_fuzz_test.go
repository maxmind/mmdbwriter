package mmdbwriter

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// Nearby, overlapping, and disjoint attempts all have the same oracle: a
// rejected mutation has no effect, whether the outer callback propagates or
// handles its error. Later ordinary inserts exercise slot reuse after rejection.
func FuzzRejectReentrantMutation(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 0, 1, 1, 1, 6, 2, 2, 2, 10})
	f.Add([]byte{1, 1, 0, 2, 6, 3, 1, 0, 7, 0, 0, 1, 4})
	f.Add([]byte{2, 0, 1, 1, 10, 3, 1, 2, 3, 0, 1, 0, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 5 {
			return
		}
		if len(data) > 129 {
			data = data[:129]
		}
		ipVersion := 6
		start := netip.MustParseAddr("1.2.3.0")
		far := netip.MustParseAddr("128.0.0.0")
		switch data[0] % 3 {
		case 0:
			ipVersion = 4
		case 2:
			start = netip.MustParseAddr("2001:db8::")
			far = netip.MustParseAddr("a001:db8::")
		}
		tree := newReentrancyTree(t, ipVersion)
		control := newReentrancyTree(t, ipVersion)
		addresses := []netip.Addr{start, start.Next(), start.Next().Next(), far, far.Next()}
		bits := start.BitLen()
		for i, addr := range addresses {
			for _, target := range []*Tree{tree, control} {
				require.NoError(
					t,
					target.Insert(netip.PrefixFrom(addr, bits), mmdbtype.Uint32(i+1)),
				)
			}
		}
		for offset := 1; offset+3 < len(data); offset += 4 {
			outerAddr := addresses[int(data[offset])%len(addresses)]
			outer := netip.PrefixFrom(outerAddr, bits-int(data[offset+1]%4)).Masked()
			innerAddr := addresses[int(data[offset+2])%len(addresses)]
			inner := netip.PrefixFrom(innerAddr, bits-int(data[offset+3]%9)).Masked()
			operation := data[offset+3]
			handle := data[offset+1]&4 != 0
			value := mmdbtype.Uint32(data[offset] % 4)
			err := tree.InsertFunc(outer, nil,
				func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
					nestedErr := rejectReentrantMutation(t, tree, inner, operation)
					if handle {
						return value, nil
					}
					return nil, nestedErr
				})
			switch {
			case handle:
				require.NoError(t, err)
				require.NoError(t, control.Insert(outer, value))
			case operation%11 == 10:
				require.ErrorIs(t, err, errWriteDuringInsert)
			default:
				require.ErrorIs(t, err, errNestedInsert)
			}
			require.False(t, tree.inserting)
			require.NoError(t, tree.auditValueStore())
			for _, addr := range addresses {
				expectedPrefix, expectedValue := control.Get(addr)
				actualPrefix, actualValue := tree.Get(addr)
				require.Equal(t, expectedPrefix, actualPrefix)
				require.Equal(t, expectedValue, actualValue)
			}
			for _, target := range []*Tree{tree, control} {
				require.NoError(t, target.Insert(netip.PrefixFrom(innerAddr, bits), value))
			}
		}
		requireReentrancyTreesEqual(t, tree, control, start, far)
	})
}
