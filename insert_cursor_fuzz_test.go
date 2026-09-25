package mmdbwriter

import (
	"bytes"
	"errors"
	"net/netip"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// Each operation carries an address, prefix length, action, value, and range
// length. A seed/header can keep addresses close to exercise deep divergence;
// other inputs use all address bits, including mapped and aliased IPv4 space.
const cursorFuzzOperationSize = 20

type cursorOperation struct {
	prefix netip.Prefix
	action byte
	value  byte
	span   byte
}

var errCursorCallback = errors.New("cursor test callback failed")

func FuzzInsertCursorMatchesRootWalk(f *testing.F) {
	for _, family := range []byte{0, 1, 2, 3} {
		data := make([]byte, 4+16*cursorFuzzOperationSize)
		data[0] = family
		data[1] = 1 // Strictly ascending starts, including neighboring /128s.
		data[2] = 2 // 32-bit records.
		data[3] = 1 // Include reserved space.
		for i := range 16 {
			op := data[4+i*cursorFuzzOperationSize:]
			op[0] = 0x20
			op[1] = 1
			op[2] = 0xd
			op[3] = 0xb8
			op[15] = byte(i)
			op[16] = 128
			if family == 0 {
				op[16] = 32
			}
			op[17] = 0
			op[18] = 1 // Equal siblings force cascading coalescing.
		}
		f.Add(data)
		descending := slices.Clone(data)
		descending[1] = 3 // Sort, then reverse to visit adjacent prefixes downward.
		f.Add(descending)
		mixed := slices.Clone(data)
		mixed[1] = 0 // Preserve alternating low/high addresses in the input.
		for i := range 16 {
			op := mixed[4+i*cursorFuzzOperationSize:]
			op[15] = byte(i / 2)
			if i%2 != 0 {
				op[15] = 15 - byte(i/2)
			}
			op[18] = 1 + byte(i%3)
		}
		f.Add(mixed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4+cursorFuzzOperationSize {
			return
		}
		opts := Options{
			RecordSize:              []int{24, 28, 32}[data[2]%3],
			IncludeReservedNetworks: data[3]&1 != 0,
			DisableIPv4Aliasing:     data[3]&2 != 0,
		}
		if data[0]%4 == 0 {
			opts.IPVersion = 4
		}
		candidate, control := newCursorTrees(t, opts)
		count := min((len(data)-4)/cursorFuzzOperationSize, 64)
		operations := make([]cursorOperation, 0, count)
		for i := range count {
			raw := data[4+i*cursorFuzzOperationSize:]
			var ip [16]byte
			copy(ip[:], raw[:16])
			// Reusing the first address's upper bytes creates close neighbors while
			// leaving another mode free to diverge at any bit in the address.
			if data[1]&4 != 0 {
				copy(ip[:15], data[4:19])
			}
			addr := netip.AddrFrom16(ip)
			switch data[0] % 4 {
			case 0:
				addr = netip.AddrFrom4([4]byte{ip[12], ip[13], ip[14], ip[15]})
			case 1:
				if raw[19]&1 != 0 {
					addr = netip.AddrFrom4([4]byte{ip[12], ip[13], ip[14], ip[15]})
				}
			case 2, 3:
			}
			prefix := netip.PrefixFrom(addr, int(raw[16])%(addr.BitLen()+1)).Masked()
			operations = append(
				operations,
				cursorOperation{
					prefix: prefix,
					action: raw[17] % 8,
					value:  raw[18] % 4,
					span:   raw[19] % 16,
				},
			)
		}
		if data[1]&1 != 0 {
			slices.SortStableFunc(operations, func(a, b cursorOperation) int {
				left, err := candidate.normalizeInsertPrefix(a.prefix)
				if err != nil {
					left = a.prefix
				}
				right, err := candidate.normalizeInsertPrefix(b.prefix)
				if err != nil {
					right = b.prefix
				}
				li, _ := candidate.prefixInsertIP(left)
				ri, _ := candidate.prefixInsertIP(right)
				return bytes.Compare(li[:], ri[:])
			})
		}
		if data[1]&2 != 0 {
			slices.Reverse(operations)
		}
		for i, op := range operations {
			expectedCalls, expectedPanic, expectedErr := applyCursorOperation(control, op)
			actualCalls, actualPanic, actualErr := applyCursorOperation(candidate, op)
			require.Equal(t, expectedPanic, actualPanic)
			if actualPanic != nil {
				require.Same(t, errCursorCallback, actualPanic)
				require.False(t, candidate.insertCursor.valid)
			}
			if expectedErr == nil {
				require.NoError(t, actualErr)
			} else {
				require.EqualError(t, actualErr, expectedErr.Error())
			}
			require.Equal(t, expectedCalls, actualCalls)
			// Check boundaries before writing: output equality alone could miss a
			// merge postponed until finalization. Probe both ends and their neighbors.
			for _, prefix := range []netip.Prefix{op.prefix, operations[0].prefix} {
				start := prefix.Addr()
				end := netipx.RangeOfPrefix(prefix).To()
				for _, addr := range []netip.Addr{start, start.Prev(), start.Next(), end, end.Next()} {
					requireCursorLookup(t, candidate, control, addr)
				}
			}
			require.NoError(t, candidate.auditValueStore())
			if i%8 == 7 {
				requireCursorOutput(t, candidate, control)
			}
		}
		requireCursorOutput(t, candidate, control)
	})
}

func applyCursorOperation(
	tree *Tree,
	op cursorOperation,
) (calls []metadataCall, panicValue any, err error) {
	defer func() { panicValue = recover() }()
	var value mmdbtype.DataType = mmdbtype.Uint32(op.value)
	if op.value == 0 {
		value = nil
	}
	callback := func(old, incoming mmdbtype.DataType, metadata inserter.Metadata) (mmdbtype.DataType, error) {
		calls = append(calls, metadataCall{existing: old, metadata: metadata})
		if len(calls) == 1+int(op.value%3) {
			if op.action == 3 || op.action == 7 {
				return nil, errCursorCallback
			}
			if op.action == 4 {
				panic(errCursorCallback)
			}
		}
		return incoming, nil
	}
	end := op.prefix.Addr()
	for range op.span {
		if next := end.Next(); next.IsValid() {
			end = next
		}
	}
	switch op.action {
	case 0:
		err = tree.Insert(op.prefix, value)
	case 1:
		err = tree.InsertPureFunc(op.prefix, value, inserter.Replace)
	case 2, 3, 4:
		err = tree.InsertFunc(op.prefix, value, callback)
	case 5:
		err = tree.InsertRange(op.prefix.Addr(), end, value)
	case 6:
		err = tree.InsertRangePureFunc(op.prefix.Addr(), end, value, inserter.Replace)
	case 7:
		err = tree.InsertRangeFunc(op.prefix.Addr(), end, value, callback)
	}
	return calls, nil, err
}
