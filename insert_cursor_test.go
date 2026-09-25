package mmdbwriter

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func newCursorTrees(t *testing.T, opts Options) (candidate, control *Tree) {
	var err error
	t.Helper()
	opts.BuildEpoch = 123
	opts.DatabaseType = "insert-cursor-test"
	opts.Description = map[string]string{"en": "Insertion cursor test"}
	candidate, err = New(opts)
	require.NoError(t, err)
	control, err = New(opts)
	require.NoError(t, err)
	control.disableInsertCursor = true
	return candidate, control
}

func requireCursorLookup(t *testing.T, candidate, control *Tree, address netip.Addr) {
	t.Helper()
	expectedPrefix, expectedValue := control.Get(address)
	actualPrefix, actualValue := candidate.Get(address)
	require.Equal(t, expectedPrefix, actualPrefix, "lookup boundary for %s", address)
	require.Equal(t, expectedValue, actualValue, "lookup value for %s", address)
}

func requireCursorOutput(t *testing.T, candidate, control *Tree) {
	t.Helper()
	require.NoError(t, candidate.auditValueStore())
	require.NoError(t, control.auditValueStore())
	expected := writeTreeBytes(t, control)
	actual := writeTreeBytes(t, candidate)
	require.Equal(t, expected, actual)
	reader, err := maxminddb.OpenBytes(actual)
	require.NoError(t, err)
	defer reader.Close()
	require.NoError(t, reader.Verify())
}

func TestInsertCursorMergesAndReusesSlots(t *testing.T) {
	for _, ipVersion := range []int{4, 6} {
		for _, recordSize := range []int{24, 28, 32} {
			t.Run(strconv.Itoa(ipVersion)+"/"+strconv.Itoa(recordSize), func(t *testing.T) {
				opts := Options{
					IPVersion:               ipVersion,
					RecordSize:              recordSize,
					IncludeReservedNetworks: true,
				}
				candidate, control := newCursorTrees(t, opts)
				start := netip.MustParseAddr("1.2.3.0")
				if ipVersion == 6 {
					start = netip.MustParseAddr("2001:db8::")
				}
				// Alternate distinct values, equal siblings, and removals. Repeating the
				// cycle forces retired nodes and compressed paths to be reused.
				for cycle := range 2 {
					for pass := range 3 {
						addr := start
						for i := range 64 {
							prefix := netip.PrefixFrom(addr, addr.BitLen())
							var value mmdbtype.DataType = mmdbtype.Uint32(i % 4)
							if pass == 1 {
								value = mmdbtype.Uint32(1)
							}
							if pass == 2 {
								value = nil
							}
							require.NoError(t, candidate.Insert(prefix, value))
							require.NoError(t, control.Insert(prefix, value))
							requireCursorLookup(t, candidate, control, start)
							requireCursorLookup(t, candidate, control, addr)
							// Validate owning nodes before checking the borrowed cursor path.
							require.NoError(t, candidate.auditValueStore())
							addr = addr.Next()
						}
						// Equal /32s or /128s must have coalesced before a read or callback.
						if pass == 1 {
							prefix, _ := candidate.Get(start)
							require.Equal(t, start.BitLen()-6, prefix.Bits())
							var actual, expected []metadataCall
							target := netip.PrefixFrom(start, start.BitLen()-6)
							require.NoError(
								t,
								candidate.InsertFunc(target, nil, captureExisting(&actual)),
							)
							require.NoError(
								t,
								control.InsertFunc(target, nil, captureExisting(&expected)),
							)
							require.Equal(t, expected, actual)
							require.Len(t, actual, 1)
						}
						requireCursorOutput(t, candidate, control)
					}
					if cycle == 0 && !candidate.poisonTreeSlots {
						require.NotEmpty(t, candidate.freeNodes)
					}
				}
				// Load uses the cursor internally; then reuse its retained path on insert.
				path := writeTempDB(t, candidate)
				opts.BuildEpoch = candidate.buildEpoch
				loaded, err := Load(path, opts)
				require.NoError(t, err)
				prefix := netip.PrefixFrom(start.Next(), start.BitLen())
				require.NoError(t, loaded.Insert(prefix, mmdbtype.Uint32(4)))
				require.NoError(t, control.Insert(prefix, mmdbtype.Uint32(4)))
				requireCursorOutput(t, loaded, control)
			})
		}
	}
}

func TestInsertCursorFailureAndPanic(t *testing.T) {
	for _, panicCallback := range []bool{false, true} {
		t.Run(strconv.FormatBool(panicCallback), func(t *testing.T) {
			candidate, control := newCursorTrees(
				t,
				Options{IPVersion: 4, IncludeReservedNetworks: true},
			)
			for _, tree := range []*Tree{candidate, control} {
				for i := range 4 {
					require.NoError(
						t,
						tree.Insert(
							netip.PrefixFrom(netip.AddrFrom4([4]byte{1, 2, 3, byte(i)}), 32),
							mmdbtype.Uint32(i),
						),
					)
				}
			}
			require.True(t, candidate.insertCursor.valid)
			failure := errors.New("callback failure")
			for _, tree := range []*Tree{candidate, control} {
				count := 0
				insert := func() error {
					return tree.InsertFunc(
						netip.MustParsePrefix("1.2.3.0/30"),
						nil,
						func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
							count++
							if count == 3 {
								if panicCallback {
									panic(failure)
								}
								return nil, failure
							}
							return mmdbtype.Uint32(1), nil
						},
					)
				}
				if panicCallback {
					require.PanicsWithValue(t, failure, func() { require.NoError(t, insert()) })
				} else {
					require.ErrorIs(t, insert(), failure)
				}
			}
			require.False(t, candidate.insertCursor.valid)
			requireCursorOutput(t, candidate, control)
			// Retry and then resume ascending inserts after the invalidated path.
			for _, p := range []string{"1.2.3.0/30", "1.2.3.4/32", "1.2.3.5/32"} {
				prefix := netip.MustParsePrefix(p)
				require.NoError(t, candidate.Insert(prefix, mmdbtype.Uint32(2)))
				require.NoError(t, control.Insert(prefix, mmdbtype.Uint32(2)))
				requireCursorLookup(t, candidate, control, prefix.Addr())
			}
			requireCursorOutput(t, candidate, control)
		})
	}
}

func TestInsertCursorPartialFailureCoalesces(t *testing.T) {
	for _, family := range []struct {
		name      string
		ipVersion int
		start     string
	}{
		{name: "IPv4", ipVersion: 4, start: "1.2.3.0"},
		{name: "IPv4 in IPv6", ipVersion: 6, start: "1.2.3.0"},
		{name: "IPv6", ipVersion: 6, start: "2001:db8::"},
	} {
		t.Run(family.name, func(t *testing.T) {
			candidate, control := newCursorTrees(t, Options{
				IPVersion:               family.ipVersion,
				IncludeReservedNetworks: true,
			})
			start := netip.MustParseAddr(family.start)
			target := netip.PrefixFrom(start, start.BitLen()-1)
			insertErr := errors.New("failed after installing equal sibling")
			for _, tree := range []*Tree{candidate, control} {
				require.NoError(
					t,
					tree.Insert(netip.PrefixFrom(start, start.BitLen()), mmdbtype.Uint32(1)),
				)
				require.NoError(
					t,
					tree.Insert(netip.PrefixFrom(start.Next(), start.BitLen()), mmdbtype.Uint32(2)),
				)
				calls := 0
				err := tree.InsertFunc(
					target,
					nil,
					func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
						calls++
						if calls == 1 {
							return mmdbtype.Uint32(2), nil
						}
						return nil, insertErr
					},
				)
				require.ErrorIs(t, err, insertErr)
				require.Equal(t, 2, calls)
				for _, addr := range []netip.Addr{start, start.Next()} {
					prefix, value := tree.Get(addr)
					require.Equal(t, target, prefix)
					require.Equal(t, mmdbtype.Uint32(2), value)
				}
				require.NoError(t, tree.auditValueStore())
				var retryCalls []metadataCall
				require.NoError(t, tree.InsertFunc(target, nil, captureExisting(&retryCalls)))
				require.Len(t, retryCalls, 1)
				require.Equal(t, target, retryCalls[0].metadata.ExistingNetwork())
			}
			requireCursorOutput(t, candidate, control)
		})
	}
}

func TestAuditInsertCursorRejectsStalePath(t *testing.T) {
	t.Setenv("MMDBWRITER_REFCOUNT_AUDIT", "")
	for _, audit := range []bool{false, true} {
		t.Run(fmt.Sprintf("audit=%t", audit), func(t *testing.T) {
			candidate, _ := newCursorTrees(t, Options{
				IPVersion:               4,
				IncludeReservedNetworks: true,
				RefcountAudit:           audit,
			})
			for _, p := range []string{"1.2.3.0/32", "1.2.3.1/32", "1.2.3.2/32"} {
				require.NoError(t, candidate.Insert(netip.MustParsePrefix(p), mmdbtype.String(p)))
			}
			require.True(t, candidate.insertCursor.valid)
			require.Greater(t, candidate.insertCursor.length, 2)
			checkFailure := func(want string) {
				t.Helper()
				// Direct audits must check cursor structure even with slot reuse enabled.
				directErr := candidate.auditValueStore()
				require.ErrorContains(t, directErr, "insertion cursor audit found "+want)
				err := candidate.maybeAuditValueStore()
				if audit {
					var auditErr *RefcountAuditError
					require.ErrorAs(t, err, &auditErr)
					require.EqualError(t, errors.Unwrap(auditErr), directErr.Error())
				} else {
					require.NoError(t, err)
				}
			}
			saved := candidate.insertCursor
			candidate.insertCursor.length = 129
			checkFailure("invalid length 129 (expected 1..32)")
			candidate.insertCursor = saved
			candidate.insertCursor.nodes[0] = noNodeIndex
			checkFailure(fmt.Sprintf("invalid root %d (expected %d)", noNodeIndex, candidate.root))
			candidate.insertCursor = saved
			// A reachable node at the wrong depth must also be rejected.
			candidate.insertCursor.nodes[1] = candidate.root
			checkFailure("stale path at depth 1")
			candidate.insertCursor = saved
			require.NoError(t, candidate.auditValueStore())
		})
	}
}

func TestInsertCursorMixedPrefixes(t *testing.T) {
	// Include equal starts, containing prefixes, descending input, address-family
	// normalization, and insertions into or across reserved and aliased space.
	prefixes := []string{
		"1.2.3.0/32", "1.2.3.1/32", "1.2.3.2/32", "1.2.3.0/24",
		"1.2.3.0/25", "1.2.2.0/24", "::ffff:1.2.3.4/128", "::ffff:1.2.3.0/120",
		"::ffff:0:0/95", "::/0", "::/96", "1.2.3.5/32", "2001:4860::/128",
		"2001:4860::1/128", "2001:4860::/64", "2002::/16", "2002:102:304::/48",
		"64:ff9b::102:304/128", "10.0.0.0/8", "9.0.0.0/7", "0.0.0.0/0",
	}
	for _, ipVersion := range []int{4, 6} {
		for _, disableAliases := range []bool{false, true} {
			for _, includeReserved := range []bool{false, true} {
				name := fmt.Sprintf(
					"%d/aliases-disabled=%t/reserved=%t",
					ipVersion,
					disableAliases,
					includeReserved,
				)
				t.Run(name, func(t *testing.T) {
					candidate, control := newCursorTrees(
						t,
						Options{
							IPVersion:               ipVersion,
							DisableIPv4Aliasing:     disableAliases,
							IncludeReservedNetworks: includeReserved,
						},
					)
					for i, text := range prefixes {
						op := cursorOperation{
							prefix: netip.MustParsePrefix(text),
							action: 2,
							value:  byte(i % 4),
						}
						expectedCalls, expectedPanic, expectedErr := applyCursorOperation(
							control,
							op,
						)
						actualCalls, actualPanic, actualErr := applyCursorOperation(candidate, op)
						require.Nil(t, expectedPanic)
						require.Nil(t, actualPanic)
						if expectedErr == nil {
							require.NoError(t, actualErr)
						} else {
							require.EqualError(t, actualErr, expectedErr.Error())
						}
						require.Equal(t, expectedCalls, actualCalls)
						requireCursorLookup(t, candidate, control, op.prefix.Addr())
						requireCursorOutput(t, candidate, control)
					}
				})
			}
		}
	}
}

func TestInsertCursorDirectionChanges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		start     string
		ipVersion int
	}{
		{"ipv4", "1.2.3.0", 4},
		{"ipv4-in-ipv6", "1.2.3.0", 6},
		{"ipv6", "2001:db8::", 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate, control := newCursorTrees(t, Options{
				IPVersion: tc.ipVersion, IncludeReservedNetworks: true,
			})
			addresses := make([]netip.Addr, 8)
			addresses[0] = netip.MustParseAddr(tc.start)
			for i := 1; i < len(addresses); i++ {
				addresses[i] = addresses[i-1].Next()
			}
			bits := addresses[0].BitLen()
			type step struct {
				offset int
				bits   int
				value  byte
			}
			var steps []step
			for i := range slices.Backward(addresses) {
				steps = append(steps, step{i, bits, []byte{1, 2, 3}[i%3]})
			}
			// Revisit equal starts, replace containing prefixes, remove and
			// rebuild subtrees, then change direction while merging siblings.
			steps = append(steps,
				step{0, bits, 1}, step{0, bits, 2}, step{0, bits - 3, 1},
				step{4, bits - 2, 0}, step{2, bits - 1, 2}, step{0, bits - 1, 2},
				step{0, bits - 3, 0}, step{7, bits, 3}, step{0, bits, 1},
				step{6, bits, 3}, step{1, bits, 1}, step{4, bits - 2, 3},
				step{0, bits - 2, 3}, step{0, bits, 1}, step{7, bits, 2},
			)
			for _, s := range steps {
				op := cursorOperation{
					prefix: netip.PrefixFrom(addresses[s.offset], s.bits),
					action: 2,
					value:  s.value,
				}
				expectedCalls, expectedPanic, expectedErr := applyCursorOperation(control, op)
				actualCalls, actualPanic, actualErr := applyCursorOperation(candidate, op)
				require.NoError(t, expectedErr)
				require.NoError(t, actualErr)
				require.Nil(t, expectedPanic)
				require.Nil(t, actualPanic)
				require.Equal(t, expectedCalls, actualCalls)
				for _, addr := range addresses {
					requireCursorLookup(t, candidate, control, addr)
				}
				require.NoError(t, candidate.auditValueStore())
				require.NoError(t, control.auditValueStore())
			}
			requireCursorOutput(t, candidate, control)
		})
	}
}
