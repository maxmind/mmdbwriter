package mmdbwriter

import (
	"bytes"
	"errors"
	"net/netip"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type metadataCall struct {
	existing mmdbtype.DataType
	metadata inserter.Metadata
}

func TestInserterMetadataRecordShapes(t *testing.T) {
	t.Run("wider existing record", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		recordValue := mmdbtype.String("existing")
		require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/24"), recordValue))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/25"),
			mmdbtype.String("new"),
			captureMetadata(&calls),
		))

		require.Len(t, calls, 1)
		assert.Equal(t, recordValue, calls[0].existing)
		assertMetadata(t, calls[0].metadata, "1.2.3.0/25", "1.2.3.0/24", 24, 32)
		assert.Equal(t, [16]byte{1, 2, 3}, calls[0].metadata.ExistingAddr)
	})

	t.Run("narrower existing records", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		existing := []netip.Prefix{
			netip.MustParsePrefix("1.2.3.0/26"),
			netip.MustParsePrefix("1.2.3.64/26"),
			netip.MustParsePrefix("1.2.3.128/25"),
		}
		for index, network := range existing {
			require.NoError(t, tree.Insert(network, mmdbtype.Uint32(index+1)))
		}

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/24"),
			mmdbtype.String("overlay"),
			captureMetadata(&calls),
		))

		// Each record reports its own network, so a callback can tell the
		// covered records apart even though all of them are more specific
		// than the insert.
		require.Len(t, calls, len(existing))
		for index, network := range existing {
			metadata := calls[index].metadata
			assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), metadata.InsertedNetwork)
			assert.Equal(t, network.Bits(), metadata.ExistingDepth)
			assert.Equal(t, 32, metadata.TreeDepth)
			assert.Equal(t, network, metadata.ExistingNetwork())
			assert.Equal(t, treeAddr(network, 32), metadata.ExistingAddr)
		}
	})

	t.Run("non-aligned existing depth", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix("1.2.3.128/25"),
			mmdbtype.String("existing"),
		))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.224/27"),
			mmdbtype.String("overlay"),
			captureMetadata(&calls),
		))

		// The raw address at a depth that ends mid-byte, asserted without
		// going through ExistingNetwork, whose Prefix call would re-mask and
		// hide a masking error.
		require.Len(t, calls, 1)
		assert.Equal(t, 25, calls[0].metadata.ExistingDepth)
		assert.Equal(t, [16]byte{1, 2, 3, 128}, calls[0].metadata.ExistingAddr)
	})

	t.Run("sibling records under one insert", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix("1.1.1.1/32"),
			mmdbtype.String("existing"),
		))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.1.1.0/31"),
			mmdbtype.String("overlay"),
			captureMetadata(&calls),
		))

		// Both /32s report the same depth, so only the network distinguishes
		// the empty sibling from the record that holds the existing value.
		require.Len(t, calls, 2)
		assert.Nil(t, calls[0].existing)
		assert.Equal(t, netip.MustParsePrefix("1.1.1.0/32"), calls[0].metadata.ExistingNetwork())
		assert.Equal(t, mmdbtype.String("existing"), calls[1].existing)
		assert.Equal(t, netip.MustParsePrefix("1.1.1.1/32"), calls[1].metadata.ExistingNetwork())
		for _, call := range calls {
			assert.Equal(t, 32, call.metadata.ExistingDepth)
			assert.Equal(t, 31, call.metadata.InsertedDepth())
		}
	})

	t.Run("exact existing record", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		prefix := netip.MustParsePrefix("1.2.3.0/24")
		require.NoError(t, tree.Insert(prefix, mmdbtype.String("existing")))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(prefix, mmdbtype.String("new"), captureMetadata(&calls)))

		require.Len(t, calls, 1)
		assertMetadata(t, calls[0].metadata, prefix.String(), prefix.String(), 24, 32)
	})
}

func TestInserterMetadataCurrentTreeShape(t *testing.T) {
	t.Run("wire equal result remerges", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		base := mmdbtype.Map{"name": mmdbtype.String("same")}
		require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/24"), base))
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/25"),
			base.Copy(),
			func(_, newValue mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
				return newValue, nil
			},
		))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/24"),
			mmdbtype.String("overlay"),
			captureMetadata(&calls),
		))
		require.Len(t, calls, 1)
		assert.Equal(t, 24, calls[0].metadata.ExistingDepth)
	})

	t.Run("earlier insert fragments record", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix("1.2.3.0/24"),
			mmdbtype.String("base"),
		))
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix("1.2.3.0/25"),
			mmdbtype.String("left"),
		))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/24"),
			mmdbtype.String("overlay"),
			captureMetadata(&calls),
		))
		require.Len(t, calls, 2)
		assert.Equal(t, 25, calls[0].metadata.ExistingDepth)
		assert.Equal(t, 25, calls[1].metadata.ExistingDepth)
	})

	t.Run("heterogeneous history coalesces", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		base := mmdbtype.String("base")
		require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.0.0/16"), base))
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix("1.2.0.0/17"),
			mmdbtype.String("temporary"),
		))
		require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.0.0/17"), base))

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.0.0/16"),
			mmdbtype.String("overlay"),
			captureMetadata(&calls),
		))
		require.Len(t, calls, 1)
		assert.Equal(t, 16, calls[0].metadata.ExistingDepth)
	})
}

func TestInserterMetadataEmptyRecords(t *testing.T) {
	t.Run("fresh tree", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/24"),
			mmdbtype.String("new"),
			captureMetadata(&calls),
		))
		require.Len(t, calls, 1)
		assert.Nil(t, calls[0].existing)
		assertMetadata(t, calls[0].metadata, "1.2.3.0/24", "0.0.0.0/1", 1, 32)
	})

	t.Run("previously split empty region", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		require.NoError(t, tree.Insert(
			netip.MustParsePrefix("1.2.2.0/24"),
			mmdbtype.String("neighbor"),
		))
		expectedNetwork, expectedValue := tree.Get(netip.MustParseAddr("1.2.3.0"))
		require.Nil(t, expectedValue)

		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(
			netip.MustParsePrefix("1.2.3.0/24"),
			mmdbtype.String("new"),
			captureMetadata(&calls),
		))
		require.Len(t, calls, 1)
		assert.Nil(t, calls[0].existing)
		assert.Equal(t, expectedNetwork, calls[0].metadata.ExistingNetwork())
		assert.Equal(t, expectedNetwork.Bits(), calls[0].metadata.ExistingDepth)
	})

	t.Run("removal", func(t *testing.T) {
		tree := newMetadataTree(t, Options{IPVersion: 4})
		prefix := netip.MustParsePrefix("1.2.3.0/24")
		require.NoError(t, tree.Insert(prefix, mmdbtype.String("existing")))
		var calls []metadataCall
		require.NoError(t, tree.InsertFunc(prefix, nil, func(
			existingValue, _ mmdbtype.DataType,
			metadata inserter.Metadata,
		) (mmdbtype.DataType, error) {
			calls = append(calls, metadataCall{existing: existingValue, metadata: metadata})
			return nil, nil
		}))
		require.Len(t, calls, 1)
		assert.Equal(t, mmdbtype.String("existing"), calls[0].existing)
		assert.Equal(t, prefix, calls[0].metadata.ExistingNetwork())
	})
}

func TestInserterMetadataRangeSubnets(t *testing.T) {
	expectedPrefixes := []netip.Prefix{
		netip.MustParsePrefix("1.2.3.1/32"),
		netip.MustParsePrefix("1.2.3.2/31"),
		netip.MustParsePrefix("1.2.3.4/31"),
		netip.MustParsePrefix("1.2.3.6/32"),
	}
	shadow := newMetadataTree(t, Options{IPVersion: 4})
	expectedExisting := make([]netip.Prefix, 0, len(expectedPrefixes))
	for _, prefix := range expectedPrefixes {
		existingNetwork, _ := shadow.Get(prefix.Addr())
		expectedExisting = append(expectedExisting, existingNetwork)
		require.NoError(t, shadow.Insert(prefix, mmdbtype.String(prefix.String())))
	}

	tree := newMetadataTree(t, Options{IPVersion: 4})
	var calls []metadataCall
	require.NoError(t, tree.InsertRangeFunc(
		netip.MustParseAddr("1.2.3.1"),
		netip.MustParseAddr("1.2.3.6"),
		mmdbtype.String("range"),
		captureMetadata(&calls),
	))

	require.Len(t, calls, len(expectedPrefixes))
	for index, prefix := range expectedPrefixes {
		metadata := calls[index].metadata
		assert.Equal(t, prefix, metadata.InsertedNetwork)
		assert.Equal(t, prefix.Bits(), metadata.InsertedDepth())
		assert.Equal(t, expectedExisting[index].Bits(), metadata.ExistingDepth)
		assert.Equal(t, expectedExisting[index], metadata.ExistingNetwork())
		assert.Equal(t, treeAddr(expectedExisting[index], 32), metadata.ExistingAddr)
		assert.Equal(t, 32, metadata.TreeDepth)
	}
}

func TestInserterMetadataIPv4InIPv6Tree(t *testing.T) {
	tree := newMetadataTree(t, Options{IPVersion: 6})
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.0.0.0/8"),
		mmdbtype.String("existing"),
	))
	var calls []metadataCall
	require.NoError(t, tree.InsertFunc(
		netip.MustParsePrefix("1.0.0.0/16"),
		mmdbtype.String("new"),
		captureMetadata(&calls),
	))

	require.Len(t, calls, 1)
	assertMetadata(t, calls[0].metadata, "1.0.0.0/16", "1.0.0.0/8", 104, 128)
	assert.Equal(t, 112, calls[0].metadata.InsertedDepth())
	assert.Equal(t, 8, calls[0].metadata.ExistingNetwork().Bits())
	assert.Equal(
		t,
		treeAddr(netip.MustParsePrefix("1.0.0.0/8"), 128),
		calls[0].metadata.ExistingAddr,
	)
	assert.Equal(t, byte(1), calls[0].metadata.ExistingAddr[12])
	assert.Equal(t, [12]byte{}, [12]byte(calls[0].metadata.ExistingAddr[:12]))
}

func TestInserterMetadataUnaliasedIPv6ShallowRecord(t *testing.T) {
	tree := newMetadataTree(t, Options{
		DisableIPv4Aliasing: true,
		IPVersion:           6,
	})
	var calls []metadataCall
	require.NoError(t, tree.InsertFunc(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("new"),
		captureMetadata(&calls),
	))

	require.Len(t, calls, 1)
	assert.Nil(t, calls[0].existing)
	assertMetadata(t, calls[0].metadata, "1.2.3.0/24", "::/1", 1, 128)
	assert.Equal(t, 120, calls[0].metadata.InsertedDepth())
}

func TestInserterMetadataFamilyFollowsInsert(t *testing.T) {
	tree := newMetadataTree(t, Options{IPVersion: 6})
	sharedRecord := netip.MustParsePrefix("::102:304/128")
	require.NoError(t, tree.Insert(sharedRecord, mmdbtype.String("shared")))

	var v6Calls []metadataCall
	require.NoError(t, tree.InsertFunc(
		sharedRecord,
		mmdbtype.String("v6"),
		captureExisting(&v6Calls),
	))
	require.Len(t, v6Calls, 1)
	assert.Equal(t, sharedRecord, v6Calls[0].metadata.ExistingNetwork())

	var v4Calls []metadataCall
	require.NoError(t, tree.InsertFunc(
		netip.MustParsePrefix("1.2.3.4/32"),
		mmdbtype.String("v4"),
		captureExisting(&v4Calls),
	))
	require.Len(t, v4Calls, 1)
	assert.Equal(t, netip.MustParsePrefix("1.2.3.4/32"), v4Calls[0].metadata.ExistingNetwork())

	v4Prefix := netip.MustParsePrefix("1.0.0.0/4")
	require.NoError(t, tree.Insert(v4Prefix, mmdbtype.String("v4-subtree")))
	var allCalls []metadataCall
	require.NoError(t, tree.InsertFunc(
		netip.MustParsePrefix("::/0"),
		mmdbtype.String("overlay"),
		captureExisting(&allCalls),
	))
	index := slices.IndexFunc(allCalls, func(call metadataCall) bool {
		return call.existing == mmdbtype.String("v4-subtree")
	})
	require.NotEqual(t, -1, index)
	assert.Equal(t, 100, allCalls[index].metadata.ExistingDepth)
	// The record is the IPv4 subtree's 0.0.0.0/4, reported in IPv6 form
	// because the insert is IPv6.
	assert.Equal(t, netip.MustParsePrefix("::/100"), allCalls[index].metadata.ExistingNetwork())
}

func TestInserterMetadataRootInsert(t *testing.T) {
	tree := newMetadataTree(t, Options{
		DisableIPv4Aliasing: true,
		IPVersion:           6,
	})
	var calls []metadataCall
	require.NoError(t, tree.InsertFunc(
		netip.MustParsePrefix("::/0"),
		mmdbtype.String("root"),
		captureMetadata(&calls),
	))
	// The insert covers both root children, which the callback tells apart by
	// the branch the walk took to reach each one.
	require.Len(t, calls, 2)
	expected := []netip.Prefix{
		netip.MustParsePrefix("::/1"),
		netip.MustParsePrefix("8000::/1"),
	}
	for index, call := range calls {
		assert.Equal(t, 1, call.metadata.ExistingDepth)
		assert.Equal(t, expected[index], call.metadata.ExistingNetwork())
		assert.Equal(t, treeAddr(expected[index], 128), call.metadata.ExistingAddr)
	}
}

func TestInserterMetadataNormalizesInsertedNetwork(t *testing.T) {
	tests := []struct {
		name     string
		prefix   netip.Prefix
		expected netip.Prefix
	}{
		{
			name:     "unmasked",
			prefix:   netip.PrefixFrom(netip.MustParseAddr("1.2.3.4"), 24),
			expected: netip.MustParsePrefix("1.2.3.0/24"),
		},
		{
			name:     "IPv4 mapped",
			prefix:   netip.MustParsePrefix("::ffff:1.2.3.4/120"),
			expected: netip.MustParsePrefix("1.2.3.0/24"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := newMetadataTree(t, Options{IPVersion: 4})
			var calls []metadataCall
			require.NoError(t, tree.InsertFunc(
				test.prefix,
				mmdbtype.String("value"),
				captureMetadata(&calls),
			))
			require.NotEmpty(t, calls)
			for _, call := range calls {
				assert.Equal(t, test.expected, call.metadata.InsertedNetwork)
			}
		})
	}
}

func TestOptionsInserterMemoizesRepeatedValues(t *testing.T) {
	calls := map[mmdbtype.String]int{}
	tree := newMetadataTree(t, Options{
		IPVersion: 4,
		Inserter: func(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
			if existingValue != nil {
				calls[existingValue.(mmdbtype.String)]++
			}
			return newValue, nil
		},
	})
	for index, value := range []mmdbtype.String{"a", "b", "a", "b"} {
		//nolint:gosec // index is bounded by the four-element literal
		prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{1, 2, 3, byte(index * 64)}), 26)
		require.NoError(t, tree.Insert(prefix, value))
	}
	clear(calls)

	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("new"),
	))
	assert.Equal(t, map[mmdbtype.String]int{"a": 1, "b": 1}, calls)
}

func TestOptionsInserterMemoizesAcrossRangeSubnets(t *testing.T) {
	calls := 0
	tree := newMetadataTree(t, Options{
		IPVersion: 4,
		Inserter: func(_, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
			calls++
			return newValue, nil
		},
	})
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("existing"),
	))
	calls = 0
	require.NoError(t, tree.InsertRange(
		netip.MustParseAddr("1.2.3.1"),
		netip.MustParseAddr("1.2.3.254"),
		mmdbtype.String("new"),
	))
	assert.Equal(t, 1, calls, "the configured pure result was not shared across range subnets")
}

func TestFailedInsertRestoresDeepSplitChain(t *testing.T) {
	tree := newMetadataTree(t, Options{IPVersion: 4})
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.0.0.0/8"),
		mmdbtype.String("base"),
	))
	before := writeTreeBytes(t, tree)

	// A /32 inside a /8 record splits 24 levels, so the unwinding merge has to
	// fire at every frame to put the record back.
	insertErr := errors.New("insert failed")
	err := tree.InsertFunc(
		netip.MustParsePrefix("1.2.3.4/32"),
		mmdbtype.String("overlay"),
		func(_, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
			return nil, insertErr
		},
	)
	require.ErrorIs(t, err, insertErr)
	require.NoError(t, tree.auditValueStore())
	assert.Equal(t, before, writeTreeBytes(t, tree),
		"the failed insert changed the database")
}

func TestFailedRangeInsertKeepsEarlierSubnets(t *testing.T) {
	tree := newMetadataTree(t, Options{IPVersion: 4})
	require.NoError(t, tree.Insert(
		netip.MustParsePrefix("1.2.3.0/24"),
		mmdbtype.String("base"),
	))

	// The range decomposes into four subnets, so failing on the third leaves
	// two installed and requires splitDepth to be reset for the failing one.
	insertErr := errors.New("insert failed mid-range")
	var networks []netip.Prefix
	var depths []int
	err := tree.InsertRangeFunc(
		netip.MustParseAddr("1.2.3.1"),
		netip.MustParseAddr("1.2.3.6"),
		mmdbtype.String("overlay"),
		func(_, newValue mmdbtype.DataType, metadata inserter.Metadata) (
			mmdbtype.DataType,
			error,
		) {
			networks = append(networks, metadata.InsertedNetwork)
			depths = append(depths, metadata.ExistingDepth)
			if len(networks) == 3 {
				return nil, insertErr
			}
			return newValue, nil
		},
	)
	require.ErrorIs(t, err, insertErr)
	assert.Equal(t, []netip.Prefix{
		netip.MustParsePrefix("1.2.3.1/32"),
		netip.MustParsePrefix("1.2.3.2/31"),
		netip.MustParsePrefix("1.2.3.4/31"),
	}, networks)
	// The depths track the fragmentation each subnet leaves behind: the first
	// splits the /24, the second lands on a /31 the split produced, and the
	// third resolves through a split chain from a /30, one level shallower
	// than the subnet being inserted.
	assert.Equal(t, []int{24, 31, 30}, depths)

	for address, expected := range map[string]mmdbtype.DataType{
		"1.2.3.0": mmdbtype.String("base"),
		"1.2.3.1": mmdbtype.String("overlay"),
		"1.2.3.2": mmdbtype.String("overlay"),
		"1.2.3.3": mmdbtype.String("overlay"),
		"1.2.3.4": mmdbtype.String("base"),
		"1.2.3.6": mmdbtype.String("base"),
	} {
		_, value := tree.Get(netip.MustParseAddr(address))
		assert.Equal(t, expected, value, "wrong value for %s", address)
	}
	require.NoError(t, tree.auditValueStore())
}

func TestFailedInserterRetryMetadata(t *testing.T) {
	insertMethods := []struct {
		name   string
		insert func(*Tree, inserter.Func) error
	}{
		{
			name: "prefix",
			insert: func(tree *Tree, insertFunc inserter.Func) error {
				return tree.InsertFunc(
					netip.MustParsePrefix("1.2.3.0/24"),
					mmdbtype.String("overlay"),
					insertFunc,
				)
			},
		},
		{
			name: "range",
			insert: func(tree *Tree, insertFunc inserter.Func) error {
				return tree.InsertRangeFunc(
					netip.MustParseAddr("1.2.3.0"),
					netip.MustParseAddr("1.2.3.255"),
					mmdbtype.String("overlay"),
					insertFunc,
				)
			},
		},
	}

	for _, method := range insertMethods {
		t.Run(method.name+" no result installed", func(t *testing.T) {
			tree := newMetadataTree(t, Options{IPVersion: 4})
			require.NoError(t, tree.Insert(
				netip.MustParsePrefix("1.2.3.0/23"),
				mmdbtype.String("base"),
			))
			insertErr := errors.New("insert failed")
			var failedMetadata inserter.Metadata
			err := method.insert(tree, func(
				_, _ mmdbtype.DataType,
				metadata inserter.Metadata,
			) (mmdbtype.DataType, error) {
				failedMetadata = metadata
				return nil, insertErr
			})
			require.ErrorIs(t, err, insertErr)

			var retryCalls []metadataCall
			require.NoError(t, method.insert(tree, captureExisting(&retryCalls)))
			require.Len(t, retryCalls, 1)
			assert.Equal(t, failedMetadata.ExistingDepth, retryCalls[0].metadata.ExistingDepth)
			assert.Equal(t, failedMetadata.ExistingAddr, retryCalls[0].metadata.ExistingAddr)
			assert.Equal(
				t,
				failedMetadata.ExistingNetwork(),
				retryCalls[0].metadata.ExistingNetwork(),
			)
			assert.Equal(t, mmdbtype.String("base"), retryCalls[0].existing)
		})

		t.Run(method.name+" partial success coalesces", func(t *testing.T) {
			tree := newMetadataTree(t, Options{IPVersion: 4})
			require.NoError(t, tree.Insert(
				netip.MustParsePrefix("1.2.3.0/25"),
				mmdbtype.String("left"),
			))
			require.NoError(t, tree.Insert(
				netip.MustParsePrefix("1.2.3.128/25"),
				mmdbtype.String("right"),
			))

			insertErr := errors.New("insert failed after success")
			calls := 0
			err := method.insert(tree, func(
				_, _ mmdbtype.DataType,
				_ inserter.Metadata,
			) (mmdbtype.DataType, error) {
				calls++
				if calls == 1 {
					return mmdbtype.String("right"), nil
				}
				return nil, insertErr
			})
			require.ErrorIs(t, err, insertErr)
			assert.Equal(t, 2, calls)

			var retryCalls []metadataCall
			require.NoError(t, method.insert(tree, captureExisting(&retryCalls)))
			require.Len(t, retryCalls, 1)
			assert.Equal(t, 24, retryCalls[0].metadata.ExistingDepth)
			assert.Equal(t, mmdbtype.String("right"), retryCalls[0].existing)
		})
	}
}

func TestLoadUsesPureOptionsInserter(t *testing.T) {
	prefixes := []netip.Prefix{
		netip.MustParsePrefix("1.2.3.0/25"),
		netip.MustParsePrefix("1.2.3.128/25"),
	}
	value := provenanceValue("equal", 25)
	sourcePath := writeAdjacentEqualSource(t, prefixes[0], value)

	calls := 0
	loaded, err := Load(sourcePath, Options{
		BuildEpoch:              1,
		IPVersion:               4,
		IncludeReservedNetworks: true,
		Inserter: func(_, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
			calls++
			return newValue, nil
		},
	})
	require.NoError(t, err)
	assert.Equal(t, len(prefixes), calls)

	physicalPrefix, got := loaded.Get(netip.MustParseAddr("1.2.3.4"))
	assert.Equal(t, netip.MustParsePrefix("1.2.3.0/24"), physicalPrefix)
	assert.Equal(t, value, got)
}

func TestMetadataLoadThenOverlayMatchesProvenanceWorkflow(t *testing.T) {
	source := newMetadataTree(t, Options{IPVersion: 4})
	base := []benchmarkInsertSpec{
		{
			network: netip.MustParsePrefix("1.0.0.0/25"),
			value:   provenanceValue("base-specific-left", 25),
		},
		{
			network: netip.MustParsePrefix("1.0.0.128/25"),
			value:   provenanceValue("base-specific-right", 25),
		},
		{
			network: netip.MustParsePrefix("2.0.0.0/24"),
			value:   provenanceValue("base-wide", 24),
		},
	}
	for _, spec := range base {
		require.NoError(t, source.Insert(spec.network, spec.value))
	}
	sourcePath := writeTempDB(t, source)

	verifyLoadedProvenanceExtents(t, sourcePath, base)
	overlays := []benchmarkInsertSpec{
		{
			network: netip.MustParsePrefix("2.0.0.0/25"),
			value:   provenanceValue("overlay-specific", 25),
		},
		{
			network: netip.MustParsePrefix("1.0.0.0/24"),
			value:   provenanceValue("overlay-wide", 24),
		},
	}

	metadataTree := loadMetadataFixture(t, sourcePath)
	for _, spec := range overlays {
		require.NoError(t, metadataTree.InsertFunc(
			spec.network,
			spec.value,
			metadataSpecificityInserter,
		))
	}

	provenanceTree := loadMetadataFixture(t, sourcePath)
	sortedOverlays := slices.Clone(overlays)
	slices.SortFunc(sortedOverlays, func(left, right benchmarkInsertSpec) int {
		return right.network.Bits() - left.network.Bits()
	})
	for _, spec := range sortedOverlays {
		require.NoError(t, provenanceTree.InsertFunc(
			spec.network,
			spec.value,
			provenanceSpecificityInserter,
		))
	}

	stripProvenance(t, metadataTree)
	stripProvenance(t, provenanceTree)
	for _, test := range []struct {
		address string
		name    mmdbtype.String
	}{
		{address: "1.0.0.1", name: "base-specific-left"},
		{address: "1.0.0.200", name: "base-specific-right"},
		{address: "2.0.0.1", name: "overlay-specific"},
		{address: "2.0.0.200", name: "base-wide"},
	} {
		for _, tree := range []*Tree{metadataTree, provenanceTree} {
			_, value := tree.Get(netip.MustParseAddr(test.address))
			valueMap := value.(mmdbtype.Map)
			assert.Equal(t, test.name, valueMap["name"])
			assert.NotContains(t, valueMap, provenancePrefixLengthKey)
		}
	}
	assert.Equal(t, writeTreeBytes(t, metadataTree), writeTreeBytes(t, provenanceTree))
}

func TestMetadataSpecificityDiffersFromProvenanceAtDocumentedBoundaries(t *testing.T) {
	t.Run("load coalesces adjacent equal records", func(t *testing.T) {
		path := writeAdjacentEqualSource(
			t,
			netip.MustParsePrefix("1.2.3.0/25"),
			provenanceValue("base", 25),
		)
		metadataTree := loadMetadataFixture(t, path)
		provenanceTree := loadMetadataFixture(t, path)
		overlay := provenanceValue("overlay", 24)
		prefix := netip.MustParsePrefix("1.2.3.0/24")
		require.NoError(t, metadataTree.InsertFunc(prefix, overlay, metadataSpecificityInserter))
		require.NoError(
			t,
			provenanceTree.InsertFunc(prefix, overlay, provenanceSpecificityInserter),
		)

		_, metadataValue := metadataTree.Get(netip.MustParseAddr("1.2.3.4"))
		_, provenanceResult := provenanceTree.Get(netip.MustParseAddr("1.2.3.4"))
		assert.Equal(t, mmdbtype.String("overlay"), metadataValue.(mmdbtype.Map)["name"])
		assert.Equal(t, mmdbtype.String("base"), provenanceResult.(mmdbtype.Map)["name"])
	})

	t.Run("root remains two records", func(t *testing.T) {
		source := newMetadataTree(t, Options{IPVersion: 4})
		require.NoError(t, source.Insert(
			netip.MustParsePrefix("0.0.0.0/0"),
			provenanceValue("base", 0),
		))
		path := writeTempDB(t, source)
		metadataTree := loadMetadataFixture(t, path)
		provenanceTree := loadMetadataFixture(t, path)
		overlay := provenanceValue("overlay", 0)
		prefix := netip.MustParsePrefix("0.0.0.0/0")
		require.NoError(t, metadataTree.InsertFunc(prefix, overlay, metadataSpecificityInserter))
		require.NoError(
			t,
			provenanceTree.InsertFunc(prefix, overlay, provenanceSpecificityInserter),
		)

		physicalPrefix, metadataValue := metadataTree.Get(netip.MustParseAddr("1.2.3.4"))
		_, provenanceResult := provenanceTree.Get(netip.MustParseAddr("1.2.3.4"))
		assert.Equal(t, netip.MustParsePrefix("0.0.0.0/1"), physicalPrefix)
		assert.Equal(t, mmdbtype.String("base"), metadataValue.(mmdbtype.Map)["name"])
		assert.Equal(t, mmdbtype.String("overlay"), provenanceResult.(mmdbtype.Map)["name"])
	})
}

func FuzzInserterMetadata(f *testing.F) {
	f.Add([]byte{
		0, 0, 0,
		1, 0, 0,
		0, 0, 0,
	})
	f.Add([]byte{
		0, 0, 0,
		1, 0, 0,
		0, 0, 1,
	})
	f.Add([]byte{
		0, 0, 0,
		1, 0, 2,
		1, 0, 0,
	})
	f.Add([]byte{
		1, 0, 0,
		1, 128, 0,
		0, 0, 3,
	})
	// A /28 inserted into a /24 record: the split chain reports an existing
	// depth that is neither the inserted depth nor a record boundary.
	f.Add([]byte{
		0, 0, 0,
		4, 0, 0,
	})
	// A /24 failing over two existing /32s: the error fires on the first
	// covered record.
	f.Add([]byte{
		8, 0, 0,
		8, 1, 0,
		0, 0, 2,
	})
	// A /31 inserted into a /24 record, so the split chain descends seven
	// levels before it resolves.
	f.Add([]byte{
		0, 0, 0,
		7, 0, 0,
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		const bytesPerOperation = 3
		const maxOperations = 64
		operationCount := min(len(data)/bytesPerOperation, maxOperations)
		if operationCount == 0 {
			return
		}

		tree := newMetadataTree(t, Options{IPVersion: 4})
		for operation := range operationCount {
			offset := operation * bytesPerOperation
			prefixLength := 24 + int(data[offset]%9)
			prefix := netip.PrefixFrom(
				netip.AddrFrom4([4]byte{1, 2, 3, data[offset+1]}),
				prefixLength,
			).Masked()
			action := data[offset+2] % 4
			expected := metadataOracleRecords(t, tree, prefix)
			insertErr := errors.New("fuzz insertion failure")
			callIndex := 0
			err := tree.InsertFunc(
				prefix,
				mmdbtype.Uint32(operation+1),
				func(
					existingValue, newValue mmdbtype.DataType,
					metadata inserter.Metadata,
				) (mmdbtype.DataType, error) {
					require.Less(t, callIndex, len(expected))
					want := expected[callIndex]
					assert.Equal(t, want.existing, existingValue)
					assert.Equal(t, prefix, metadata.InsertedNetwork)
					assert.Equal(t, prefixLength, metadata.InsertedDepth())
					assert.Equal(t, want.metadata.ExistingDepth, metadata.ExistingDepth)
					assert.Equal(t, want.metadata.ExistingAddr, metadata.ExistingAddr)
					assert.Equal(t, want.metadata.ExistingNetwork(), metadata.ExistingNetwork())
					assert.Equal(t, 32, metadata.TreeDepth)

					currentCall := callIndex
					callIndex++
					switch action {
					case 0:
						return newValue, nil
					case 1:
						return nil, nil
					case 2:
						return nil, insertErr
					case 3:
						if currentCall == 1 || len(expected) == 1 {
							return nil, insertErr
						}
						return newValue, nil
					default:
						panic("unreachable fuzz action")
					}
				},
			)

			switch action {
			case 0, 1:
				require.NoError(t, err)
				assert.Len(t, expected, callIndex)
			case 2, 3:
				require.ErrorIs(t, err, insertErr)
			}
		}
	})
}

func metadataOracleRecords(
	t *testing.T,
	tree *Tree,
	insertedNetwork netip.Prefix,
) []metadataCall {
	t.Helper()
	require.True(t, insertedNetwork.Addr().Is4())
	require.GreaterOrEqual(t, insertedNetwork.Bits(), 24)
	require.LessOrEqual(t, insertedNetwork.Bits(), 32)
	records := make([]metadataCall, 0, insertedNetwork.Addr().BitLen())
	var previous netip.Prefix
	for address := insertedNetwork.Addr(); insertedNetwork.Contains(address); address = address.Next() {
		existingNetwork, existingValue := tree.Get(address)
		if existingNetwork == previous {
			continue
		}
		previous = existingNetwork
		records = append(records, metadataCall{
			existing: existingValue,
			metadata: inserter.Metadata{
				InsertedNetwork: insertedNetwork,
				ExistingDepth:   existingNetwork.Bits(),
				TreeDepth:       32,
			},
		})
		records[len(records)-1].metadata.ExistingAddr = treeAddr(existingNetwork, 32)
	}
	return records
}

const provenancePrefixLengthKey mmdbtype.String = "_prefix_length"

func provenanceValue(name mmdbtype.String, prefixLength uint16) mmdbtype.Map {
	return mmdbtype.Map{
		"name":                    name,
		provenancePrefixLengthKey: mmdbtype.Uint16(prefixLength),
	}
}

func metadataSpecificityInserter(
	existingValue, newValue mmdbtype.DataType,
	metadata inserter.Metadata,
) (mmdbtype.DataType, error) {
	if existingValue != nil && metadata.ExistingDepth > metadata.InsertedDepth() {
		return existingValue, nil
	}
	return newValue, nil
}

func provenanceSpecificityInserter(
	existingValue, newValue mmdbtype.DataType,
	_ inserter.Metadata,
) (mmdbtype.DataType, error) {
	if existingValue == nil {
		return newValue, nil
	}
	existingDepth := existingValue.(mmdbtype.Map)[provenancePrefixLengthKey].(mmdbtype.Uint16)
	newDepth := newValue.(mmdbtype.Map)[provenancePrefixLengthKey].(mmdbtype.Uint16)
	if existingDepth > newDepth {
		return existingValue, nil
	}
	return newValue, nil
}

func stripProvenance(tb testing.TB, tree *Tree) {
	tb.Helper()
	require.NoError(tb, tree.InsertFunc(
		netip.MustParsePrefix("0.0.0.0/0"),
		nil,
		func(existingValue, _ mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
			if existingValue == nil {
				return nil, nil
			}
			value := existingValue.Copy().(mmdbtype.Map)
			delete(value, provenancePrefixLengthKey)
			return value, nil
		},
	))
}

func verifyLoadedProvenanceExtents(
	t *testing.T,
	path string,
	specs []benchmarkInsertSpec,
) {
	t.Helper()
	tree := loadMetadataFixture(t, path)
	for _, spec := range specs {
		physicalPrefix, value := tree.Get(spec.network.Addr())
		storedDepth := value.(mmdbtype.Map)[provenancePrefixLengthKey].(mmdbtype.Uint16)
		assert.Equal(t, int(storedDepth), physicalPrefix.Bits())
	}
}

func loadMetadataFixture(tb testing.TB, path string) *Tree {
	tb.Helper()
	tree, err := Load(path, Options{
		BuildEpoch:              1,
		IPVersion:               4,
		IncludeReservedNetworks: true,
	})
	require.NoError(tb, err)
	return tree
}

func writeAdjacentEqualSource(
	t *testing.T,
	leftPrefix netip.Prefix,
	value mmdbtype.DataType,
) string {
	t.Helper()
	rightPrefix := netip.PrefixFrom(nextPrefixAddr(t, leftPrefix), leftPrefix.Bits())
	tree := newMetadataTree(t, Options{IPVersion: 4})
	require.NoError(t, tree.Insert(leftPrefix, value))
	require.NoError(t, tree.Insert(rightPrefix, mmdbtype.String("temporary")))
	tree.expandPaths(tree.root, 0)
	parentPrefix := netip.PrefixFrom(leftPrefix.Addr(), leftPrefix.Bits()-1).Masked()
	parent := recordAtPrefix(t, tree, parentPrefix)
	require.Equal(t, recordTypeNode, parent.recordType)
	node := tree.nodeAt(parent.nodeIndex)
	require.Equal(t, recordTypeData, node.children[0].recordType)
	require.Equal(t, recordTypeData, node.children[1].recordType)
	tree.valueStore.retain(node.children[0].value)
	tree.valueStore.release(node.children[1].value)
	node.children[1].value = node.children[0].value
	return writeTempDB(t, tree)
}

func recordAtPrefix(t *testing.T, tree *Tree, prefix netip.Prefix) *record {
	t.Helper()
	ip, prefixLength := tree.prefixInsertIP(prefix)
	currentNodeIndex := tree.root
	for depth := range prefixLength {
		currentNode := tree.nodeAt(currentNodeIndex)
		record := &currentNode.children[bitAt(ip, depth)]
		if depth+1 == prefixLength {
			return record
		}
		require.Contains(t, []recordType{recordTypeNode, recordTypeFixedNode}, record.recordType)
		currentNodeIndex = record.nodeIndex
	}
	t.Fatal("the root is not represented by a record")
	return nil
}

func nextPrefixAddr(t *testing.T, prefix netip.Prefix) netip.Addr {
	t.Helper()
	next := netipx.RangeOfPrefix(prefix).To().Next()
	require.True(t, next.IsValid(), "prefix %s has no following address", prefix)
	return next
}

func writeTreeBytes(tb testing.TB, tree *Tree) []byte {
	tb.Helper()
	var output bytes.Buffer
	_, err := tree.WriteTo(&output)
	require.NoError(tb, err)
	return output.Bytes()
}

func treeAddr(prefix netip.Prefix, treeDepth int) [16]byte {
	var address [16]byte
	if prefix.Addr().Is4() {
		as4 := prefix.Addr().As4()
		if treeDepth == 32 {
			copy(address[:4], as4[:])
		} else {
			copy(address[12:], as4[:])
		}
		return address
	}
	return prefix.Addr().As16()
}

func captureMetadata(calls *[]metadataCall) inserter.Func {
	return func(
		existingValue, newValue mmdbtype.DataType,
		metadata inserter.Metadata,
	) (mmdbtype.DataType, error) {
		*calls = append(*calls, metadataCall{existing: existingValue, metadata: metadata})
		return newValue, nil
	}
}

func captureExisting(calls *[]metadataCall) inserter.Func {
	return func(
		existingValue, _ mmdbtype.DataType,
		metadata inserter.Metadata,
	) (mmdbtype.DataType, error) {
		*calls = append(*calls, metadataCall{existing: existingValue, metadata: metadata})
		return existingValue, nil
	}
}

func assertMetadata(
	t *testing.T,
	metadata inserter.Metadata,
	inserted,
	existing string,
	existingDepth,
	treeDepth int,
) {
	t.Helper()
	assert.Equal(t, netip.MustParsePrefix(inserted), metadata.InsertedNetwork)
	assert.Equal(t, netip.MustParsePrefix(existing), metadata.ExistingNetwork())
	assert.Equal(t, existingDepth, metadata.ExistingDepth)
	assert.Equal(t, treeDepth, metadata.TreeDepth)
}

func newMetadataTree(t *testing.T, options Options) *Tree {
	t.Helper()
	options.BuildEpoch = 1
	options.DatabaseType = "inserter-metadata-test"
	options.Description = map[string]string{"en": "Inserter metadata test"}
	options.IncludeReservedNetworks = true
	options.RecordSize = 24
	tree, err := New(options)
	require.NoError(t, err)
	return tree
}
