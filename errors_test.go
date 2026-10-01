package mmdbwriter

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbwriter/v2/inserter"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestNetworkErrorAddressFamily(t *testing.T) {
	// These bytes can represent either family. Only the insertion knows which.
	ip := [16]byte{12: 1, 13: 2, 14: 3, 15: 4}
	for _, test := range []struct {
		name     string
		as4      bool
		inserted string
		blocked  string
	}{
		{"IPv4", true, "1.2.3.0/24", "1.0.0.0/8"},
		{"IPv6", false, "::102:300/120", "::100:0/104"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inserted := netip.MustParsePrefix(test.inserted)
			blocked := netip.MustParsePrefix(test.blocked)

			err := newReservedNetworkError(ip, 104, 120, 128, test.as4)
			var reservedErr *ReservedNetworkError
			require.ErrorAs(t, err, &reservedErr)
			assert.Equal(t, inserted, reservedErr.InsertedNetwork)
			assert.Equal(t, blocked, reservedErr.ReservedNetwork)
			require.EqualError(t, err, fmt.Sprintf(
				"attempt to insert %s into %s, which is a reserved network", inserted, blocked,
			))

			err = newAliasedNetworkError(ip, 104, 120, 128, test.as4)
			var aliasedErr *AliasedNetworkError
			require.ErrorAs(t, err, &aliasedErr)
			assert.Equal(t, inserted, aliasedErr.InsertedNetwork)
			assert.Equal(t, blocked, aliasedErr.AliasedNetwork)
			require.EqualError(t, err, fmt.Sprintf(
				"attempt to insert %s into %s, which is an aliased network", inserted, blocked,
			))
		})
	}
}

func TestInsertNetworkErrorAddressFamily(t *testing.T) {
	for _, test := range []struct {
		name      string
		ipVersion int
		input     string
		inserted  string
		blocked   string
		aliased   bool
	}{
		{"IPv6 loopback", 6, "::1/128", "::1/128", "::/104", false},
		{"IPv6 subnet", 6, "::/104", "::/104", "::/104", false},
		{"IPv6 unmasked", 6, "::1/104", "::/104", "::/104", false},
		{"IPv4 in IPv6 tree", 6, "0.0.0.1/32", "0.0.0.1/32", "0.0.0.0/8", false},
		{"IPv4 tree", 4, "0.0.0.1/32", "0.0.0.1/32", "0.0.0.0/8", false},
		{"mapped IPv4", 6, "::ffff:0.0.0.1/128", "0.0.0.1/32", "0.0.0.0/8", false},
		{"native IPv6 reserved", 6, "2001:db8::1/128", "2001:db8::1/128", "2001:db8::/32", false},
		{"native IPv6 alias", 6, "2002:100::/24", "2002:100::/24", "2002::/16", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, method := range []string{
				"Insert", "Insert with Options.Inserter", "InsertPureFunc", "InsertFunc",
				"InsertRange", "InsertRange with Options.Inserter", "InsertRangePureFunc", "InsertRangeFunc",
			} {
				t.Run(method, func(t *testing.T) {
					opts := Options{IPVersion: test.ipVersion}
					if method == "Insert with Options.Inserter" ||
						method == "InsertRange with Options.Inserter" {
						opts.Inserter = inserter.Replace
					}
					tree, err := New(opts)
					require.NoError(t, err)

					prefix := netip.MustParsePrefix(test.input)
					ipRange := netipx.RangeOfPrefix(prefix)
					value := mmdbtype.String("value")
					withMetadata := func(_, replacement mmdbtype.DataType, _ inserter.Metadata) (mmdbtype.DataType, error) {
						t.Error("inserter called for a blocked network")
						return replacement, nil
					}
					switch method {
					case "Insert", "Insert with Options.Inserter":
						err = tree.Insert(prefix, value)
					case "InsertPureFunc":
						err = tree.InsertPureFunc(prefix, value, inserter.Replace)
					case "InsertFunc":
						err = tree.InsertFunc(prefix, value, withMetadata)
					case "InsertRange", "InsertRange with Options.Inserter":
						err = tree.InsertRange(ipRange.From(), ipRange.To(), value)
					case "InsertRangePureFunc":
						err = tree.InsertRangePureFunc(
							ipRange.From(),
							ipRange.To(),
							value,
							inserter.Replace,
						)
					case "InsertRangeFunc":
						err = tree.InsertRangeFunc(
							ipRange.From(),
							ipRange.To(),
							value,
							withMetadata,
						)
					default:
						t.Fatalf("unknown insertion method %q", method)
					}

					inserted := netip.MustParsePrefix(test.inserted)
					blocked := netip.MustParsePrefix(test.blocked)
					kind := "a reserved"
					if test.aliased {
						kind = "an aliased"
						var aliasedErr *AliasedNetworkError
						require.ErrorAs(t, err, &aliasedErr)
						assert.Equal(t, inserted, aliasedErr.InsertedNetwork)
						assert.Equal(t, blocked, aliasedErr.AliasedNetwork)
					} else {
						var reservedErr *ReservedNetworkError
						require.ErrorAs(t, err, &reservedErr)
						assert.Equal(t, inserted, reservedErr.InsertedNetwork)
						assert.Equal(t, blocked, reservedErr.ReservedNetwork)
					}
					require.EqualError(t, err, fmt.Sprintf(
						"attempt to insert %s into %s, which is %s network",
						inserted,
						blocked,
						kind,
					))
				})
			}
		})
	}
}
