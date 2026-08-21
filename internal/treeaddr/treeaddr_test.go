package treeaddr

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefixFromInsertIP(t *testing.T) {
	v4 := [16]byte{1, 2, 3, 0}
	v4InV6 := [16]byte{12: 1, 13: 2, 14: 3}
	v6 := [16]byte{0x20, 0x01, 0x0d, 0xb8}

	tests := []struct {
		name      string
		ip        [16]byte
		prefixLen int
		treeDepth int
		as4       bool
		expected  string
		wantErr   bool
	}{
		{
			name:      "32-bit tree ignores as4",
			ip:        v4,
			prefixLen: 24,
			treeDepth: 32,
			expected:  "1.2.3.0/24",
		},
		{
			name:      "32-bit tree with as4 set",
			ip:        v4,
			prefixLen: 24,
			treeDepth: 32,
			as4:       true,
			expected:  "1.2.3.0/24",
		},
		{
			name:      "IPv4 in a 128-bit tree",
			ip:        v4InV6,
			prefixLen: 120,
			treeDepth: 128,
			as4:       true,
			expected:  "1.2.3.0/24",
		},
		{
			name:      "IPv4 subtree root",
			ip:        v4InV6,
			prefixLen: 96,
			treeDepth: 128,
			as4:       true,
			expected:  "0.0.0.0/0",
		},
		{
			name:      "IPv6 in a 128-bit tree",
			ip:        v6,
			prefixLen: 32,
			treeDepth: 128,
			expected:  "2001:db8::/32",
		},
		{
			// The same bytes decode as either family, which is why the caller
			// decides rather than the layout.
			name:      "IPv4-shaped bytes read as IPv6",
			ip:        v4InV6,
			prefixLen: 120,
			treeDepth: 128,
			expected:  "::1.2.3.0/120",
		},
		{
			// Callers must not ask for IPv4 above the subtree: 40-96 is
			// negative, which Addr.Prefix rejects.
			name:      "as4 shallower than the IPv4 subtree",
			ip:        v4InV6,
			prefixLen: 40,
			treeDepth: 128,
			as4:       true,
			wantErr:   true,
		},
		{
			name:      "prefix length past the tree depth",
			ip:        v4,
			prefixLen: 33,
			treeDepth: 32,
			wantErr:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prefix, err := PrefixFromInsertIP(test.ip, test.prefixLen, test.treeDepth, test.as4)
			if test.wantErr {
				require.Error(t, err)
				assert.Equal(t, netip.Prefix{}, prefix)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, netip.MustParsePrefix(test.expected), prefix)
		})
	}
}

func TestIsIPv4SubtreeIP(t *testing.T) {
	assert.True(t, IsIPv4SubtreeIP([16]byte{12: 1, 13: 2, 14: 3, 15: 4}))
	assert.True(t, IsIPv4SubtreeIP([16]byte{}))
	assert.False(t, IsIPv4SubtreeIP([16]byte{0x20, 0x01}))
	// A genuine IPv6 address inside ::/96 satisfies the test, which is why
	// callers that know the family should not use it. See ENG-5302.
	assert.True(t, IsIPv4SubtreeIP([16]byte{15: 1}))
}
