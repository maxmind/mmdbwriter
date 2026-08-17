// Package treeaddr converts between the byte layout used by the search tree
// and netip prefixes.
package treeaddr

import (
	"fmt"
	"net/netip"
)

// PrefixFromInsertIP reverses the search tree's address layout. The as4
// argument selects an IPv4 result for a 128-bit tree. A 32-bit tree always
// returns an IPv4 prefix.
func PrefixFromInsertIP(
	ip [16]byte,
	prefixLen,
	treeDepth int,
	as4 bool,
) (netip.Prefix, error) {
	if treeDepth == 32 {
		addr := netip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]})
		return prefixFromAddr(addr, prefixLen)
	}

	if as4 {
		addr := netip.AddrFrom4([4]byte{ip[12], ip[13], ip[14], ip[15]})
		return prefixFromAddr(addr, prefixLen-96)
	}

	addr := netip.AddrFrom16(ip)
	return prefixFromAddr(addr, prefixLen)
}

func prefixFromAddr(addr netip.Addr, prefixLen int) (netip.Prefix, error) {
	prefix, err := addr.Prefix(prefixLen)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf(
			"creating prefix from addr %s and prefix length %d: %w",
			addr,
			prefixLen,
			err,
		)
	}
	return prefix, nil
}

// IsIPv4SubtreeIP reports whether ip is in the IPv4 subtree of a 128-bit
// search tree.
func IsIPv4SubtreeIP(ip [16]byte) bool {
	for _, b := range ip[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}
