package mmdbwriter

import (
	"errors"
	"fmt"
	"net/netip"
)

// AliasedNetworkError is returned when inserting a aliased network into
// a Tree where DisableIPv4Aliasing in Options is false.
type AliasedNetworkError struct {
	// AliasedNetwork is the aliased network being inserted into.
	AliasedNetwork netip.Prefix
	// InsertedNetwork is the network being inserted into the Tree.
	InsertedNetwork netip.Prefix
}

func newAliasedNetworkError(ip [16]byte, curPrefixLen, recPrefixLen, treeDepth int) error {
	anErr := &AliasedNetworkError{}
	var err error
	anErr.InsertedNetwork, err = prefixFromInsertIP(ip, recPrefixLen, treeDepth)
	if err != nil {
		return errors.Join(
			fmt.Errorf(
				"creating inserted network prefix with prefix length %d: %w",
				recPrefixLen,
				err,
			),
			anErr,
		)
	}

	anErr.AliasedNetwork, err = prefixFromInsertIP(ip, curPrefixLen, treeDepth)
	if err != nil {
		return errors.Join(
			fmt.Errorf(
				"creating aliased network prefix with prefix length %d: %w",
				curPrefixLen,
				err,
			),
			anErr,
		)
	}
	return anErr
}

func (r *AliasedNetworkError) Error() string {
	return fmt.Sprintf(
		"attempt to insert %s into %s, which is an aliased network",
		r.InsertedNetwork,
		r.AliasedNetwork,
	)
}

// ReservedNetworkError is returned when inserting a reserved network into
// a Tree where IncludeReservedNetworks in Options is false.
type ReservedNetworkError struct {
	// InsertedNetwork is the network being inserted into the Tree.
	InsertedNetwork netip.Prefix
	// ReservedNetwork is the reserved network being inserted into.
	ReservedNetwork netip.Prefix
}

var _ error = &ReservedNetworkError{}

func newReservedNetworkError(
	ip [16]byte,
	curPrefixLen,
	recPrefixLen,
	treeDepth int,
) error {
	rnErr := &ReservedNetworkError{}
	var err error
	rnErr.InsertedNetwork, err = prefixFromInsertIP(ip, recPrefixLen, treeDepth)
	if err != nil {
		return errors.Join(
			fmt.Errorf(
				"creating inserted network prefix with prefix length %d: %w",
				recPrefixLen,
				err,
			),
			rnErr,
		)
	}

	rnErr.ReservedNetwork, err = prefixFromInsertIP(ip, curPrefixLen, treeDepth)
	if err != nil {
		return errors.Join(
			fmt.Errorf(
				"creating reserved network prefix with prefix length %d: %w",
				curPrefixLen,
				err,
			),
			rnErr,
		)
	}
	return rnErr
}

func prefixFromInsertIP(ip [16]byte, prefixLen, treeDepth int) (netip.Prefix, error) {
	// Reverse Tree.addrInsertIP's byte layout so error prefixes are reported in
	// the same address family callers inserted.
	if treeDepth == 32 {
		addr := netip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]})
		return prefixFromAddr(addr, prefixLen)
	}

	if isIPv4SubtreeIP(ip) && prefixLen >= 96 {
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

func isIPv4SubtreeIP(ip [16]byte) bool {
	for _, b := range ip[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}

func (r *ReservedNetworkError) Error() string {
	return fmt.Sprintf(
		"attempt to insert %s into %s, which is a reserved network",
		r.InsertedNetwork,
		r.ReservedNetwork,
	)
}
