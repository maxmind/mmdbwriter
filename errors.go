package mmdbwriter

import (
	"errors"
	"fmt"
	"net/netip"

	"github.com/maxmind/mmdbwriter/v2/internal/treeaddr"
)

// errNilInserterFunc is returned by the insertion methods that take an inserter
// function when that function is nil. A nil Options.Inserter is not an error;
// it is equivalent to inserter.Replace.
var errNilInserterFunc = errors.New("inserter function must not be nil")

var errReentrantMutation = errors.New(
	"cannot insert into or write a tree during an active insert or write",
)

// AliasedNetworkError is returned when inserting a aliased network into
// a Tree where DisableIPv4Aliasing in Options is false.
type AliasedNetworkError struct {
	// AliasedNetwork is the aliased network being inserted into.
	AliasedNetwork netip.Prefix
	// InsertedNetwork is the network being inserted into the Tree.
	InsertedNetwork netip.Prefix
}

func newAliasedNetworkError(
	ip [16]byte,
	curPrefixLen,
	recPrefixLen,
	treeDepth int,
	as4 bool,
) error {
	anErr := &AliasedNetworkError{}
	var err error
	anErr.InsertedNetwork, err = treeaddr.PrefixFromInsertIP(ip, recPrefixLen, treeDepth, as4)
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

	anErr.AliasedNetwork, err = treeaddr.PrefixFromInsertIP(ip, curPrefixLen, treeDepth, as4)
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
	as4 bool,
) error {
	rnErr := &ReservedNetworkError{}
	var err error
	rnErr.InsertedNetwork, err = treeaddr.PrefixFromInsertIP(ip, recPrefixLen, treeDepth, as4)
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

	rnErr.ReservedNetwork, err = treeaddr.PrefixFromInsertIP(ip, curPrefixLen, treeDepth, as4)
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

func (r *ReservedNetworkError) Error() string {
	return fmt.Sprintf(
		"attempt to insert %s into %s, which is a reserved network",
		r.InsertedNetwork,
		r.ReservedNetwork,
	)
}
