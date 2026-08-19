// Package inserter provides some common inserter functions for
// mmdbwriter.Tree.
//
// Every function in this package is pure: its result and error depend only on
// its arguments. They are all safe to pass to mmdbwriter.Tree.InsertPureFunc
// and mmdbwriter.Tree.InsertRangePureFunc, which may memoize repeated argument
// pairs and share a result across records.
package inserter

import (
	"fmt"
	"maps"
	"net/netip"

	"github.com/maxmind/mmdbwriter/v2/internal/treeaddr"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// Metadata describes the insertion an inserter is resolving and the record it
// is about to change. That record is what a Tree.Get for an address in it would
// have returned just before the insertion. Metadata does not identify the
// network that established an existing value, so a policy that needs that
// provenance must store it in the value.
//
// For a range insert, each decomposed prefix is a separate insertion, and
// metadata for a later prefix reflects the changes earlier prefixes made.
//
// Earlier insertions shape the record boundaries reported here. They can split
// a record and remerge it, leave a fragment of a wider record, or split empty
// space into narrower empty records.
//
// ExistingDepth is set for every record. ExistingAddr is zero and
// ExistingNetwork returns the zero Prefix when the existing record is more
// specific than InsertedNetwork, which the walk would have to track branch
// paths to report.
//
// A Metadata is inconsistent when InsertedNetwork is invalid, TreeDepth is
// neither 32 nor 128, or a 32-bit tree carries a non-IPv4 insertion. The
// insertion methods never produce one. The methods below return zero values
// for it, and other hand-built combinations are unspecified.
//
// Fields may be added in later releases. Construct Metadata values with keyed
// literals.
type Metadata struct {
	// Force keyed literals and keep Metadata non-comparable, reserving the
	// right to add fields of any type.
	_ [0]func()

	// InsertedNetwork is the network being inserted. For a range insert, it is
	// the individual prefix into which the range decomposed.
	InsertedNetwork netip.Prefix

	// ExistingDepth is the depth in tree bits of the record that held the
	// existing value, in [0, TreeDepth]. Each record reports its own depth, so
	// a record wider than InsertedNetwork reports that width, not the depth
	// this insertion splits it to: inserting a /24 over a /16 record reports
	// the /16's depth.
	//
	// An IPv4 network in an IPv6 tree is 96 bits deeper than its address-family
	// prefix length, so compare ExistingDepth with InsertedDepth(), not
	// InsertedNetwork.Bits().
	ExistingDepth int

	// ExistingAddr is the tree-space address of the existing record, zeroed at
	// and below ExistingDepth. Most callers want ExistingNetwork instead. A
	// 32-bit tree uses bytes 0-3. A 128-bit tree uses all 16 bytes, with IPv4
	// addresses in bytes 12-15. It is a copy, not a view of tree storage.
	ExistingAddr [16]byte

	// TreeDepth is 32 for an IPv4 tree and 128 for an IPv6 tree. It is needed
	// to interpret ExistingAddr's layout.
	TreeDepth int
}

// InsertedDepth returns InsertedNetwork's depth in tree bits from the root,
// adding 96 for an IPv4 network in an IPv6 tree. It returns 0 for an
// inconsistent Metadata. It does not read ExistingDepth.
func (m Metadata) InsertedDepth() int {
	depth, _ := m.insertedDepth()
	return depth
}

// insertedDepth returns InsertedNetwork's depth in tree bits, and reports
// whether the fields it reads are consistent.
func (m Metadata) insertedDepth() (int, bool) {
	if !m.InsertedNetwork.IsValid() ||
		(m.TreeDepth != 32 && m.TreeDepth != 128) ||
		(m.TreeDepth == 32 && !m.InsertedNetwork.Addr().Is4()) {
		return 0, false
	}

	depth := m.InsertedNetwork.Bits()
	if m.TreeDepth == 128 && m.InsertedNetwork.Addr().Is4() {
		depth += 96
	}
	return depth, true
}

// ExistingNetwork returns the network of the record that held the existing
// value, as that record stood just before this insertion. The result follows
// the inserted network's address family, as Tree.Get follows its query. For an
// IPv4 insert into an IPv6 tree, records shallower than the IPv4 subtree are
// returned in IPv6 form.
//
// It returns the zero Prefix for an inconsistent Metadata, an ExistingDepth
// outside [0, TreeDepth], or an existing record more specific than
// InsertedNetwork.
func (m Metadata) ExistingNetwork() netip.Prefix {
	insertedDepth, ok := m.insertedDepth()
	if !ok ||
		m.ExistingDepth < 0 ||
		m.ExistingDepth > m.TreeDepth ||
		m.ExistingDepth > insertedDepth {
		return netip.Prefix{}
	}

	as4 := m.TreeDepth == 32 ||
		(m.InsertedNetwork.Addr().Is4() && m.ExistingDepth >= 96)
	prefix, err := treeaddr.PrefixFromInsertIP(
		m.ExistingAddr,
		m.ExistingDepth,
		m.TreeDepth,
		as4,
	)
	if err != nil {
		return netip.Prefix{}
	}
	return prefix
}

// PureFunc resolves an insertion into a tree record without receiving insertion
// metadata. existingValue is nil for an empty record, and newValue is the value
// passed to the insert method or decoded during Load. Returning nil leaves the
// record empty or removes the existing value.
//
// A PureFunc's result and error must depend only on its arguments. It must not
// depend on invocation count, order, or external mutable state. Repeated
// argument pairs may be memoized during an insertion, and a non-nil result may
// be shared by multiple records.
//
// A PureFunc must not modify either argument. The existing value is a shared,
// read-only view of tree storage. The new value is the value passed to the
// insert call, or a shared view of a decoded record during Load. Call Copy on a
// value to get a private copy you can modify. Any non-nil returned value becomes
// tree-owned and must not be modified after the function returns.
//
// A panic propagates. The insertion methods' guarantees for callbacks that
// return errors do not apply to a panic.
//
// Only direct inserts and a PureFunc's result are validated, so a PureFunc can
// receive an unsupported input value, such as a raw mmdbtype.Pointer or an
// out-of-range mmdbtype.Uint128, and must replace or discard it.
type PureFunc func(
	existingValue,
	newValue mmdbtype.DataType,
) (mmdbtype.DataType, error)

// Func resolves an insertion into a tree record. existingValue is nil for an
// empty record, and newValue is the value passed to the insert method. Returning
// nil leaves the record empty or removes the existing value. A Func is evaluated
// separately for every covered record when passed to Tree.InsertFunc or
// Tree.InsertRangeFunc.
//
// metadata describes the insertion and the current tree record represented by
// existingValue. For a range, InsertedNetwork is the individual decomposed
// prefix.
//
// The rules PureFunc documents for modifying values, for panics, and for
// unvalidated input values apply here too. A Func is never memoized, because
// its metadata varies per record.
type Func func(
	existingValue,
	newValue mmdbtype.DataType,
	metadata Metadata,
) (mmdbtype.DataType, error)

// Remove removes any records for the network being inserted.
func Remove(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
	return nil, nil
}

// Replace replaces the existing value with the new value.
func Replace(_, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
	return newValue, nil
}

// TopLevelMerge is an inserter for Map values that will update an
// existing Map by adding the top-level keys and values from the new Map,
// replacing any existing values for the keys.
//
// Both the new and existing value must be a Map. An error will be returned
// otherwise.
func TopLevelMerge(
	existingValue,
	newValue mmdbtype.DataType,
) (mmdbtype.DataType, error) {
	newMap, ok := newValue.(mmdbtype.Map)
	if !ok {
		return nil, fmt.Errorf(
			"the new value is a %T, not a Map; TopLevelMerge only works if both values are Map values",
			newValue,
		)
	}

	if existingValue == nil {
		return newValue, nil
	}

	// A possible optimization would be to not bother copying
	// values that will be replaced.
	existingMap, ok := existingValue.(mmdbtype.Map)
	if !ok {
		return nil, fmt.Errorf(
			"the existing value is a %T, not a Map; TopLevelMerge only works if both values are Map values",
			existingValue,
		)
	}
	returnMap := make(mmdbtype.Map, len(existingMap)+len(newMap))
	maps.Copy(returnMap, existingMap)
	maps.Copy(returnMap, newMap)

	return returnMap, nil
}

// DeepMerge recursively updates an existing value. Map and Slice values will be
// merged recursively. Other values will be replaced by the new value. The
// returned value may be the existing container or retain unchanged nested
// containers from it. The result must therefore be treated as immutable.
func DeepMerge(
	existingValue,
	newValue mmdbtype.DataType,
) (mmdbtype.DataType, error) {
	value, _, err := deepMerge(existingValue, newValue)
	return value, err
}

func deepMerge(
	existingValue,
	newValue mmdbtype.DataType,
) (mmdbtype.DataType, bool, error) {
	if existingValue == nil {
		return newValue, newValue != nil, nil
	}
	if newValue == nil {
		return existingValue, false, nil
	}
	switch existingValue := existingValue.(type) {
	case mmdbtype.Map:
		newMap, ok := newValue.(mmdbtype.Map)
		if !ok {
			// The new value is not a map. Overwrite the existing value
			return newValue, true, nil
		}

		var returnMap mmdbtype.Map
		for k, v := range newMap {
			existingChild, exists := existingValue[k]
			nv, changed, err := deepMerge(existingChild, v)
			if err != nil {
				return nil, false, err
			}
			if exists && !changed {
				continue
			}
			if returnMap == nil {
				returnMap = make(mmdbtype.Map, len(existingValue)+len(newMap))
				maps.Copy(returnMap, existingValue)
			}
			returnMap[k] = nv
		}
		if returnMap == nil {
			return existingValue, false, nil
		}
		return returnMap, true, nil
	case mmdbtype.Slice:
		newSlice, ok := newValue.(mmdbtype.Slice)
		if !ok {
			return newValue, true, nil
		}
		length := max(len(newSlice), len(existingValue))

		var rv mmdbtype.Slice
		for i := range length {
			var ev, nv mmdbtype.DataType
			if i < len(existingValue) {
				ev = existingValue[i]
			}
			if i < len(newSlice) {
				nv = newSlice[i]
			}
			merged, changed, err := deepMerge(ev, nv)
			if err != nil {
				return nil, false, err
			}
			if i < len(existingValue) && !changed {
				continue
			}
			if rv == nil {
				rv = make(mmdbtype.Slice, length)
				// Restore skipped existing indices; new tail indices are
				// assigned below, so the result cannot contain accidental holes.
				copy(rv, existingValue)
			}
			rv[i] = merged
		}
		if rv == nil {
			return existingValue, false, nil
		}
		return rv, true, nil
	default:
		if existingValue.Equal(newValue) {
			return existingValue, false, nil
		}
		return newValue, true, nil
	}
}
