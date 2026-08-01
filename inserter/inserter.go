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

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// Func resolves an insertion into a tree record. existingValue is nil for an
// empty record, and newValue is the value passed to the insert method. Returning
// nil leaves the record empty or removes the existing value. A Func is evaluated
// separately for every covered record when passed to Tree.InsertFunc or
// Tree.InsertRangeFunc. Tree.InsertPureFunc and Tree.InsertRangePureFunc may
// memoize repeated argument pairs and share a non-nil result across records.
//
// A Func must not modify either argument, as values may be shared with other
// records. Any non-nil returned value becomes tree-owned and must not be
// modified after the function returns.
type Func func(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error)

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
func TopLevelMerge(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
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
func DeepMerge(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
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
