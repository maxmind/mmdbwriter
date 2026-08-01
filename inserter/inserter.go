// Package inserter provides some common inserter functions for
// mmdbwriter.Tree.
package inserter

import (
	"fmt"
	"maps"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// Resolver resolves an insertion into a tree record. A Resolver must be either
// a Func or a PureFunc.
type Resolver interface {
	Function() Func
	IsPure() bool

	isResolver()
}

// Func resolves an insertion into a tree record. existingValue is nil for an
// empty record, and newValue is the value passed to the insert method. Returning
// nil leaves the record empty or removes the existing value. A Func is evaluated
// separately for every covered record and is not memoized.
//
// A Func must not modify either argument, as values may be shared with other
// records. Use PureFunc when the result and error depend only on the arguments.
type Func func(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error)

// Function returns f.
func (f Func) Function() Func {
	return f
}

// IsPure reports whether repeated argument pairs may be memoized.
func (Func) IsPure() bool {
	return false
}

func (Func) isResolver() {}

// PureFunc is a Func whose result and error depend only on its arguments. It
// must not modify either argument or depend on invocation count, order, or
// external mutable state. The writer may memoize repeated argument pairs within
// one insert operation.
type PureFunc Func

// Function returns f as a Func.
func (f PureFunc) Function() Func {
	return Func(f)
}

// IsPure reports whether repeated argument pairs may be memoized.
func (PureFunc) IsPure() bool {
	return true
}

func (PureFunc) isResolver() {}

// Remove removes any records for the network being inserted.
//
//nolint:gochecknoglobals // Exported stateless inserters are callable function values.
var Remove = PureFunc(func(_, _ mmdbtype.DataType) (mmdbtype.DataType, error) {
	return nil, nil
})

// Replace replaces the existing value with the new value.
//
//nolint:gochecknoglobals // Exported stateless inserters are callable function values.
var Replace = PureFunc(func(_, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
	return newValue, nil
})

// TopLevelMerge is an inserter for Map values that will update an
// existing Map by adding the top-level keys and values from the new Map,
// replacing any existing values for the keys.
//
// Both the new and existing value must be a Map. An error will be returned
// otherwise.
//
//nolint:gochecknoglobals // Exported stateless inserters are callable function values.
var TopLevelMerge = PureFunc(topLevelMerge)

func topLevelMerge(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
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
//
//nolint:gochecknoglobals // Exported stateless inserters are callable function values.
var DeepMerge = PureFunc(deepMergeFunc)

func deepMergeFunc(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
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
