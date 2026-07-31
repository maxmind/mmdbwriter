// Package inserter provides some common inserter functions for
// mmdbwriter.Tree.
package inserter

import (
	"fmt"
	"maps"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// Func resolves an insertion into a tree record. existingValue is nil for an
// empty record, and newValue is the value passed to the insert method. Returning
// nil leaves the record empty or removes the existing value.
//
// A Func must be pure: its result and error must depend only on its arguments.
// The writer may memoize repeated argument pairs within one insert operation,
// so callers must not depend on invocation count or order. A Func must not
// modify either argument.
type Func func(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error)

// Remove any records for the network being inserted.
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
	unchanged := true
	for key, value := range newMap {
		existing, exists := existingMap[key]
		if !exists || existing == nil || value == nil || !existing.Equal(value) {
			unchanged = false
			break
		}
	}
	if unchanged {
		return existingValue, nil
	}

	returnMap := make(mmdbtype.Map, len(existingMap)+len(newMap))
	maps.Copy(returnMap, existingMap)
	maps.Copy(returnMap, newMap)

	return returnMap, nil
}

// DeepMerge recursively updates an existing value. Map and Slice values will be
// merged recursively. Other values will be replaced by the new value.
func DeepMerge(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
	return deepMerge(existingValue, newValue)
}

func deepMerge(existingValue, newValue mmdbtype.DataType) (mmdbtype.DataType, error) {
	if existingValue == nil {
		return newValue, nil
	}
	if newValue == nil {
		return existingValue, nil
	}
	switch existingValue := existingValue.(type) {
	case mmdbtype.Map:
		newMap, ok := newValue.(mmdbtype.Map)
		if !ok {
			// The new value is not a map. Overwrite the existing value
			return newValue, nil
		}

		var returnMap mmdbtype.Map
		for k, v := range newMap {
			existingChild, exists := existingValue[k]
			nv, err := deepMerge(existingChild, v)
			if err != nil {
				return nil, err
			}
			if exists && existingChild != nil && nv != nil && existingChild.Equal(nv) {
				continue
			}
			if returnMap == nil {
				returnMap = make(mmdbtype.Map, len(existingValue)+len(newMap))
				maps.Copy(returnMap, existingValue)
			}
			returnMap[k] = nv
		}
		if returnMap == nil {
			return existingValue, nil
		}
		return returnMap, nil
	case mmdbtype.Slice:
		newSlice, ok := newValue.(mmdbtype.Slice)
		if !ok {
			return newValue, nil
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
			merged, err := deepMerge(ev, nv)
			if err != nil {
				return nil, err
			}
			if i < len(existingValue) && ev != nil && merged != nil && ev.Equal(merged) {
				continue
			}
			if i < len(existingValue) && ev == nil && merged == nil {
				continue
			}
			if rv == nil {
				rv = make(mmdbtype.Slice, length)
				copy(rv, existingValue)
			}
			rv[i] = merged
		}
		if rv == nil {
			return existingValue, nil
		}
		return rv, nil
	default:
		if existingValue.Equal(newValue) {
			return existingValue, nil
		}
		return newValue, nil
	}
}
