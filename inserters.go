package mmdbwriter

import "github.com/pkg/errors"

// XXX separate package?

// Remove any records for the network being inserted.
func Remove(value DataType) (DataType, error) {
	return nil, nil
}

// ReplaceWith generates an inserter function that replaces the existing
// value with the new value.
func ReplaceWith(value DataType) func(DataType) (DataType, error) {
	return func(_ DataType) (DataType, error) {
		return value, nil
	}
}

// TopLevelMergeWith creates an inserter for Map values that will update an
// existing Map by adding the top-level keys and values from the new Map,
// replacing any existing values for the keys.
//
// Both the new and existing value must be a Map. An error will be returned
// otherwise.
func TopLevelMergeWith(newValue DataType) func(DataType) (DataType, error) {
	return func(existingValue DataType) (DataType, error) {
		newMap, ok := newValue.(Map)
		if !ok {
			return nil, errors.Errorf(
				"the new value is a %T, not a Map. TopLevelMergeWith only works if both values are Map values.",
				newValue,
			)
		}

		if existingValue == nil {
			return newValue, nil
		}

		// A possible optimization would be to not bother copying
		// values that will be replaced.
		existingMap, ok := existingValue.(Map)
		if !ok {
			return nil, errors.Errorf(
				"the existing value is a %T, not a Map. TopLevelMergeWith only works if both values are Map values.",
				newValue,
			)
		}

		returnMap := existingMap.Copy().(Map)

		for k, v := range newMap {
			returnMap[k] = v.Copy()
		}

		return returnMap, nil
	}
}

// DeepMerge creates an inserter that will recursively update an existing
// value. Map and Slice values will be merged recursively. Other values will
// be replaced by the new value.
func DeepMerge(newValue DataType) func(DataType) (DataType, error) {
	return func(existingValue DataType) (DataType, error) {
		return deepMerge(existingValue, newValue)
	}
}

func deepMerge(existingValue, newValue DataType) (DataType, error) {
	if existingValue == nil {
		return newValue, nil
	}
	if newValue == nil {
		return existingValue, nil
	}
	switch existingValue := existingValue.(type) {
	case Map:
		newMap, ok := newValue.(Map)
		if !ok {
			return newValue, nil
		}
		existingMap := existingValue.Copy().(Map)
		for k, v := range newMap {
			nv, err := deepMerge(existingMap[k], v)
			if err != nil {
				return nil, err
			}
			existingMap[k] = nv
		}
		return existingMap, nil
	case Slice:
		newSlice, ok := newValue.(Slice)
		if !ok {
			return newValue, nil
		}
		length := len(existingValue)
		if len(newSlice) > length {
			length = len(newSlice)
		}

		rv := make(Slice, length)
		for i := range rv {
			var ev, nv DataType
			if i < len(existingValue) {
				ev = existingValue[i]
			}
			if i < len(newSlice) {
				nv = newSlice[i]
			}
			var err error
			rv[i], err = deepMerge(ev, nv)
			if err != nil {
				return nil, err
			}
		}
		return rv, nil
	default:
		return newValue, nil
	}
}
