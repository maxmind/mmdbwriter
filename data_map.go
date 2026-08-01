package mmdbwriter

import (
	"math"
	"reflect"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type dataMapHash uint64

// Please note, if you change the order of these fields, please check
// alignment as we end up storing quite a few in memory.
type dataMapValue struct {
	data mmdbtype.DataType
	next *dataMapValue
	hash dataMapHash

	// Alternatively, we could use a weak map for the data map, but I
	// don't see any very good options at the moment. We should revist
	// if something happens with https://github.com/golang/go/issues/43615
	refCount uint32
}

// dataMap is used to deduplicate data inserted into the tree. Hashes select a
// bucket; values in the bucket are compared exactly before they are reused.
type dataMap struct {
	data   map[dataMapHash]*dataMapValue
	hasher *dataHasher
}

func newDataMap() *dataMap {
	return &dataMap{
		data:   map[dataMapHash]*dataMapValue{},
		hasher: newDataHasher(),
	}
}

// store stores the value in the dataMap and returns the dataMapValue for it.
// If the value is already in the dataMap, the reference count for it is
// incremented.
func (dm *dataMap) store(v mmdbtype.DataType) (*dataMapValue, error) {
	return dm.storeByGeneratedKey(v)
}

func (dm *dataMap) storeByGeneratedKey(v mmdbtype.DataType) (*dataMapValue, error) {
	hash, err := dm.hasher.Hash(v)
	if err != nil {
		return nil, err
	}
	return dm.storeByHash(v, dataMapHash(hash)), nil
}

func (dm *dataMap) storeByHash(v mmdbtype.DataType, dmHash dataMapHash) *dataMapValue {
	for dmv := dm.data[dmHash]; dmv != nil; dmv = dmv.next {
		if wireDataEqual(dmv.data, v) {
			dmv.refCount++
			return dmv
		}
	}

	dmv := &dataMapValue{
		data:     v,
		hash:     dmHash,
		next:     dm.data[dmHash],
		refCount: 1,
	}
	dm.data[dmHash] = dmv
	return dmv
}

// wireDataEqual compares the data that will be written to the MMDB. Pointer
// forms are normalized before comparison. The mmdbtype Equal methods are then
// sufficient except for containers, which may hold floats, and floats, where
// signed zero has different wire encodings despite comparing equal in Go.
func wireDataEqual(first, second mmdbtype.DataType) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	var firstOK, secondOK bool
	first, firstOK = dereferenceDataType(first)
	second, secondOK = dereferenceDataType(second)
	if !firstOK || !secondOK {
		return false
	}
	switch first := first.(type) {
	case mmdbtype.Float32:
		second, ok := second.(mmdbtype.Float32)
		return ok && math.Float32bits(float32(first)) == math.Float32bits(float32(second))
	case mmdbtype.Float64:
		second, ok := second.(mmdbtype.Float64)
		return ok && math.Float64bits(float64(first)) == math.Float64bits(float64(second))
	case mmdbtype.Map:
		second, ok := second.(mmdbtype.Map)
		if !ok || len(first) != len(second) {
			return false
		}
		if reflect.ValueOf(first).Pointer() == reflect.ValueOf(second).Pointer() {
			return true
		}
		for key, firstValue := range first {
			secondValue, ok := second[key]
			if !ok || !wireDataEqual(firstValue, secondValue) {
				return false
			}
		}
		return true
	case mmdbtype.Slice:
		second, ok := second.(mmdbtype.Slice)
		if !ok || len(first) != len(second) {
			return false
		}
		if len(first) == 0 || &first[0] == &second[0] {
			return true
		}
		for index, firstValue := range first {
			if !wireDataEqual(firstValue, second[index]) {
				return false
			}
		}
		return true
	default:
		return first.Equal(second)
	}
}

// addRef adds a reference to the value.
func (dm *dataMap) addRef(v *dataMapValue) {
	// This is here mostly so that we don't have to guard against it
	// elsewhere.
	if v == nil {
		return
	}
	v.refCount++
}

// remove removes a reference to the value. If the reference count
// drops to zero, the value is removed from the dataMap.
func (dm *dataMap) remove(v *dataMapValue) {
	// This is here mostly so that we don't have to guard against it
	// elsewhere.
	if v == nil {
		return
	}
	v.refCount--

	if v.refCount == 0 {
		var previous *dataMapValue
		for current := dm.data[v.hash]; current != nil; current = current.next {
			if current != v {
				previous = current
				continue
			}
			if previous == nil {
				dm.data[v.hash] = current.next
			} else {
				previous.next = current.next
			}
			if dm.data[v.hash] == nil {
				delete(dm.data, v.hash)
			}
			break
		}
		v.next = nil
	}
}
