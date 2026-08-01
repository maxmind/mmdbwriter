package mmdbwriter

import (
	"math"
	"reflect"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type dataMapHash uint64

type dataMapIdentityKind byte

const (
	dataMapIdentityBytes dataMapIdentityKind = iota
	dataMapIdentityMap
	dataMapIdentitySlice
	dataMapIdentityUint128
)

type dataMapIdentityKey struct {
	ptr  uintptr
	kind dataMapIdentityKind
	size int
}

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
	data              map[dataMapHash]*dataMapValue
	hasher            *dataHasher
	keyByDataIdentity map[dataMapIdentityKey]*dataMapValue
}

func newDataMap() *dataMap {
	return newDataMapWithHasher(newDataHasher())
}

func newDataMapWithHasher(hasher *dataHasher) *dataMap {
	return &dataMap{
		data:   map[dataMapHash]*dataMapValue{},
		hasher: hasher,
	}
}

// store stores the value in the dataMap and returns the dataMapValue for it.
// If the value is already in the dataMap, the reference count for it is
// incremented.
func (dm *dataMap) store(v mmdbtype.DataType) (*dataMapValue, error) {
	return dm.storeByGeneratedKey(v)
}

func (dm *dataMap) storeWithIdentity(v mmdbtype.DataType) (*dataMapValue, error) {
	identity, ok := keyIdentity(v)
	if !ok {
		return dm.store(v)
	}

	if dmv, ok := dm.keyByDataIdentity[identity]; ok {
		// Only retained values are pinned by dm.data. Equal-but-distinct values
		// can be mutated or collected, so stale identity entries must be
		// discarded instead of treated as hits.
		if dmv.refCount != 0 {
			if retainedIdentity, ok := keyIdentity(dmv.data); ok && retainedIdentity == identity {
				dmv.refCount++
				return dmv, nil
			}
		}
		delete(dm.keyByDataIdentity, identity)
	}

	dmv, err := dm.storeByGeneratedKey(v)
	if err != nil {
		return nil, err
	}
	if dm.keyByDataIdentity == nil {
		dm.keyByDataIdentity = map[dataMapIdentityKey]*dataMapValue{}
	}
	// Register only the identity of the retained value. A generated-key dedupe
	// can return an older equal value, whose pointer is the only safe one to
	// cache because dm.data pins it until remove.
	if retainedIdentity, ok := keyIdentity(dmv.data); ok && retainedIdentity == identity {
		dm.keyByDataIdentity[identity] = dmv
	}
	return dmv, nil
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

func keyIdentity(v mmdbtype.DataType) (dataMapIdentityKey, bool) {
	var ok bool
	v, ok = dereferenceDataType(v)
	if !ok {
		return dataMapIdentityKey{}, false
	}
	switch t := v.(type) {
	case mmdbtype.Bytes:
		if len(t) == 0 {
			return dataMapIdentityKey{kind: dataMapIdentityBytes}, true
		}
		return dataMapIdentityKey{
			ptr:  sliceIdentityPointer(t),
			kind: dataMapIdentityBytes,
			size: len(t),
		}, true
	case mmdbtype.Map:
		if len(t) == 0 {
			return dataMapIdentityKey{kind: dataMapIdentityMap}, true
		}
		return dataMapIdentityKey{
			ptr:  reflect.ValueOf(t).Pointer(),
			kind: dataMapIdentityMap,
			size: len(t),
		}, true
	case mmdbtype.Slice:
		if len(t) == 0 {
			return dataMapIdentityKey{kind: dataMapIdentitySlice}, true
		}
		return dataMapIdentityKey{
			ptr:  sliceIdentityPointer(t),
			kind: dataMapIdentitySlice,
			size: len(t),
		}, true
	case *mmdbtype.Uint128:
		if t == nil {
			return dataMapIdentityKey{}, false
		}
		return dataMapIdentityKey{
			ptr:  reflect.ValueOf(t).Pointer(),
			kind: dataMapIdentityUint128,
		}, true
	default:
		return dataMapIdentityKey{}, false
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
		if dm.keyByDataIdentity != nil {
			if identity, ok := keyIdentity(v.data); ok {
				if dm.keyByDataIdentity[identity] == v {
					delete(dm.keyByDataIdentity, identity)
				}
			}
		}
		v.next = nil
	}
}
