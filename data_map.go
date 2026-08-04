package mmdbwriter

import (
	"reflect"
	"unsafe"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// dataMapHash is a digest from a dataHasher. Values are only comparable within
// a single dataHasher instance, because each one is independently seeded.
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
	data                map[dataMapHash]*dataMapValue
	hasher              *dataHasher
	valueByDataIdentity map[dataMapIdentityKey]*dataMapValue
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
	return dm.storeByContentHash(v)
}

func (dm *dataMap) storeWithIdentity(v mmdbtype.DataType) (*dataMapValue, error) {
	identity, ok := keyIdentity(v)
	if !ok {
		return dm.store(v)
	}

	if dmv, ok := dm.valueByDataIdentity[identity]; ok {
		// Only retained values are pinned by dm.data. Equal-but-distinct values
		// can be mutated or collected, so stale identity entries must be
		// discarded instead of treated as hits.
		if dmv.refCount != 0 {
			if retainedIdentity, ok := keyIdentity(dmv.data); ok && retainedIdentity == identity {
				dmv.refCount++
				return dmv, nil
			}
		}
		delete(dm.valueByDataIdentity, identity)
	}

	dmv, err := dm.storeByContentHash(v)
	if err != nil {
		return nil, err
	}
	if dm.valueByDataIdentity == nil {
		dm.valueByDataIdentity = map[dataMapIdentityKey]*dataMapValue{}
	}
	// Register only the identity of the retained value. A hash dedupe can
	// return an older equal value, whose pointer is the only safe one to
	// cache because dm.data pins it until remove.
	if retainedIdentity, ok := keyIdentity(dmv.data); ok && retainedIdentity == identity {
		dm.valueByDataIdentity[identity] = dmv
	}
	return dmv, nil
}

func (dm *dataMap) storeByContentHash(v mmdbtype.DataType) (*dataMapValue, error) {
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
// forms are normalized before comparison, after which the mmdbtype Equal
// methods are wire-exact. Containers are walked here rather than through
// Map.Equal and Slice.Equal so that nested pointer forms are normalized too.
//
// A value holding a nil typed pointer is reported unequal to everything,
// including an identical nil. That would break the one-live-value-per-wire-value
// invariant if such a value could be stored, but it cannot: hashValue rejects
// nil pointers, so a store fails before reaching the dataMap.
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

func sliceIdentityPointer[T any](value []T) uintptr {
	// The pointer is used only as an identity while a typed strong reference
	// keeps the slice live. It is never dereferenced or converted back.
	//nolint:gosec // converting to uintptr is intentional for the identity key
	return uintptr(unsafe.Pointer(unsafe.SliceData(value)))
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
//
// A nil value is ignored. Removing a reference that was never taken panics,
// as does removing a value that is not in the bucket its hash names: both mean
// the reference counting or the index is already corrupt, and continuing would
// spread the damage silently.
func (dm *dataMap) remove(v *dataMapValue) {
	// This is here mostly so that we don't have to guard against it
	// elsewhere.
	if v == nil {
		return
	}
	if v.refCount == 0 {
		panic("mmdbwriter: dataMap.remove called on a value with no references")
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
		if dm.valueByDataIdentity != nil {
			if identity, ok := keyIdentity(v.data); ok {
				if dm.valueByDataIdentity[identity] == v {
					delete(dm.valueByDataIdentity, identity)
				}
			}
		}
		v.next = nil
	}
}
