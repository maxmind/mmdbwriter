package mmdbwriter

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/big"
	"math/bits"
	"reflect"
	"unsafe"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

const (
	dataHashCacheSize     = 4_096
	dataHashProbationSize = 256
	dataHashMix           = uint64(0x9e3779b97f4a7c15)
)

type dataHashKind byte

const (
	dataHashBool dataHashKind = iota + 1
	dataHashBytes
	dataHashFloat32
	dataHashFloat64
	dataHashInt32
	dataHashMap
	dataHashPointer
	dataHashSlice
	dataHashString
	dataHashUint16
	dataHashUint32
	dataHashUint64
	dataHashUint128
	dataHashKindCount
)

type dataHashCacheEntry struct {
	identity dataMapIdentityKey
	hash     uint64
	used     bool

	// Typed strong references prevent an identity from being reused by the Go
	// runtime while its cached hash remains live.
	mapValue   mmdbtype.Map
	sliceValue mmdbtype.Slice
}

// dataHasher hashes the typed structure of an MMDB value. Nested containers
// are cached by identity because inserted values are immutable. A small
// probation cache prevents one-off containers from displacing reused values in
// the bounded clock cache. Hash matches are always followed by an exact
// comparison, so correctness does not depend on collision resistance.
type dataHasher struct {
	seed            maphash.Seed
	typeSalts       [dataHashKindCount]uint64
	cacheByIdentity map[dataMapIdentityKey]int
	cache           []dataHashCacheEntry
	cacheHand       int
	probationByID   map[dataMapIdentityKey]int
	probation       []dataHashCacheEntry
	probationHand   int
}

func newDataHasher() *dataHasher {
	hasher := &dataHasher{
		seed:            maphash.MakeSeed(),
		cacheByIdentity: map[dataMapIdentityKey]int{},
		probationByID:   map[dataMapIdentityKey]int{},
	}
	for kind := dataHashBool; kind < dataHashKindCount; kind++ {
		hasher.typeSalts[kind] = maphash.Comparable(hasher.seed, kind)
	}
	return hasher
}

func (h *dataHasher) Hash(value mmdbtype.DataType) (uint64, error) {
	var ok bool
	original := value
	value, ok = dereferenceDataType(value)
	if !ok {
		return 0, fmt.Errorf("cannot hash a nil %T", original)
	}
	// The data map already caches top-level identities. Avoid retaining every
	// distinct record in the nested-container cache as well.
	switch value := value.(type) {
	case mmdbtype.Map:
		return h.hashMapContents(value)
	case mmdbtype.Slice:
		return h.hashSliceContents(value)
	case mmdbtype.Bytes:
		return h.hashBytes(dataHashBytes, value), nil
	case *mmdbtype.Uint128:
		if value == nil {
			return 0, errors.New("cannot hash a nil *mmdbtype.Uint128")
		}
		return h.hashBytes(dataHashUint128, (*big.Int)(value).Bytes()), nil
	default:
		return h.hashValue(value)
	}
}

func (h *dataHasher) hashValue(value mmdbtype.DataType) (uint64, error) {
	if value == nil {
		return 0, errors.New("cannot hash a nil MMDB value")
	}
	original := value
	var ok bool
	value, ok = dereferenceDataType(value)
	if !ok {
		return 0, fmt.Errorf("cannot hash a nil %T", original)
	}
	switch value := value.(type) {
	case mmdbtype.Bool:
		return h.hashScalar(dataHashBool, uint64(boolToUint8(bool(value)))), nil
	case mmdbtype.Bytes:
		return h.hashBytes(dataHashBytes, value), nil
	case mmdbtype.Float32:
		return h.hashScalar(dataHashFloat32, uint64(math.Float32bits(float32(value)))), nil
	case mmdbtype.Float64:
		return h.hashScalar(dataHashFloat64, math.Float64bits(float64(value))), nil
	case mmdbtype.Int32:
		//nolint:gosec // the signed bit pattern is part of the encoded value
		return h.hashScalar(dataHashInt32, uint64(uint32(value))), nil
	case mmdbtype.Map:
		return h.hashMap(value)
	case mmdbtype.Pointer:
		return h.hashScalar(dataHashPointer, uint64(value)), nil
	case mmdbtype.Slice:
		return h.hashSlice(value)
	case mmdbtype.String:
		return h.hashScalar(dataHashString, maphash.String(h.seed, string(value))), nil
	case mmdbtype.Uint16:
		return h.hashScalar(dataHashUint16, uint64(value)), nil
	case mmdbtype.Uint32:
		return h.hashScalar(dataHashUint32, uint64(value)), nil
	case mmdbtype.Uint64:
		return h.hashScalar(dataHashUint64, uint64(value)), nil
	case *mmdbtype.Uint128:
		if value == nil {
			return 0, errors.New("cannot hash a nil *mmdbtype.Uint128")
		}
		return h.hashBytes(dataHashUint128, (*big.Int)(value).Bytes()), nil
	default:
		return 0, fmt.Errorf("unsupported MMDB data type %T", value)
	}
}

func (h *dataHasher) hashScalar(kind dataHashKind, value uint64) uint64 {
	return dataHashMix64(h.typeSalts[kind] ^ value)
}

func (h *dataHasher) hashBytes(kind dataHashKind, value []byte) uint64 {
	return h.hashScalar(kind, maphash.Bytes(h.seed, value))
}

func (h *dataHasher) hashMap(value mmdbtype.Map) (uint64, error) {
	identity := mapDataIdentity(value)
	if digest, ok := h.cachedHash(identity); ok {
		return digest, nil
	}
	if entry, ok := h.probationHash(identity); ok {
		h.rememberHash(entry)
		return entry.hash, nil
	}

	digest, err := h.hashMapContents(value)
	if err != nil {
		return 0, err
	}
	h.rememberProbation(dataHashCacheEntry{identity: identity, hash: digest, mapValue: value})
	return digest, nil
}

func (h *dataHasher) hashMapContents(value mmdbtype.Map) (uint64, error) {
	// Both accumulators are commutative so map iteration order cannot affect the
	// result. The second accumulator makes canceling sums less likely.
	var sum, xor uint64
	for key, child := range value {
		childHash, err := h.hashValue(child)
		if err != nil {
			return 0, fmt.Errorf("hashing map key %q: %w", key, err)
		}
		keyHash := maphash.String(h.seed, string(key))
		entryHash := dataHashMix64(keyHash ^ bits.RotateLeft64(childHash, 17))
		sum += entryHash
		xor ^= bits.RotateLeft64(entryHash, int(entryHash>>58))
	}
	digest := dataHashMix64(
		h.typeSalts[dataHashMap] ^ uint64(
			len(value),
		)*dataHashMix ^ sum ^ bits.RotateLeft64(
			xor,
			23,
		),
	)
	return digest, nil
}

func (h *dataHasher) hashSlice(value mmdbtype.Slice) (uint64, error) {
	identity := sliceDataIdentity(value)
	if hash, ok := h.cachedHash(identity); ok {
		return hash, nil
	}
	if entry, ok := h.probationHash(identity); ok {
		h.rememberHash(entry)
		return entry.hash, nil
	}

	hash, err := h.hashSliceContents(value)
	if err != nil {
		return 0, err
	}
	h.rememberProbation(dataHashCacheEntry{identity: identity, hash: hash, sliceValue: value})
	return hash, nil
}

func (h *dataHasher) hashSliceContents(value mmdbtype.Slice) (uint64, error) {
	hash := dataHashMix64(h.typeSalts[dataHashSlice] ^ uint64(len(value)))
	for index, child := range value {
		childHash, err := h.hashValue(child)
		if err != nil {
			return 0, fmt.Errorf("hashing slice index %d: %w", index, err)
		}
		hash = dataHashMix64(hash ^ dataHashMix64(uint64(index)*dataHashMix^childHash))
	}
	return hash, nil
}

func (h *dataHasher) cachedHash(identity dataMapIdentityKey) (uint64, bool) {
	index, ok := h.cacheByIdentity[identity]
	if !ok {
		return 0, false
	}
	h.cache[index].used = true
	return h.cache[index].hash, true
}

func (h *dataHasher) probationHash(identity dataMapIdentityKey) (dataHashCacheEntry, bool) {
	index, ok := h.probationByID[identity]
	if !ok {
		return dataHashCacheEntry{}, false
	}
	delete(h.probationByID, identity)
	entry := h.probation[index]
	h.probation[index] = dataHashCacheEntry{}
	return entry, true
}

func (h *dataHasher) rememberProbation(entry dataHashCacheEntry) {
	if len(h.probation) < dataHashProbationSize {
		index := len(h.probation)
		h.probation = append(h.probation, entry)
		h.probationByID[entry.identity] = index
		return
	}

	oldIdentity := h.probation[h.probationHand].identity
	if index, ok := h.probationByID[oldIdentity]; ok && index == h.probationHand {
		delete(h.probationByID, oldIdentity)
	}
	h.probation[h.probationHand] = entry
	h.probationByID[entry.identity] = h.probationHand
	h.probationHand = (h.probationHand + 1) % len(h.probation)
}

func (h *dataHasher) rememberHash(entry dataHashCacheEntry) {
	entry.used = true
	if len(h.cache) < dataHashCacheSize {
		index := len(h.cache)
		h.cache = append(h.cache, entry)
		h.cacheByIdentity[entry.identity] = index
		return
	}

	for h.cache[h.cacheHand].used {
		h.cache[h.cacheHand].used = false
		h.cacheHand = (h.cacheHand + 1) % len(h.cache)
	}
	delete(h.cacheByIdentity, h.cache[h.cacheHand].identity)
	h.cache[h.cacheHand] = entry
	h.cacheByIdentity[entry.identity] = h.cacheHand
	h.cacheHand = (h.cacheHand + 1) % len(h.cache)
}

func dataHashMix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func boolToUint8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

// dereferenceDataType normalizes pointers to MMDB types whose DataType methods
// have value receivers. The bool reports whether the pointer, if any, was
// non-nil. Uint128 is intentionally left as a pointer because only its pointer
// form implements DataType.
func dereferenceDataType(value mmdbtype.DataType) (mmdbtype.DataType, bool) {
	switch value := value.(type) {
	case *mmdbtype.Bool:
		return dereference(value)
	case *mmdbtype.Bytes:
		return dereference(value)
	case *mmdbtype.Float32:
		return dereference(value)
	case *mmdbtype.Float64:
		return dereference(value)
	case *mmdbtype.Int32:
		return dereference(value)
	case *mmdbtype.Map:
		return dereference(value)
	case *mmdbtype.Pointer:
		return dereference(value)
	case *mmdbtype.Slice:
		return dereference(value)
	case *mmdbtype.String:
		return dereference(value)
	case *mmdbtype.Uint16:
		return dereference(value)
	case *mmdbtype.Uint32:
		return dereference(value)
	case *mmdbtype.Uint64:
		return dereference(value)
	default:
		return value, true
	}
}

func dereference[T mmdbtype.DataType](value *T) (mmdbtype.DataType, bool) {
	if value == nil {
		return nil, false
	}
	return *value, true
}

func mapDataIdentity(value mmdbtype.Map) dataMapIdentityKey {
	return dataMapIdentityKey{
		ptr: reflect.ValueOf(value).Pointer(), kind: dataMapIdentityMap, size: len(value),
	}
}

func sliceDataIdentity(value mmdbtype.Slice) dataMapIdentityKey {
	return dataMapIdentityKey{
		ptr:  sliceIdentityPointer(value),
		kind: dataMapIdentitySlice,
		size: len(value),
	}
}

func sliceIdentityPointer[T any](value []T) uintptr {
	// The pointer is used only as an identity while a typed strong reference
	// keeps the slice live. It is never dereferenced or converted back.
	//nolint:gosec // converting to uintptr is intentional for the identity key
	return uintptr(unsafe.Pointer(unsafe.SliceData(value)))
}
