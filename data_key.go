package mmdbwriter

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/big"
	"math/bits"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

const dataHashMix = uint64(0x9e3779b97f4a7c15)

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

// dataHasher hashes the typed structure of an MMDB value. Hash matches are
// always followed by an exact comparison, so correctness does not depend on
// collision resistance.
type dataHasher struct {
	seed      maphash.Seed
	typeSalts [dataHashKindCount]uint64
}

func newDataHasher() *dataHasher {
	hasher := &dataHasher{seed: maphash.MakeSeed()}
	for kind := dataHashBool; kind < dataHashKindCount; kind++ {
		hasher.typeSalts[kind] = maphash.Comparable(hasher.seed, kind)
	}
	return hasher
}

func (h *dataHasher) Hash(value mmdbtype.DataType) (uint64, error) {
	return h.hashValue(value)
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
		return h.hashMapContents(value)
	case mmdbtype.Pointer:
		return h.hashScalar(dataHashPointer, uint64(value)), nil
	case mmdbtype.Slice:
		return h.hashSliceContents(value)
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
