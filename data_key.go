package mmdbwriter

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	stdhash "hash"
	"hash/maphash"
	"math"
	"math/big"
	"math/bits"
	"reflect"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

const (
	dataHashCacheSize     = 4_096
	dataHashProbationSize = 256
	dataHashMix           = uint64(0x9e3779b97f4a7c15)
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
	cacheByIdentity map[dataMapIdentityKey]int
	cache           []dataHashCacheEntry
	cacheHand       int
	probationByID   map[dataMapIdentityKey]int
	probation       []dataHashCacheEntry
	probationHand   int
}

func newDataHasher() *dataHasher {
	return &dataHasher{
		seed:            maphash.MakeSeed(),
		cacheByIdentity: map[dataMapIdentityKey]int{},
		probationByID:   map[dataMapIdentityKey]int{},
	}
}

func (h *dataHasher) Hash(value mmdbtype.DataType) (uint64, error) {
	// The data map already caches top-level identities. Avoid retaining every
	// distinct record in the nested-container cache as well.
	switch value := value.(type) {
	case mmdbtype.Map:
		return h.hashMapContents(value)
	case mmdbtype.Slice:
		return h.hashSliceContents(value)
	case mmdbtype.Bytes:
		return dataHashMix64(2 ^ maphash.Bytes(h.seed, value)), nil
	case *mmdbtype.Uint128:
		if value == nil {
			return 0, errors.New("cannot hash a nil *mmdbtype.Uint128")
		}
		return dataHashMix64(13 ^ maphash.Bytes(h.seed, (*big.Int)(value).Bytes())), nil
	default:
		return h.hashValue(value)
	}
}

func (h *dataHasher) hashValue(value mmdbtype.DataType) (uint64, error) {
	if value == nil {
		return 0, errors.New("cannot hash a nil MMDB value")
	}
	switch value := value.(type) {
	case mmdbtype.Bool:
		return dataHashMix64(1 ^ uint64(boolToUint8(bool(value)))), nil
	case mmdbtype.Bytes:
		return dataHashMix64(2 ^ maphash.Bytes(h.seed, value)), nil
	case mmdbtype.Float32:
		return dataHashMix64(3 ^ uint64(math.Float32bits(float32(value)))), nil
	case mmdbtype.Float64:
		return dataHashMix64(4 ^ math.Float64bits(float64(value))), nil
	case mmdbtype.Int32:
		//nolint:gosec // the signed bit pattern is part of the encoded value
		return dataHashMix64(5 ^ uint64(uint32(value))), nil
	case mmdbtype.Map:
		return h.hashMap(value)
	case mmdbtype.Pointer:
		return dataHashMix64(7 ^ uint64(value)), nil
	case mmdbtype.Slice:
		return h.hashSlice(value)
	case mmdbtype.String:
		return dataHashMix64(9 ^ maphash.String(h.seed, string(value))), nil
	case mmdbtype.Uint16:
		return dataHashMix64(10 ^ uint64(value)), nil
	case mmdbtype.Uint32:
		return dataHashMix64(11 ^ uint64(value)), nil
	case mmdbtype.Uint64:
		return dataHashMix64(12 ^ uint64(value)), nil
	case *mmdbtype.Uint128:
		if value == nil {
			return 0, errors.New("cannot hash a nil *mmdbtype.Uint128")
		}
		return dataHashMix64(13 ^ maphash.Bytes(h.seed, (*big.Int)(value).Bytes())), nil
	default:
		return 0, fmt.Errorf("unsupported MMDB data type %T", value)
	}
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
	digest := dataHashMix64(6 ^ uint64(len(value))*dataHashMix ^ sum ^ bits.RotateLeft64(xor, 23))
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
	hash := dataHashMix64(8 ^ uint64(len(value)))
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

func mapDataIdentity(value mmdbtype.Map) dataMapIdentityKey {
	return dataMapIdentityKey{
		ptr: reflect.ValueOf(value).Pointer(), kind: dataMapIdentityMap, size: len(value),
	}
}

func sliceDataIdentity(value mmdbtype.Slice) dataMapIdentityKey {
	return dataMapIdentityKey{
		ptr: reflect.ValueOf(value).Pointer(), kind: dataMapIdentitySlice, size: len(value),
	}
}

// keyWriter is similar to dataWriter but it will never use pointers. This
// will produce a unique key for the type.
type keyWriter struct {
	*bytes.Buffer

	sha256 stdhash.Hash
	key    [sha256.Size]byte
}

func newKeyWriter() *keyWriter {
	return &keyWriter{Buffer: &bytes.Buffer{}, sha256: sha256.New()}
}

// Key generates a unique key for the data structure v.
//
// This is just a quick hack. I am sure there is
// something better.
func (kw *keyWriter) Key(v mmdbtype.DataType) ([]byte, error) {
	kw.Truncate(0)
	kw.sha256.Reset()
	_, err := v.WriteTo(kw)
	if err != nil {
		return nil, err
	}
	if _, err := kw.WriteTo(kw.sha256); err != nil {
		return nil, fmt.Errorf("writing key to writer: %w", err)
	}
	return kw.sha256.Sum(kw.key[:0]), nil
}

// KeyString is intentionally identical to Key but takes a concrete String to
// keep map-key writes from boxing into DataType on the write hot path.
func (kw *keyWriter) KeyString(v mmdbtype.String) ([]byte, error) {
	kw.Truncate(0)
	kw.sha256.Reset()
	_, err := v.WriteTo(kw)
	if err != nil {
		return nil, err
	}
	if _, err := kw.WriteTo(kw.sha256); err != nil {
		return nil, fmt.Errorf("writing key to writer: %w", err)
	}
	return kw.sha256.Sum(kw.key[:0]), nil
}

func (kw *keyWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	return t.WriteTo(kw)
}

// WriteOrWritePointerString mirrors WriteOrWritePointer without converting the
// map key to DataType. keyWriter never emits pointers.
func (kw *keyWriter) WriteOrWritePointerString(t mmdbtype.String) (int64, error) {
	return t.WriteTo(kw)
}
