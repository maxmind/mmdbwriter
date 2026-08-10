package mmdbwriter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// valueRef is a stable handle into a valueStore. Zero is the nil value. A
// handle can be reused after its reference count reaches zero. Identity-cache
// entries therefore own their reference or live no longer than the node.
type valueRef uint32

const nilValueRef valueRef = 0

type valueKind uint8

const (
	valueKindInvalid valueKind = iota
	valueKindBool
	valueKindBytes
	valueKindFloat32
	valueKindFloat64
	valueKindInt32
	valueKindMap
	valueKindSlice
	valueKindString
	valueKindUint16
	valueKindUint32
	valueKindUint64
	valueKindUint128
)

type valueNode struct {
	hash uint64

	payloadOffset  uint32
	payloadLen     uint32
	childrenOffset uint32
	childrenLen    uint32

	refCount uint32
	// nextInBucket chains the live nodes that share a hash bucket. The chain
	// replaces a per-bucket slice and is pointer-free, so the garbage
	// collector never scans it.
	nextInBucket valueRef
	kind         valueKind

	// Materialized values are immutable store-owned views. Keeping the view on
	// the canonical node lets every record share nested maps and slices.
	materialized mmdbtype.DataType
	identity     dataIdentityKey
	hasIdentity  bool
}

type byteArena struct {
	data []byte
	free map[uint32][]uint32
}

func (a *byteArena) put(value []byte) (uint32, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if uint64(len(value)) > math.MaxUint32 {
		return 0, errors.New("value payload exceeds the value-store limit")
	}
	length := uint32(len(value)) //nolint:gosec // length was bounded above
	var offset uint32
	if offsets := a.free[length]; len(offsets) != 0 {
		offset = offsets[len(offsets)-1]
		a.free[length] = offsets[:len(offsets)-1]
		copy(a.data[offset:offset+length], value)
		return offset, nil
	}
	if uint64(len(a.data))+uint64(length) > math.MaxUint32 {
		return 0, errors.New("value payload arena exceeds the value-store limit")
	}
	offset = uint32(len(a.data)) //nolint:gosec // arena length was bounded above
	a.data = append(a.data, value...)
	return offset, nil
}

func (a *byteArena) release(offset, length uint32) {
	if length == 0 {
		return
	}
	if a.free == nil {
		a.free = map[uint32][]uint32{}
	}
	a.free[length] = append(a.free[length], offset)
}

type refArena struct {
	data []valueRef
	free map[uint32][]uint32
}

func (a *refArena) put(value []valueRef) (uint32, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if uint64(len(value)) > math.MaxUint32 {
		return 0, errors.New("value child list exceeds the value-store limit")
	}
	length := uint32(len(value)) //nolint:gosec // length was bounded above
	var offset uint32
	if offsets := a.free[length]; len(offsets) != 0 {
		offset = offsets[len(offsets)-1]
		a.free[length] = offsets[:len(offsets)-1]
		copy(a.data[offset:offset+length], value)
		return offset, nil
	}
	if uint64(len(a.data))+uint64(length) > math.MaxUint32 {
		return 0, errors.New("value child arena exceeds the value-store limit")
	}
	offset = uint32(len(a.data)) //nolint:gosec // arena length was bounded above
	a.data = append(a.data, value...)
	return offset, nil
}

func (a *refArena) release(offset, length uint32) {
	if length == 0 {
		return
	}
	clear(a.data[offset : offset+length])
	if a.free == nil {
		a.free = map[uint32][]uint32{}
	}
	a.free[length] = append(a.free[length], offset)
}

type dataIdentityKind uint8

const (
	dataIdentityBytes dataIdentityKind = iota
	dataIdentityMap
	dataIdentitySlice
	dataIdentityUint128
)

type dataIdentityKey struct {
	ptr  uintptr
	kind dataIdentityKind
	size int
}

type callerIdentityEntry struct {
	key   dataIdentityKey
	value mmdbtype.DataType // strong reference; prevents pointer-address reuse
	ref   valueRef          // separately retained while the entry is present
	prev  int
	next  int
}

// callerIdentityCacheSize bounds the caller-identity cache. The cache serves
// direct inserts that present the same object repeatedly, such as callers
// that cache decoded records. Each entry pins the caller's value and the
// interned node it maps to. The bound therefore trades retained memory for
// repeat-insert speed. It must comfortably exceed the distinct values a
// build presents repeatedly.
const callerIdentityCacheSize = 1 << 20

// valueStore hash-conses all nodes in an MMDB value tree. Buckets are keyed
// by a seeded maphash, but candidates are always compared exactly, so
// collisions cannot alter correctness.
type valueStore struct {
	nodes    []valueNode
	freeRefs []valueRef
	// buckets holds the head of each hash chain. Nodes link the rest through
	// nextInBucket.
	buckets  map[uint64]valueRef
	payloads byteArena
	children refArena

	materializedByIdentity map[dataIdentityKey]valueRef
	callerByIdentity       map[dataIdentityKey]int
	callerIdentity         []callerIdentityEntry
	callerIdentityHead     int
	callerIdentityTail     int
	callerIdentityLimit    int

	hashFunc    func([]byte) uint64
	hashScratch []byte
	// releaseScratch is the reusable worklist for cascading releases. It
	// avoids recursion through deep values and amortizes the worklist
	// allocation across releases.
	releaseScratch []valueRef
	// encodeScratch is reused for every scalar encoding. internNode copies
	// payloads into the arena, so the buffer never outlives one call. Scalars
	// do not nest, so one buffer suffices where containers need pools.
	encodeScratch scalarWriter
	// pairScratch and childScratch pool the per-container working slices.
	// Containers nest, so each internMap or internSlice call takes a slice
	// and returns it when done.
	pairScratch  [][]mapEntry
	childScratch [][]valueRef
}

// mapEntry carries one key and value of a map being interned, so the map is
// iterated once and sorted without further lookups.
type mapEntry struct {
	key   string
	child mmdbtype.DataType
}

func newValueStore() *valueStore {
	return newValueStoreWithHash(nil)
}

// newValueStoreWithHash exists so tests can force hash collisions. A nil
// hashFunc selects the seeded default.
func newValueStoreWithHash(hashFunc func([]byte) uint64) *valueStore {
	if hashFunc == nil {
		seed := maphash.MakeSeed()
		hashFunc = func(b []byte) uint64 { return maphash.Bytes(seed, b) }
	}
	return &valueStore{
		nodes:                  make([]valueNode, 1), // ref zero is nil
		buckets:                map[uint64]valueRef{},
		materializedByIdentity: map[dataIdentityKey]valueRef{},
		callerByIdentity:       map[dataIdentityKey]int{},
		callerIdentityHead:     -1,
		callerIdentityTail:     -1,
		callerIdentityLimit:    callerIdentityCacheSize,
		hashFunc:               hashFunc,
	}
}

// dereferenceDataType normalizes pointers to MMDB types whose DataType methods
// have value receivers. The bool reports whether the pointer, if any, was
// non-nil. Uint128 is intentionally left as a pointer because only its pointer
// form implements DataType. *Uint128 is therefore exempt: it is returned
// unchanged with a true result even when it is nil, so callers must nil-check
// it themselves.
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

func sliceIdentityPointer[T any](value []T) uintptr {
	// The pointer is used only as an identity while a typed strong reference
	// keeps the slice live. It is never dereferenced or converted back.
	//nolint:gosec // converting to uintptr is intentional for the identity key
	return uintptr(unsafe.Pointer(unsafe.SliceData(value)))
}

func dataIdentity(value mmdbtype.DataType) (dataIdentityKey, bool) {
	value, ok := dereferenceDataType(value)
	if !ok {
		return dataIdentityKey{}, false
	}
	switch value := value.(type) {
	case mmdbtype.Bytes:
		if len(value) == 0 {
			return dataIdentityKey{kind: dataIdentityBytes}, true
		}
		return dataIdentityKey{
			ptr: sliceIdentityPointer(value), kind: dataIdentityBytes, size: len(value),
		}, true
	case mmdbtype.Map:
		if len(value) == 0 {
			return dataIdentityKey{kind: dataIdentityMap}, true
		}
		return dataIdentityKey{
			ptr: reflect.ValueOf(value).Pointer(), kind: dataIdentityMap, size: len(value),
		}, true
	case mmdbtype.Slice:
		if len(value) == 0 {
			return dataIdentityKey{kind: dataIdentitySlice}, true
		}
		return dataIdentityKey{
			ptr: sliceIdentityPointer(value), kind: dataIdentitySlice, size: len(value),
		}, true
	case *mmdbtype.Uint128:
		if value == nil {
			return dataIdentityKey{}, false
		}
		return dataIdentityKey{
			ptr: reflect.ValueOf(value).Pointer(), kind: dataIdentityUint128,
		}, true
	default:
		return dataIdentityKey{}, false
	}
}

func (s *valueStore) node(ref valueRef) *valueNode {
	if ref == 0 || int(ref) >= len(s.nodes) || s.nodes[ref].kind == valueKindInvalid {
		panic(fmt.Sprintf("invalid value reference %d", ref))
	}
	return &s.nodes[ref]
}

func (s *valueStore) payload(node *valueNode) []byte {
	return s.payloads.data[node.payloadOffset : node.payloadOffset+node.payloadLen]
}

func (s *valueStore) childRefs(node *valueNode) []valueRef {
	return s.children.data[node.childrenOffset : node.childrenOffset+node.childrenLen]
}

func (s *valueStore) retain(ref valueRef) {
	if ref == nilValueRef {
		return
	}
	node := s.node(ref)
	if node.refCount == math.MaxUint32 {
		panic("value reference count overflow")
	}
	node.refCount++
}

func (s *valueStore) release(ref valueRef) {
	if ref == nilValueRef {
		return
	}
	worklist := s.releaseScratch[:0]
	worklist = append(worklist, ref)
	for len(worklist) != 0 {
		current := worklist[len(worklist)-1]
		worklist = worklist[:len(worklist)-1]
		node := s.node(current)
		if node.refCount == 0 {
			panic("value reference count underflow")
		}
		node.refCount--
		if node.refCount != 0 {
			continue
		}

		// Unlink the exact ref from its collision chain before the slot can
		// be reused. Chain order is otherwise stable and has no observable
		// effect.
		if head := s.buckets[node.hash]; head == current {
			if node.nextInBucket == nilValueRef {
				delete(s.buckets, node.hash)
			} else {
				s.buckets[node.hash] = node.nextInBucket
			}
		} else {
			for prev := head; prev != nilValueRef; prev = s.nodes[prev].nextInBucket {
				if s.nodes[prev].nextInBucket == current {
					s.nodes[prev].nextInBucket = node.nextInBucket
					break
				}
			}
		}

		if node.hasIdentity {
			delete(s.materializedByIdentity, node.identity)
		}
		worklist = append(worklist, s.childRefs(node)...)
		s.payloads.release(node.payloadOffset, node.payloadLen)
		s.children.release(node.childrenOffset, node.childrenLen)
		*node = valueNode{}
		s.freeRefs = append(s.freeRefs, current)
	}
	s.releaseScratch = worklist
}

func (s *valueStore) intern(value mmdbtype.DataType) (valueRef, error) {
	if value == nil {
		return nilValueRef, nil
	}
	original := value
	value, ok := dereferenceDataType(value)
	if !ok {
		return nilValueRef, fmt.Errorf("cannot intern a nil %T", original)
	}
	if identity, ok := dataIdentity(value); ok {
		if ref, ok := s.materializedByIdentity[identity]; ok {
			s.retain(ref)
			return ref, nil
		}
		if index, ok := s.callerByIdentity[identity]; ok {
			entry := &s.callerIdentity[index]
			if entry.value != nil && entry.ref != nilValueRef {
				s.touchCallerIdentity(index)
				s.retain(entry.ref)
				return entry.ref, nil
			}
			delete(s.callerByIdentity, identity)
		}
	}

	return s.internUncached(value)
}

// internString takes a concrete String so map keys avoid boxing into
// DataType, which otherwise allocates for every key of every container.
func (s *valueStore) internString(value mmdbtype.String) (valueRef, error) {
	s.encodeScratch.Reset()
	if _, err := value.WriteTo(&s.encodeScratch); err != nil {
		return nilValueRef, fmt.Errorf("encoding %T for value store: %w", value, err)
	}
	ref, _, err := s.internNode(valueKindString, s.encodeScratch.Bytes(), nil)
	return ref, err
}

func (s *valueStore) internUncached(value mmdbtype.DataType) (valueRef, error) {
	switch value := value.(type) {
	case mmdbtype.Map:
		return s.internMap(value)
	case mmdbtype.Slice:
		return s.internSlice(value)
	default:
		kind, err := kindOf(value)
		if err != nil {
			return nilValueRef, err
		}
		s.encodeScratch.Reset()
		if _, err := value.WriteTo(&s.encodeScratch); err != nil {
			return nilValueRef, fmt.Errorf("encoding %T for value store: %w", value, err)
		}
		ref, _, err := s.internNode(kind, s.encodeScratch.Bytes(), nil)
		return ref, err
	}
}

func (s *valueStore) takePairScratch() []mapEntry {
	if n := len(s.pairScratch); n != 0 {
		pairs := s.pairScratch[n-1]
		s.pairScratch = s.pairScratch[:n-1]
		return pairs[:0]
	}
	return nil
}

func (s *valueStore) putPairScratch(pairs []mapEntry) {
	// Entries reference caller-owned keys and values; clear the full backing
	// array so pooling does not pin those object graphs.
	pairs = pairs[:cap(pairs)]
	clear(pairs)
	s.pairScratch = append(s.pairScratch, pairs)
}

func (s *valueStore) takeChildScratch() []valueRef {
	if n := len(s.childScratch); n != 0 {
		children := s.childScratch[n-1]
		s.childScratch = s.childScratch[:n-1]
		return children[:0]
	}
	return nil
}

func (s *valueStore) putChildScratch(children []valueRef) {
	s.childScratch = append(s.childScratch, children)
}

func (s *valueStore) internMap(value mmdbtype.Map) (valueRef, error) {
	pairs := s.takePairScratch()
	defer func() { s.putPairScratch(pairs) }()
	for key, child := range value {
		pairs = append(pairs, mapEntry{key: string(key), child: child})
	}
	slices.SortFunc(pairs, func(left, right mapEntry) int {
		return strings.Compare(left.key, right.key)
	})

	children := s.takeChildScratch()
	defer func() { s.putChildScratch(children) }()
	releaseChildren := func() {
		for _, child := range children {
			s.release(child)
		}
	}
	for _, pair := range pairs {
		keyRef, err := s.internString(mmdbtype.String(pair.key))
		if err != nil {
			releaseChildren()
			return nilValueRef, err
		}
		children = append(children, keyRef)
		if pair.child == nil {
			releaseChildren()
			return nilValueRef, fmt.Errorf("map key %q has a nil value", pair.key)
		}
		childRef, err := s.intern(pair.child)
		if err != nil {
			releaseChildren()
			return nilValueRef, fmt.Errorf("interning value for map key %q: %w", pair.key, err)
		}
		children = append(children, childRef)
	}
	return s.internOwnedChildren(valueKindMap, children)
}

func (s *valueStore) internSlice(value mmdbtype.Slice) (valueRef, error) {
	children := s.takeChildScratch()
	defer func() { s.putChildScratch(children) }()
	for index, child := range value {
		if child == nil {
			for _, ref := range children {
				s.release(ref)
			}
			return nilValueRef, fmt.Errorf("slice index %d has a nil value", index)
		}
		ref, err := s.intern(child)
		if err != nil {
			for _, ref := range children {
				s.release(ref)
			}
			return nilValueRef, fmt.Errorf("interning slice index %d: %w", index, err)
		}
		children = append(children, ref)
	}
	return s.internOwnedChildren(valueKindSlice, children)
}

// internOwnedChildren consumes one reference to every child, whether it
// creates a node or finds an existing canonical node.
func (s *valueStore) internOwnedChildren(
	kind valueKind,
	children []valueRef,
) (valueRef, error) {
	ref, created, err := s.internNode(kind, nil, children)
	if err != nil {
		for _, child := range children {
			s.release(child)
		}
		return nilValueRef, err
	}
	// internNode retains children only when it creates a new parent. A dedupe
	// hit consumes the temporary tree by releasing it here.
	if !created {
		for _, child := range children {
			s.release(child)
		}
	}
	return ref, nil
}

func (s *valueStore) internNode(
	kind valueKind,
	payload []byte,
	children []valueRef,
) (valueRef, bool, error) {
	hash := s.hashNode(kind, payload, children)
	for ref := s.buckets[hash]; ref != nilValueRef; ref = s.nodes[ref].nextInBucket {
		node := &s.nodes[ref]
		if node.kind == kind &&
			bytes.Equal(s.payload(node), payload) &&
			slices.Equal(s.childRefs(node), children) {
			s.retain(ref)
			return ref, false, nil
		}
	}

	payloadOffset, err := s.payloads.put(payload)
	if err != nil {
		return nilValueRef, false, err
	}
	childrenOffset, err := s.children.put(children)
	if err != nil {
		s.payloads.release(
			payloadOffset,
			uint32(len(payload)), //nolint:gosec // payload arena accepted this length
		)
		return nilValueRef, false, err
	}

	var ref valueRef
	if len(s.freeRefs) != 0 {
		ref = s.freeRefs[len(s.freeRefs)-1]
		s.freeRefs = s.freeRefs[:len(s.freeRefs)-1]
	} else {
		if uint64(len(s.nodes)) > math.MaxUint32 {
			s.payloads.release(
				payloadOffset,
				uint32(len(payload)), //nolint:gosec // payload arena accepted this length
			)
			s.children.release(
				childrenOffset,
				uint32(len(children)), //nolint:gosec // child arena accepted this length
			)
			return nilValueRef, false, errors.New("value store contains too many live nodes")
		}
		ref = valueRef(len(s.nodes)) //nolint:gosec // node count was bounded above
		s.nodes = append(s.nodes, valueNode{})
	}
	s.nodes[ref] = valueNode{
		hash:           hash,
		payloadOffset:  payloadOffset,
		payloadLen:     uint32(len(payload)), //nolint:gosec // payload arena accepted this length
		childrenOffset: childrenOffset,
		childrenLen:    uint32(len(children)), //nolint:gosec // child arena accepted this length
		refCount:       1,
		nextInBucket:   s.buckets[hash],
		kind:           kind,
	}
	s.buckets[hash] = ref
	return ref, true, nil
}

func (s *valueStore) hashNode(kind valueKind, payload []byte, children []valueRef) uint64 {
	required := 1 + len(payload) + 8*len(children)
	if cap(s.hashScratch) < required {
		s.hashScratch = make([]byte, required)
	} else {
		s.hashScratch = s.hashScratch[:required]
	}
	s.hashScratch[0] = byte(kind)
	copy(s.hashScratch[1:], payload)
	offset := 1 + len(payload)
	for _, child := range children {
		binary.LittleEndian.PutUint64(s.hashScratch[offset:], s.node(child).hash)
		offset += 8
	}
	return s.hashFunc(s.hashScratch)
}

func (s *valueStore) rememberCallerIdentity(value mmdbtype.DataType, ref valueRef) {
	identity, ok := dataIdentity(value)
	if !ok || ref == nilValueRef {
		return
	}
	if _, storeOwned := s.materializedByIdentity[identity]; storeOwned {
		return
	}
	if _, exists := s.callerByIdentity[identity]; exists {
		return
	}
	if s.callerIdentityLimit <= 0 {
		return
	}
	entry := callerIdentityEntry{key: identity, value: value, ref: ref, prev: -1, next: -1}
	s.retain(ref)
	if len(s.callerIdentity) < s.callerIdentityLimit {
		index := len(s.callerIdentity)
		s.callerIdentity = append(s.callerIdentity, entry)
		s.callerByIdentity[identity] = index
		s.linkCallerIdentityHead(index)
		return
	}
	index := s.callerIdentityTail
	old := s.callerIdentity[index]
	delete(s.callerByIdentity, old.key)
	s.unlinkCallerIdentity(index)
	s.callerIdentity[index] = entry
	s.callerByIdentity[identity] = index
	s.linkCallerIdentityHead(index)
	s.release(old.ref)
}

func (s *valueStore) touchCallerIdentity(index int) {
	if index == s.callerIdentityHead {
		return
	}
	s.unlinkCallerIdentity(index)
	s.linkCallerIdentityHead(index)
}

func (s *valueStore) unlinkCallerIdentity(index int) {
	entry := &s.callerIdentity[index]
	if entry.prev >= 0 {
		s.callerIdentity[entry.prev].next = entry.next
	} else {
		s.callerIdentityHead = entry.next
	}
	if entry.next >= 0 {
		s.callerIdentity[entry.next].prev = entry.prev
	} else {
		s.callerIdentityTail = entry.prev
	}
	entry.prev = -1
	entry.next = -1
}

func (s *valueStore) linkCallerIdentityHead(index int) {
	entry := &s.callerIdentity[index]
	entry.prev = -1
	entry.next = s.callerIdentityHead
	if s.callerIdentityHead >= 0 {
		s.callerIdentity[s.callerIdentityHead].prev = index
	} else {
		s.callerIdentityTail = index
	}
	s.callerIdentityHead = index
}

func kindOf(value mmdbtype.DataType) (valueKind, error) {
	switch value := value.(type) {
	case mmdbtype.Bool:
		return valueKindBool, nil
	case mmdbtype.Bytes:
		return valueKindBytes, nil
	case mmdbtype.Float32:
		return valueKindFloat32, nil
	case mmdbtype.Float64:
		return valueKindFloat64, nil
	case mmdbtype.Int32:
		return valueKindInt32, nil
	case mmdbtype.String:
		return valueKindString, nil
	case mmdbtype.Uint16:
		return valueKindUint16, nil
	case mmdbtype.Uint32:
		return valueKindUint32, nil
	case mmdbtype.Uint64:
		return valueKindUint64, nil
	case *mmdbtype.Uint128:
		// dereferenceDataType leaves *Uint128 alone, since only its pointer
		// form implements DataType, so the nil check happens here.
		if value == nil {
			return valueKindInvalid, errors.New("cannot intern a nil *mmdbtype.Uint128")
		}
		// The wire encoding holds only the magnitude. A negative value would
		// intern to the same node as its absolute value, and a wider value
		// would produce output that readers reject.
		integer := (*big.Int)(value)
		if integer.Sign() < 0 {
			return valueKindInvalid, errors.New("cannot intern a negative *mmdbtype.Uint128")
		}
		if integer.BitLen() > 128 {
			return valueKindInvalid, errors.New(
				"cannot intern a *mmdbtype.Uint128 wider than 128 bits")
		}
		return valueKindUint128, nil
	default:
		return valueKindInvalid, fmt.Errorf("unsupported MMDB data type %T", value)
	}
}

func (s *valueStore) materialize(ref valueRef) mmdbtype.DataType {
	if ref == nilValueRef {
		return nil
	}
	node := s.node(ref)
	if node.materialized != nil {
		return node.materialized
	}

	var value mmdbtype.DataType
	switch node.kind {
	case valueKindMap:
		children := s.childRefs(node)
		valueMap := make(mmdbtype.Map, len(children)/2)
		for index := 0; index < len(children); index += 2 {
			key := s.materialize(children[index]).(mmdbtype.String)
			valueMap[key] = s.materialize(children[index+1])
		}
		value = valueMap
	case valueKindSlice:
		children := s.childRefs(node)
		valueSlice := make(mmdbtype.Slice, len(children))
		for index, child := range children {
			valueSlice[index] = s.materialize(child)
		}
		value = valueSlice
	default:
		value = materializeScalar(node.kind, s.payload(node))
	}
	node.materialized = value
	if identity, ok := dataIdentity(value); ok {
		node.identity = identity
		node.hasIdentity = true
		s.materializedByIdentity[identity] = ref
	}
	return value
}

func materializeScalar(kind valueKind, encoded []byte) mmdbtype.DataType {
	if kind == valueKindBool {
		return mmdbtype.Bool(encoded[0]&0x1f != 0)
	}
	payload := scalarPayload(encoded)
	switch kind {
	case valueKindBytes:
		return mmdbtype.Bytes(bytes.Clone(payload))
	case valueKindString:
		return mmdbtype.String(payload)
	case valueKindFloat32:
		return mmdbtype.Float32(math.Float32frombits(binary.BigEndian.Uint32(payload)))
	case valueKindFloat64:
		return mmdbtype.Float64(math.Float64frombits(binary.BigEndian.Uint64(payload)))
	case valueKindInt32:
		var raw uint32
		for _, b := range payload {
			raw = raw<<8 | uint32(b)
		}
		return mmdbtype.Int32(int32(raw))
	case valueKindUint16:
		var raw uint16
		for _, b := range payload {
			raw = raw<<8 | uint16(b)
		}
		return mmdbtype.Uint16(raw)
	case valueKindUint32:
		var raw uint32
		for _, b := range payload {
			raw = raw<<8 | uint32(b)
		}
		return mmdbtype.Uint32(raw)
	case valueKindUint64:
		var raw uint64
		for _, b := range payload {
			raw = raw<<8 | uint64(b)
		}
		return mmdbtype.Uint64(raw)
	case valueKindUint128:
		integer := new(big.Int).SetBytes(payload)
		value := mmdbtype.Uint128(*integer)
		return &value
	default:
		panic(fmt.Sprintf("cannot materialize scalar kind %d", kind))
	}
}

func scalarPayload(encoded []byte) []byte {
	if len(encoded) == 0 {
		panic("empty scalar encoding")
	}
	index := 1
	if encoded[0]>>5 == 0 {
		index++ // extended type byte
	}
	size := int(encoded[0] & 0x1f)
	switch size {
	case 29:
		size = 29 + int(encoded[index])
		index++
	case 30:
		size = 285 + int(binary.BigEndian.Uint16(encoded[index:index+2]))
		index += 2
	case 31:
		size = 65821 + int(encoded[index])<<16 + int(encoded[index+1])<<8 + int(encoded[index+2])
		index += 3
	}
	return encoded[index : index+size]
}

// scalarWriter implements mmdbtype's private writer interface without hashing
// or pointer generation. Every intern attempt encodes its scalar, but the
// store retains only one final encoding per canonical node.
type scalarWriter struct{ bytes.Buffer }

func (w *scalarWriter) WriteOrWritePointer(value mmdbtype.DataType) (int64, error) {
	return value.WriteTo(w)
}

func (w *scalarWriter) WriteOrWritePointerString(value mmdbtype.String) (int64, error) {
	return value.WriteTo(w)
}
