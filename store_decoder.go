package mmdbwriter

import (
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// storeDecoder implements mmdbdata.CursorUnmarshaler by interning directly
// into a valueStore. It never constructs an intermediate map or slice graph.
// The offset cache owns one reference per decoded MMDB offset until close runs.
type storeDecoder struct {
	store  *valueStore
	cache  map[uint]valueRef
	result valueRef
	// pairScratch pools the per-map working slices. Maps nest, so each
	// decodeMap call takes a slice and returns it when done.
	pairScratch [][]decodedPair
}

var _ mmdbdata.CursorUnmarshaler = (*storeDecoder)(nil)

// decodedPair carries one interned key and value of a map being decoded.
type decodedPair struct {
	key      string
	keyRef   valueRef
	valueRef valueRef
}

func (d *storeDecoder) takePairScratch() []decodedPair {
	if n := len(d.pairScratch); n != 0 {
		pairs := d.pairScratch[n-1]
		d.pairScratch = d.pairScratch[:n-1]
		return pairs[:0]
	}
	return nil
}

func (d *storeDecoder) putPairScratch(pairs []decodedPair) {
	// Entries reference decoded keys. Clear the full backing array so pooling
	// does not pin them.
	pairs = pairs[:cap(pairs)]
	clear(pairs)
	d.pairScratch = append(d.pairScratch, pairs)
}

func newStoreDecoder(store *valueStore) *storeDecoder {
	return &storeDecoder{store: store, cache: map[uint]valueRef{}}
}

func (d *storeDecoder) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	ref, next, err := d.decodeRef(cursor)
	if err != nil {
		return mmdbdata.Cursor{}, err
	}
	// Release a result the caller never took, so a repeated Decode does not
	// leak its reference.
	d.store.release(d.result)
	d.result = ref
	return next, nil
}

// takeResult transfers ownership of the most recently decoded top-level ref.
func (d *storeDecoder) takeResult() valueRef {
	ref := d.result
	d.result = nilValueRef
	return ref
}

func (d *storeDecoder) close() {
	if d.result != nilValueRef {
		d.store.release(d.result)
		d.result = nilValueRef
	}
	for _, ref := range d.cache {
		d.store.release(ref)
	}
	clear(d.cache)
}

func (d *storeDecoder) decodeRef(
	cursor mmdbdata.Cursor,
) (valueRef, mmdbdata.Cursor, error) {
	offset := cursor.Offset()
	if ref, ok := d.cache[offset]; ok {
		next, err := cursor.Skip()
		if err != nil {
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"skipping cached value at offset %d: %w", offset, err)
		}
		d.store.retain(ref)
		return ref, next, nil
	}

	kind, err := cursor.Kind()
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("peeking kind: %w", err)
	}

	var ref valueRef
	var next mmdbdata.Cursor
	switch kind {
	case mmdbdata.KindMap:
		ref, next, err = d.decodeMap(cursor)
	case mmdbdata.KindSlice:
		ref, next, err = d.decodeSlice(cursor)
	case mmdbdata.KindString:
		var value string
		value, next, err = cursor.ReadString()
		if err == nil {
			ref, err = d.store.internString(mmdbtype.String(value))
		}
	case mmdbdata.KindFloat64:
		var value float64
		value, next, err = cursor.ReadFloat64()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Float64(value))
		}
	case mmdbdata.KindBytes:
		var value []byte
		value, next, err = cursor.ReadBytes()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Bytes(value))
		}
	case mmdbdata.KindUint16:
		var value uint64
		value, next, err = cursor.ReadUint()
		if err == nil {
			// #nosec G115 -- kind is Uint16.
			ref, err = d.store.internUncached(mmdbtype.Uint16(value))
		}
	case mmdbdata.KindUint32:
		var value uint64
		value, next, err = cursor.ReadUint()
		if err == nil {
			// #nosec G115 -- kind is Uint32.
			ref, err = d.store.internUncached(mmdbtype.Uint32(value))
		}
	case mmdbdata.KindInt32:
		var value int32
		value, next, err = cursor.ReadInt32()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Int32(value))
		}
	case mmdbdata.KindUint64:
		var value uint64
		value, next, err = cursor.ReadUint()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Uint64(value))
		}
	case mmdbdata.KindUint128:
		var hi, lo uint64
		hi, lo, next, err = cursor.ReadUint128()
		if err == nil {
			integer := new(big.Int).SetUint64(hi)
			integer.Lsh(integer, 64)
			integer.Add(integer, new(big.Int).SetUint64(lo))
			value := mmdbtype.Uint128(*integer)
			ref, err = d.store.internUncached(&value)
		}
	case mmdbdata.KindBool:
		var value bool
		value, next, err = cursor.ReadBool()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Bool(value))
		}
	case mmdbdata.KindFloat32:
		var value float32
		value, next, err = cursor.ReadFloat32()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Float32(value))
		}
	default:
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
			"unsupported data type %v at offset %d", kind, offset)
	}
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
			"decoding %v at offset %d: %w", kind, offset, err)
	}

	// The returned ref and the cache each own one reference.
	d.store.retain(ref)
	d.cache[offset] = ref
	return ref, next, nil
}

func (d *storeDecoder) decodeMap(
	cursor mmdbdata.Cursor,
) (valueRef, mmdbdata.Cursor, error) {
	entries, err := cursor.Map()
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("reading map: %w", err)
	}
	pairs := d.takePairScratch()
	defer func() { d.putPairScratch(pairs) }()
	release := func() {
		for _, pair := range pairs {
			d.store.release(pair.keyRef)
			d.store.release(pair.valueRef)
		}
	}
	var next mmdbdata.Cursor
	for {
		key, valueCursor, ok := entries.Next(next)
		if !ok {
			break
		}
		keyRef, keyErr := d.store.internString(mmdbtype.String(key))
		if keyErr != nil {
			release()
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"interning map key %q: %w", key, keyErr)
		}
		childRef, valueNext, valueErr := d.decodeRef(valueCursor)
		if valueErr != nil {
			d.store.release(keyRef)
			release()
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"decoding value for map key %q: %w", key, valueErr)
		}
		next = valueNext
		pairs = append(pairs, decodedPair{key: string(key), keyRef: keyRef, valueRef: childRef})
	}
	next, err = entries.End()
	if err != nil {
		release()
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("reading map entry: %w", err)
	}
	// The sort must stay byte-order identical to internMap's, or loaded and
	// inserted maps stop deduplicating against each other.
	slices.SortFunc(pairs, func(left, right decodedPair) int {
		return strings.Compare(left.key, right.key)
	})
	for index := 1; index < len(pairs); index++ {
		if pairs[index].key == pairs[index-1].key {
			key := pairs[index].key
			release()
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("map has duplicate key %q", key)
		}
	}
	children := d.store.takeChildScratch()
	defer func() { d.store.putChildScratch(children) }()
	for _, pair := range pairs {
		children = append(children, pair.keyRef, pair.valueRef)
	}
	ref, err := d.store.internOwnedChildren(valueKindMap, children)
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, err
	}
	return ref, next, nil
}

func (d *storeDecoder) decodeSlice(
	cursor mmdbdata.Cursor,
) (valueRef, mmdbdata.Cursor, error) {
	values, err := cursor.Slice()
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("reading slice: %w", err)
	}
	// The declared size comes from the source database header, so grow into
	// the pooled slice instead of trusting the header for one allocation.
	children := d.store.takeChildScratch()
	defer func() { d.store.putChildScratch(children) }()
	var next mmdbdata.Cursor
	for {
		index, valueCursor, ok := values.Next(next)
		if !ok {
			break
		}
		ref, valueNext, valueErr := d.decodeRef(valueCursor)
		if valueErr != nil {
			for _, child := range children {
				d.store.release(child)
			}
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"decoding slice index %d: %w", index, valueErr)
		}
		next = valueNext
		children = append(children, ref)
	}
	next, err = values.End()
	if err != nil {
		for _, child := range children {
			d.store.release(child)
		}
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("reading slice element: %w", err)
	}
	ref, err := d.store.internOwnedChildren(valueKindSlice, children)
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, err
	}
	return ref, next, nil
}
