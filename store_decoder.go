package mmdbwriter

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/oschwald/maxminddb-golang/v2"
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
	// record decodes search-tree records for decodeTopLevel. It is a field so
	// that passing it to Decode does not allocate.
	record recordDecoder
	// pairScratch pools the per-map working slices. Maps nest, so each
	// decodeMap call takes a slice and returns it when done.
	pairScratch [][]decodedPair
}

var (
	_ mmdbdata.CursorUnmarshaler = (*storeDecoder)(nil)
	_ mmdbdata.CursorUnmarshaler = (*recordDecoder)(nil)
)

// recordDecoder decodes the search-tree record at offset into its decoder.
// decodeRecord must only see an offset that is not in the cache.
type recordDecoder struct {
	decoder *storeDecoder
	offset  uint
}

func (r *recordDecoder) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	ref, next, err := r.decoder.decodeRecord(cursor, r.offset)
	if err != nil {
		return mmdbdata.Cursor{}, err
	}
	r.decoder.setResult(ref)
	return next, nil
}

// decodedPair carries one interned key and value of a map being decoded.
type decodedPair struct {
	// key borrows the source buffer through sorting and duplicate detection.
	// putPairScratch clears it when decodeMap returns so the pool does not retain it.
	key      []byte
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
	d := &storeDecoder{store: store, cache: map[uint]valueRef{}}
	d.record.decoder = d
	return d
}

func (d *storeDecoder) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	ref, next, err := d.decodeRef(cursor)
	if err != nil {
		return mmdbdata.Cursor{}, err
	}
	d.setResult(ref)
	return next, nil
}

// setResult stores ref as the top-level result. It releases a result the
// caller never took, so a repeated Decode does not leak its reference.
func (d *storeDecoder) setResult(ref valueRef) {
	d.store.release(d.result)
	d.result = ref
}

// takeResult transfers ownership of the most recently decoded top-level ref.
func (d *storeDecoder) takeResult() valueRef {
	ref := d.result
	d.result = nilValueRef
	return ref
}

// cachedTopLevel returns a new reference to the value decoded at offset, if
// the cache has one. A top-level result needs no successor cursor, so this
// skips the Skip that a cache hit in decodeRef does for an inline container.
func (d *storeDecoder) cachedTopLevel(offset uint) (valueRef, bool) {
	ref, ok := d.cache[offset]
	if ok {
		d.store.retain(ref)
	}
	return ref, ok
}

// decodeTopLevel decodes the record for res after cachedTopLevel missed its
// offset, and returns a reference that the caller owns.
func (d *storeDecoder) decodeTopLevel(res maxminddb.Result) (valueRef, error) {
	offset := uint(res.Offset())
	d.record.offset = offset
	if err := res.Decode(&d.record); err != nil {
		return nilValueRef, fmt.Errorf("decoding record at offset %d: %w", offset, err)
	}
	return d.takeResult(), nil
}

// decodeRecord decodes a search-tree record at recordOffset that is not in
// the cache.
func (d *storeDecoder) decodeRecord(
	cursor mmdbdata.Cursor,
	recordOffset uint,
) (valueRef, mmdbdata.Cursor, error) {
	offset, err := cursor.Offset()
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("resolving offset: %w", err)
	}
	if offset == recordOffset {
		// cachedTopLevel already missed this offset.
		return d.decodeUncached(cursor, offset)
	}
	// The record points at a pointer, as the Perl writer can write. Cache
	// the value under the record offset too, so the next record with this
	// offset hits cachedTopLevel.
	ref, next, err := d.decodeCached(cursor, offset)
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, err
	}
	d.store.retain(ref)
	d.cache[recordOffset] = ref
	return ref, next, nil
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
	offset, err := cursor.Offset()
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("resolving offset: %w", err)
	}
	return d.decodeCached(cursor, offset)
}

// decodeCached returns the cached value at the resolved offset, or decodes it.
func (d *storeDecoder) decodeCached(
	cursor mmdbdata.Cursor,
	offset uint,
) (valueRef, mmdbdata.Cursor, error) {
	if ref, ok := d.cache[offset]; ok {
		next, skipErr := cursor.Skip()
		if skipErr != nil {
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"skipping cached value at offset %d: %w", offset, skipErr,
			)
		}
		d.store.retain(ref)
		return ref, next, nil
	}
	return d.decodeUncached(cursor, offset)
}

// decodeUncached decodes the value at the resolved offset and caches it. The
// cache must not hold offset yet.
func (d *storeDecoder) decodeUncached(
	cursor mmdbdata.Cursor,
	offset uint,
) (valueRef, mmdbdata.Cursor, error) {
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
			ref, err = d.store.internScalar(mmdbtype.String(value))
		}
	case mmdbdata.KindFloat64:
		var value float64
		value, next, err = cursor.ReadFloat64()
		if err == nil {
			ref, err = d.store.internScalar(mmdbtype.Float64(value))
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
			//nolint:gosec // ReadUint widens a validated uint16 value.
			ref, err = d.store.internScalar(mmdbtype.Uint16(value))
		}
	case mmdbdata.KindUint32:
		var value uint64
		value, next, err = cursor.ReadUint()
		if err == nil {
			//nolint:gosec // ReadUint widens a validated uint32 value.
			ref, err = d.store.internScalar(mmdbtype.Uint32(value))
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
			ref, err = d.store.internScalar(mmdbtype.Uint128{High: hi, Low: lo})
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
			"unsupported data type %v at offset %d", kind, offset,
		)
	}
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
			"decoding %v at offset %d: %w", kind, offset, err,
		)
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
		keyRef, keyErr := d.store.internStringBytes(key)
		if keyErr != nil {
			release()
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"interning map key %q: %w", key, keyErr,
			)
		}
		childRef, valueNext, valueErr := d.decodeRef(valueCursor)
		if valueErr != nil {
			d.store.release(keyRef)
			release()
			return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf(
				"decoding value for map key %q: %w", key, valueErr,
			)
		}
		next = valueNext
		pairs = append(pairs, decodedPair{key: key, keyRef: keyRef, valueRef: childRef})
	}
	next, err = entries.End()
	if err != nil {
		release()
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("reading map entry: %w", err)
	}
	// The sort must stay byte-order identical to internMap's, or loaded and
	// inserted maps stop deduplicating against each other.
	slices.SortFunc(pairs, func(left, right decodedPair) int {
		return bytes.Compare(left.key, right.key)
	})
	for index := 1; index < len(pairs); index++ {
		if bytes.Equal(pairs[index].key, pairs[index-1].key) {
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
				"decoding slice index %d: %w", index, valueErr,
			)
		}
		next = valueNext
		children = append(children, ref)
	}
	next, err = values.End()
	if err != nil {
		for _, child := range children {
			d.store.release(child)
		}
		return nilValueRef, mmdbdata.Cursor{}, fmt.Errorf("finishing slice: %w", err)
	}
	ref, err := d.store.internOwnedChildren(valueKindSlice, children)
	if err != nil {
		return nilValueRef, mmdbdata.Cursor{}, err
	}
	return ref, next, nil
}
