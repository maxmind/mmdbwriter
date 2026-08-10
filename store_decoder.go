package mmdbwriter

import (
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

// storeDecoder implements mmdbdata.Unmarshaler by interning directly into a
// valueStore. It never constructs an intermediate map or slice graph. The
// offset cache owns one reference per decoded MMDB offset until close runs.
type storeDecoder struct {
	store  *valueStore
	cache  map[uint]valueRef
	result valueRef
	// pairScratch pools the per-map working slices. Maps nest, so each
	// decodeMap call takes a slice and returns it when done.
	pairScratch [][]decodedPair
}

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

func (d *storeDecoder) UnmarshalMaxMindDB(decoder *mmdbdata.Decoder) error {
	ref, err := d.decodeRef(decoder)
	if err != nil {
		return err
	}
	// Release a result the caller never took, so a repeated Decode does not
	// leak its reference.
	d.store.release(d.result)
	d.result = ref
	return nil
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

func (d *storeDecoder) decodeRef(decoder *mmdbdata.Decoder) (valueRef, error) {
	offset := decoder.Offset()
	if ref, ok := d.cache[offset]; ok {
		d.store.retain(ref)
		return ref, nil
	}

	kind, err := decoder.PeekKind()
	if err != nil {
		return nilValueRef, fmt.Errorf("peeking kind: %w", err)
	}

	var ref valueRef
	switch kind {
	case mmdbdata.KindMap:
		ref, err = d.decodeMap(decoder)
	case mmdbdata.KindSlice:
		ref, err = d.decodeSlice(decoder)
	case mmdbdata.KindString:
		var value string
		value, err = decoder.ReadString()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.String(value))
		}
	case mmdbdata.KindFloat64:
		var value float64
		value, err = decoder.ReadFloat64()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Float64(value))
		}
	case mmdbdata.KindBytes:
		var value []byte
		value, err = decoder.ReadBytes()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Bytes(value))
		}
	case mmdbdata.KindUint16:
		var value uint16
		value, err = decoder.ReadUint16()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Uint16(value))
		}
	case mmdbdata.KindUint32:
		var value uint32
		value, err = decoder.ReadUint32()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Uint32(value))
		}
	case mmdbdata.KindInt32:
		var value int32
		value, err = decoder.ReadInt32()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Int32(value))
		}
	case mmdbdata.KindUint64:
		var value uint64
		value, err = decoder.ReadUint64()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Uint64(value))
		}
	case mmdbdata.KindUint128:
		var hi, lo uint64
		hi, lo, err = decoder.ReadUint128()
		if err == nil {
			integer := new(big.Int).SetUint64(hi)
			integer.Lsh(integer, 64)
			integer.Add(integer, new(big.Int).SetUint64(lo))
			value := mmdbtype.Uint128(*integer)
			ref, err = d.store.internUncached(&value)
		}
	case mmdbdata.KindBool:
		var value bool
		value, err = decoder.ReadBool()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Bool(value))
		}
	case mmdbdata.KindFloat32:
		var value float32
		value, err = decoder.ReadFloat32()
		if err == nil {
			ref, err = d.store.internUncached(mmdbtype.Float32(value))
		}
	default:
		return nilValueRef, fmt.Errorf("unsupported data type: %v", kind)
	}
	if err != nil {
		return nilValueRef, err
	}

	// The returned ref and the cache each own one reference.
	d.store.retain(ref)
	d.cache[offset] = ref
	return ref, nil
}

func (d *storeDecoder) decodeMap(decoder *mmdbdata.Decoder) (valueRef, error) {
	iterator, _, err := decoder.ReadMap()
	if err != nil {
		return nilValueRef, fmt.Errorf("reading map: %w", err)
	}
	pairs := d.takePairScratch()
	defer func() { d.putPairScratch(pairs) }()
	release := func() {
		for _, pair := range pairs {
			d.store.release(pair.keyRef)
			d.store.release(pair.valueRef)
		}
	}
	for key, iteratorErr := range iterator {
		if iteratorErr != nil {
			release()
			return nilValueRef, iteratorErr
		}
		keyRef, keyErr := d.store.internUncached(mmdbtype.String(key))
		if keyErr != nil {
			release()
			return nilValueRef, keyErr
		}
		childRef, valueErr := d.decodeRef(decoder)
		if valueErr != nil {
			d.store.release(keyRef)
			release()
			return nilValueRef, valueErr
		}
		pairs = append(pairs, decodedPair{key: string(key), keyRef: keyRef, valueRef: childRef})
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
			return nilValueRef, fmt.Errorf("map has duplicate key %q", key)
		}
	}
	children := make([]valueRef, 0, len(pairs)*2)
	for _, pair := range pairs {
		children = append(children, pair.keyRef, pair.valueRef)
	}
	return d.store.internOwnedChildren(valueKindMap, children)
}

func (d *storeDecoder) decodeSlice(decoder *mmdbdata.Decoder) (valueRef, error) {
	iterator, size, err := decoder.ReadSlice()
	if err != nil {
		return nilValueRef, fmt.Errorf("reading slice: %w", err)
	}
	children := make(
		[]valueRef,
		0,
		int(size), // #nosec G115 -- Decoder container sizes are bounded by the source buffer.
	)
	for iteratorErr := range iterator {
		if iteratorErr != nil {
			for _, child := range children {
				d.store.release(child)
			}
			return nilValueRef, iteratorErr
		}
		ref, valueErr := d.decodeRef(decoder)
		if valueErr != nil {
			for _, child := range children {
				d.store.release(child)
			}
			return nilValueRef, valueErr
		}
		children = append(children, ref)
	}
	return d.store.internOwnedChildren(valueKindSlice, children)
}
