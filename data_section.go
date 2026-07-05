package mmdbwriter

import (
	"bytes"
	"fmt"
	"math"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

type writtenType struct {
	pointer mmdbtype.Pointer
	size    int64
}

type dataWriter struct {
	*bytes.Buffer

	dataMap     *dataMap
	offsets     map[dataMapKey]writtenType
	keyWriter   *keyWriter
	usePointers bool
}

func newDataWriter(dataMap *dataMap, usePointers bool) *dataWriter {
	return &dataWriter{
		Buffer:      &bytes.Buffer{},
		dataMap:     dataMap,
		offsets:     map[dataMapKey]writtenType{},
		keyWriter:   newKeyWriter(),
		usePointers: usePointers,
	}
}

func (dw *dataWriter) maybeWrite(value *dataMapValue) (int, error) {
	written, ok := dw.offsets[value.key]
	if ok {
		return int(written.pointer), nil
	}

	offset := dw.Len()
	size, err := value.data.WriteTo(dw)
	if err != nil {
		return 0, err
	}

	if offset > math.MaxUint32 {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}

	//nolint:gosec // we check for overflow above
	written = writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	}

	dw.offsets[value.key] = written

	return int(written.pointer), nil
}

func (dw *dataWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	if !dw.usePointers {
		return t.WriteTo(dw)
	}

	keyBytes, err := dw.keyWriter.Key(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.offsets[dataMapKey(keyBytes)]
	if ok && written.size > written.pointer.WrittenSize() {
		// Only use a pointer if it would take less space than writing the
		// type again.
		return written.pointer.WriteTo(dw)
	}
	// We can't use the pointers[dataMapKey(keyBytes)] optimization to
	// avoid an allocation below as the backing buffer for key may change when
	// we call t.WriteTo. That said, this is the less common code path
	// so it doesn't matter too much.
	key := dataMapKey(keyBytes)

	// TODO: A possible optimization here for simple types would be to just
	// write key to the dataWriter. This won't necessarily work for Map and
	// Slice though as they may have internal pointers missing from key.
	// I briefly tested this and didn't see much difference, but it might
	// be worth exploring more.
	offset := dw.Len()
	size, err := t.WriteTo(dw)
	if err != nil || ok {
		return size, err
	}

	if offset > math.MaxUint32 {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}

	//nolint:gosec // we check for overflow above
	dw.offsets[key] = writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	}
	return size, nil
}

func (dw *dataWriter) WriteOrWritePointerString(t mmdbtype.String) (int64, error) {
	if !dw.usePointers {
		return t.WriteTo(dw)
	}

	// This mirrors WriteOrWritePointer but accepts a concrete String to avoid
	// boxing map keys into DataType while writing sorted maps.
	keyBytes, err := dw.keyWriter.KeyString(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.offsets[dataMapKey(keyBytes)]
	if ok && written.size > written.pointer.WrittenSize() {
		// Only use a pointer if it would take less space than writing the
		// type again.
		return written.pointer.WriteTo(dw)
	}

	// Take a stable copy of the key before storing it; the key writer reuses
	// its buffer on the next key generation.
	key := dataMapKey(keyBytes)
	offset := dw.Len()
	size, err := t.WriteTo(dw)
	if err != nil || ok {
		return size, err
	}

	if offset > math.MaxUint32 {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}

	//nolint:gosec // we check for overflow above
	dw.offsets[key] = writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	}
	return size, nil
}
