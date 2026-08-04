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

// noOffsetIndex terminates a chain of dataOffset entries. It is not the zero
// value, so a dataOffset must always have next set explicitly; zero is a valid
// arena index.
const noOffsetIndex = -1

type dataOffset struct {
	data    mmdbtype.DataType
	written writtenType
	next    int
}

type dataWriter struct {
	*bytes.Buffer

	dataMap *dataMap
	// offsets is keyed by hashes from dataMap.hasher. maybeWrite reuses the
	// hash already stored on a dataMapValue while WriteOrWritePointer recomputes
	// one, and both probe this map, so the two paths only share pointers because
	// dataWriter borrows the dataMap's hasher rather than making its own. Giving
	// dataWriter a separate hasher would compile and silently stop nested values
	// from reusing top-level offsets.
	offsets     map[dataMapHash]int
	offsetArena []dataOffset
	usePointers bool
}

func newDataWriter(dataMap *dataMap, usePointers bool) *dataWriter {
	return &dataWriter{
		Buffer:      &bytes.Buffer{},
		dataMap:     dataMap,
		offsets:     map[dataMapHash]int{},
		usePointers: usePointers,
	}
}

func (dw *dataWriter) maybeWrite(value *dataMapValue) (int, error) {
	written, ok := dw.findOffset(value.hash, value.data)
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

	dw.rememberOffset(value.hash, value.data, written)

	return int(written.pointer), nil
}

func (dw *dataWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	if !dw.usePointers {
		return t.WriteTo(dw)
	}

	dmHash, err := dw.dataMap.hasher.Hash(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.findOffset(dmHash, t)
	if ok && written.size > written.pointer.WrittenSize() {
		// Only use a pointer if it would take less space than writing the
		// type again.
		return written.pointer.WriteTo(dw)
	}

	offset := dw.Len()
	size, err := t.WriteTo(dw)
	if err != nil || ok {
		return size, err
	}

	if offset > math.MaxUint32 {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}

	//nolint:gosec // we check for overflow above
	dw.rememberOffset(dmHash, t, writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	})
	return size, nil
}

// WriteOrWritePointerString mirrors WriteOrWritePointer for a String, hashing
// and comparing without boxing so a repeated map key costs no allocation. The
// value is boxed only when a new offset is recorded.
func (dw *dataWriter) WriteOrWritePointerString(t mmdbtype.String) (int64, error) {
	if !dw.usePointers {
		return t.WriteTo(dw)
	}

	dmHash := dw.dataMap.hasher.HashString(t)
	written, ok := dw.findOffsetString(dmHash, t)
	if ok && written.size > written.pointer.WrittenSize() {
		return written.pointer.WriteTo(dw)
	}

	offset := dw.Len()
	size, err := t.WriteTo(dw)
	if err != nil || ok {
		return size, err
	}

	if offset > math.MaxUint32 {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}

	//nolint:gosec // we check for overflow above
	dw.rememberOffset(dmHash, t, writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	})
	return size, nil
}

func (dw *dataWriter) findOffsetString(
	hash dataMapHash,
	value mmdbtype.String,
) (writtenType, bool) {
	index, ok := dw.offsets[hash]
	if !ok {
		return writtenType{}, false
	}
	for index != noOffsetIndex {
		offset := &dw.offsetArena[index]
		if offsetMatchesString(offset.data, value) {
			return offset.written, true
		}
		index = offset.next
	}
	return writtenType{}, false
}

// offsetMatchesString reports whether data encodes the same String as value. It
// normalizes pointer forms exactly as wireDataEqual does, since a stored value
// may be one; comparing the query without boxing is the point, and data is
// already an interface.
func offsetMatchesString(data mmdbtype.DataType, value mmdbtype.String) bool {
	data, ok := dereferenceDataType(data)
	if !ok {
		return false
	}
	existing, isString := data.(mmdbtype.String)
	return isString && existing == value
}

func (dw *dataWriter) findOffset(
	hash dataMapHash,
	data mmdbtype.DataType,
) (writtenType, bool) {
	index, ok := dw.offsets[hash]
	if !ok {
		return writtenType{}, false
	}
	for index != noOffsetIndex {
		offset := &dw.offsetArena[index]
		if wireDataEqual(offset.data, data) {
			return offset.written, true
		}
		index = offset.next
	}
	return writtenType{}, false
}

func (dw *dataWriter) rememberOffset(
	hash dataMapHash,
	data mmdbtype.DataType,
	written writtenType,
) {
	next := noOffsetIndex
	if index, ok := dw.offsets[hash]; ok {
		next = index
	}
	dw.offsetArena = append(dw.offsetArena, dataOffset{
		data:    data,
		written: written,
		next:    next,
	})
	dw.offsets[hash] = len(dw.offsetArena) - 1
}
