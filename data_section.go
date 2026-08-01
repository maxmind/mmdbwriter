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

type dataOffset struct {
	data        mmdbtype.DataType
	stringValue mmdbtype.String
	written     writtenType
	next        int
	isString    bool
}

type dataWriter struct {
	*bytes.Buffer

	dataMap     *dataMap
	offsets     map[dataMapHash]int
	offsetArena []dataOffset
	usePointers bool
}

func newDataWriter(dataMap *dataMap, usePointers bool) *dataWriter {
	return newDataWriterWithCapacity(dataMap, usePointers, 0)
}

func newDataWriterWithCapacity(
	dataMap *dataMap,
	usePointers bool,
	offsetCapacity int,
) *dataWriter {
	return &dataWriter{
		Buffer:      &bytes.Buffer{},
		dataMap:     dataMap,
		offsets:     make(map[dataMapHash]int, offsetCapacity),
		offsetArena: make([]dataOffset, 0, offsetCapacity),
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

	hash, err := dw.dataMap.hasher.Hash(t)
	if err != nil {
		return 0, err
	}

	dmHash := dataMapHash(hash)
	written, ok := dw.findOffset(dmHash, t)
	if ok && written.size > written.pointer.WrittenSize() {
		// Only use a pointer if it would take less space than writing the
		// type again.
		return written.pointer.WriteTo(dw)
	}

	// TODO: A possible optimization here for simple types would be to just
	// write the type to the dataWriter's buffer from a compact representation.
	// This is less straightforward for Map and Slice as they may contain
	// internal pointers.
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
	dw.rememberOffset(dmHash, t, writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	})
	return size, nil
}

func (dw *dataWriter) WriteOrWritePointerString(t mmdbtype.String) (int64, error) {
	if !dw.usePointers {
		return t.WriteTo(dw)
	}

	// This mirrors WriteOrWritePointer but accepts a concrete String so hashing
	// map keys does not need the general top-level hash dispatch.
	hash, err := dw.dataMap.hasher.hashValue(t)
	if err != nil {
		return 0, err
	}

	dmHash := dataMapHash(hash)
	written, ok := dw.findStringOffset(dmHash, t)
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
	dw.rememberStringOffset(dmHash, t, writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	})
	return size, nil
}

func (dw *dataWriter) findOffset(
	hash dataMapHash,
	data mmdbtype.DataType,
) (writtenType, bool) {
	index, ok := dw.offsets[hash]
	for ok {
		offset := &dw.offsetArena[index]
		if offset.matches(data) {
			return offset.written, true
		}
		index = offset.next
		ok = index >= 0
	}
	return writtenType{}, false
}

func (dw *dataWriter) findStringOffset(
	hash dataMapHash,
	data mmdbtype.String,
) (writtenType, bool) {
	index, ok := dw.offsets[hash]
	for ok {
		offset := &dw.offsetArena[index]
		if offset.matchesString(data) {
			return offset.written, true
		}
		index = offset.next
		ok = index >= 0
	}
	return writtenType{}, false
}

func (dw *dataWriter) rememberOffset(
	hash dataMapHash,
	data mmdbtype.DataType,
	written writtenType,
) {
	if normalized, ok := dereferenceDataType(data); ok {
		if stringValue, ok := normalized.(mmdbtype.String); ok {
			dw.rememberStringOffset(hash, stringValue, written)
			return
		}
	}
	next := -1
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

func (dw *dataWriter) rememberStringOffset(
	hash dataMapHash,
	data mmdbtype.String,
	written writtenType,
) {
	next := -1
	if index, ok := dw.offsets[hash]; ok {
		next = index
	}
	dw.offsetArena = append(dw.offsetArena, dataOffset{
		stringValue: data,
		written:     written,
		next:        next,
		isString:    true,
	})
	dw.offsets[hash] = len(dw.offsetArena) - 1
}

func (offset *dataOffset) matches(data mmdbtype.DataType) bool {
	if !offset.isString {
		return wireDataEqual(offset.data, data)
	}
	normalized, ok := dereferenceDataType(data)
	if !ok {
		return false
	}
	stringValue, ok := normalized.(mmdbtype.String)
	return ok && offset.stringValue == stringValue
}

func (offset *dataOffset) matchesString(data mmdbtype.String) bool {
	if offset.isString {
		return offset.stringValue == data
	}
	normalized, ok := dereferenceDataType(offset.data)
	if !ok {
		return false
	}
	stringValue, ok := normalized.(mmdbtype.String)
	return ok && stringValue == data
}
