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
	data    mmdbtype.DataType
	written writtenType
	next    int
}

type dataWriter struct {
	*bytes.Buffer

	dataMap     *dataMap
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

func (dw *dataWriter) findOffset(
	hash dataMapHash,
	data mmdbtype.DataType,
) (writtenType, bool) {
	index, ok := dw.offsets[hash]
	// next == -1 terminates a chain; every nonnegative next is a live arena index.
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

func (dw *dataWriter) rememberOffset(
	hash dataMapHash,
	data mmdbtype.DataType,
	written writtenType,
) {
	dw.appendOffset(hash, dataOffset{
		data:    data,
		written: written,
	})
}

func (dw *dataWriter) appendOffset(hash dataMapHash, entry dataOffset) {
	entry.next = -1
	if index, ok := dw.offsets[hash]; ok {
		entry.next = index
	}
	dw.offsetArena = append(dw.offsetArena, entry)
	dw.offsets[hash] = len(dw.offsetArena) - 1
}

func (offset *dataOffset) matches(data mmdbtype.DataType) bool {
	return wireDataEqual(offset.data, data)
}
