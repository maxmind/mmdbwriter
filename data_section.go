package mmdbwriter

import (
	"bytes"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

type writtenType struct {
	pointer mmdbtype.Pointer
	size    int64
}

type dataWriter struct {
	*bytes.Buffer
	pointers  map[string]writtenType
	keyWriter *keyWriter
}

func newDataWriter() *dataWriter {
	return &dataWriter{
		Buffer:    &bytes.Buffer{},
		pointers:  map[string]writtenType{},
		keyWriter: newKeyWriter(),
	}
}

func (dw *dataWriter) maybeWrite(t mmdbtype.DataType) (int, error) {
	key, err := dw.keyWriter.key(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.pointers[string(key)]
	if ok {
		return int(written.pointer), nil
	}
	// We can't use the pointers[string(key)] optimization below
	// as the backing buffer for key may change when we call
	// t.WriteTo. That said, this is the less common code path
	// so it doesn't matter too much.
	keyStr := string(key)

	offset := dw.Len()
	size, err := t.WriteTo(dw)
	if err != nil {
		return 0, err
	}

	written = writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	}

	dw.pointers[keyStr] = written

	return int(written.pointer), nil
}

func (dw *dataWriter) WriteOrWritePointer(t mmdbtype.DataType) (int64, error) {
	key, err := dw.keyWriter.key(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.pointers[string(key)]
	if ok && written.size > written.pointer.WrittenSize() {
		// Only use a pointer if it would take less space than writing the
		// type again.
		return written.pointer.WriteTo(dw)
	}
	// We can't use the pointers[string(key)] optimization below
	// as the backing buffer for key may change when we call
	// t.WriteTo. That said, this is the less common code path
	// so it doesn't matter too much.
	keyStr := string(key)

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

	dw.pointers[keyStr] = writtenType{
		pointer: mmdbtype.Pointer(offset),
		size:    size,
	}
	return size, nil
}
