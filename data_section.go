package mmdbwriter

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type writtenType struct {
	pointer pointer
	size    int64
}

type dataWriter struct {
	*bytes.Buffer
	pointers map[string]writtenType
}

func newDataWriter() *dataWriter {
	return &dataWriter{
		Buffer:   &bytes.Buffer{},
		pointers: map[string]writtenType{},
	}
}

func (dw *dataWriter) maybeWrite(t DataType) (int, error) {
	key, err := key(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.pointers[key]
	if ok {
		return int(written.pointer), nil
	}

	offset := dw.Len()
	size, err := t.writeTo(dw)
	if err != nil {
		return 0, err
	}

	written = writtenType{
		pointer: pointer(offset),
		size:    size,
	}

	dw.pointers[key] = written

	return int(written.pointer), nil
}

func (dw *dataWriter) writeOrWritePointer(t DataType) (int64, error) {
	key, err := key(t)
	if err != nil {
		return 0, err
	}

	written, ok := dw.pointers[key]
	if ok && written.size > written.pointer.writtenSize() {
		// Only use a pointer if it would take less space than writing the
		// type again.
		return written.pointer.writeTo(dw)
	}

	offset := dw.Len()
	size, err := t.writeTo(dw)
	if err != nil || ok {
		return size, err
	}

	dw.pointers[key] = writtenType{
		pointer: pointer(offset),
		size:    size,
	}
	return size, nil
}

// This is just a quick hack. I am sure there is
// something better
func key(t DataType) (string, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return "", errors.Wrap(err, "error marshalling to JSON")
	}
	return fmt.Sprintf("%d\x00%s", t.typeNum(), bytes), nil
}
