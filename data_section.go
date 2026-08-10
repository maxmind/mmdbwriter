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
	written bool
}

type dataWriter struct {
	*bytes.Buffer

	store *valueStore
	// offsets is indexed by valueRef. Refs are dense, so a slice replaces the
	// hashing and exact comparison the previous writer needed for every value.
	offsets     []writtenType
	usePointers bool
}

func newDataWriter(store *valueStore, usePointers bool) *dataWriter {
	return &dataWriter{
		Buffer:      &bytes.Buffer{},
		store:       store,
		offsets:     make([]writtenType, len(store.nodes)),
		usePointers: usePointers,
	}
}

// ensureOffset grows the offset table for refs interned after the writer was
// created, such as metadata values.
func (dw *dataWriter) ensureOffset(ref valueRef) {
	if int(ref) < len(dw.offsets) {
		return
	}
	dw.offsets = append(dw.offsets, make([]writtenType, int(ref)-len(dw.offsets)+1)...)
}

func (dw *dataWriter) maybeWrite(ref valueRef) (int, error) {
	dw.ensureOffset(ref)
	if written := dw.offsets[ref]; written.written {
		return int(written.pointer), nil
	}

	offset := dw.Len()
	size, err := dw.writeValue(ref)
	if err != nil {
		return 0, err
	}
	if offset > math.MaxUint32 {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}
	dw.offsets[ref] = writtenType{
		pointer: mmdbtype.Pointer(offset), //nolint:gosec // checked above
		size:    size,
		written: true,
	}
	return offset, nil
}

// writeValue writes a canonical value. Callers register the offset of a first
// occurrence themselves, after the full value is emitted, which matches the
// historical greedy pointer algorithm.
func (dw *dataWriter) writeValue(ref valueRef) (int64, error) {
	node := dw.store.node(ref)
	start := dw.Len()
	if node.kind != valueKindMap && node.kind != valueKindSlice {
		written, err := dw.Write(dw.store.payload(node))
		return int64(written), err
	}

	size := int(node.childrenLen)
	if node.kind == valueKindMap {
		size /= 2
	}
	if err := writeContainerHeader(dw, node.kind, size); err != nil {
		return int64(dw.Len() - start), err
	}
	for _, child := range dw.store.childRefs(node) {
		if _, err := dw.writeOrWritePointer(child); err != nil {
			return int64(dw.Len() - start), err
		}
	}
	return int64(dw.Len() - start), nil
}

func (dw *dataWriter) rememberOffset(ref valueRef, offset int, size int64) {
	dw.ensureOffset(ref)
	if dw.offsets[ref].written || offset > math.MaxUint32 {
		return
	}
	dw.offsets[ref] = writtenType{
		pointer: mmdbtype.Pointer(offset), //nolint:gosec // checked above
		size:    size,
		written: true,
	}
}

func (dw *dataWriter) writeOrWritePointer(ref valueRef) (int64, error) {
	dw.ensureOffset(ref)
	written := dw.offsets[ref]
	// Only use a pointer if it would take less space than writing the value
	// again.
	if dw.usePointers && written.written && written.size > written.pointer.WrittenSize() {
		return written.pointer.WriteTo(dw)
	}
	start := dw.Len()
	size, err := dw.writeValue(ref)
	if err != nil {
		return size, err
	}
	if !written.written {
		dw.rememberOffset(ref, start, size)
	}
	return size, nil
}

// WriteOrWritePointer and WriteOrWritePointerString satisfy mmdbtype's writer
// interface, which Pointer.WriteTo requires. They write the value in full and
// never record an offset: an offset for a reference the caller later releases
// could be recycled and then point at unrelated data.
func (dw *dataWriter) WriteOrWritePointer(value mmdbtype.DataType) (int64, error) {
	return value.WriteTo(dw)
}

func (dw *dataWriter) WriteOrWritePointerString(value mmdbtype.String) (int64, error) {
	return value.WriteTo(dw)
}

func writeContainerHeader(
	writer interface{ WriteByte(byte) error },
	kind valueKind,
	size int,
) error {
	typeNumber := byte(7) // map
	if kind == valueKindSlice {
		typeNumber = 11
	}
	extended := typeNumber >= 8
	var first byte
	var second byte
	if extended {
		second = typeNumber - 7
	} else {
		first = typeNumber << 5
	}

	remaining := 0
	remainingSize := 0
	switch {
	case size < 29:
		first |= byte(size) //nolint:gosec // this branch bounds size below 29
	case size < 285:
		first |= 29
		remaining = size - 29
		remainingSize = 1
	case size < 65821:
		first |= 30
		remaining = size - 285
		remainingSize = 2
	case size < 16843037:
		first |= 31
		remaining = size - 65821
		remainingSize = 3
	default:
		return fmt.Errorf("cannot store %d container entries", size)
	}
	if err := writer.WriteByte(first); err != nil {
		return fmt.Errorf("writing container control byte: %w", err)
	}
	if extended {
		if err := writer.WriteByte(second); err != nil {
			return fmt.Errorf("writing extended container type: %w", err)
		}
	}
	for index := remainingSize - 1; index >= 0; index-- {
		value := byte(remaining >> (8 * index)) //nolint:gosec // one encoded size byte
		if err := writer.WriteByte(value); err != nil {
			return fmt.Errorf("writing container size: %w", err)
		}
	}
	return nil
}
