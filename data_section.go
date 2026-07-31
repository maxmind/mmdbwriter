package mmdbwriter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

const dataSpillThreshold = 64 << 20

type writtenType struct {
	pointer mmdbtype.Pointer
	size    int64
	written bool
}

// spool begins in memory and moves to a temporary file once it crosses the
// spill threshold. It implements the byte/string methods required by the MMDB
// encoders while retaining only a small amount of heap for large sections.
type spool struct {
	buffer      bytes.Buffer
	file        *os.File
	path        string
	scratchPath string
	size        int64
	threshold   int64
}

func newSpool(scratchPath string) *spool {
	return &spool{scratchPath: scratchPath, threshold: dataSpillThreshold}
}

func (s *spool) Len() int { return int(s.size) }

func (s *spool) Write(value []byte) (int, error) {
	if err := s.spillIfNeeded(int64(len(value))); err != nil {
		return 0, err
	}
	var (
		written int
		err     error
	)
	if s.file == nil {
		written, err = s.buffer.Write(value)
	} else {
		written, err = s.file.Write(value)
	}
	s.size += int64(written)
	if err != nil {
		return written, fmt.Errorf("writing data spool: %w", err)
	}
	return written, nil
}

func (s *spool) WriteByte(value byte) error {
	var one [1]byte
	one[0] = value
	_, err := s.Write(one[:])
	return err
}

func (s *spool) WriteString(value string) (int, error) { //nolint:unparam // writer interface
	if err := s.spillIfNeeded(int64(len(value))); err != nil {
		return 0, err
	}
	var (
		written int
		err     error
	)
	if s.file == nil {
		written, err = s.buffer.WriteString(value)
	} else {
		written, err = s.file.WriteString(value)
	}
	s.size += int64(written)
	if err != nil {
		return written, fmt.Errorf("writing string to data spool: %w", err)
	}
	return written, nil
}

func (s *spool) spillIfNeeded(additional int64) error {
	if s.file != nil || s.size+additional <= s.threshold {
		return nil
	}
	directory := s.scratchPath
	if directory != "" {
		info, err := os.Stat(directory)
		if err != nil {
			return fmt.Errorf("checking scratch path %s: %w", directory, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("scratch path %s is not a directory", directory)
		}
	}
	file, err := os.CreateTemp(directory, "mmdbwriter-data-*.tmp")
	if err != nil {
		return fmt.Errorf("creating data spool in %s: %w", filepath.Clean(directory), err)
	}
	s.file = file
	s.path = file.Name()
	if _, err := s.buffer.WriteTo(file); err != nil {
		_ = s.Close()
		return fmt.Errorf("spilling data section: %w", err)
	}
	return nil
}

func (s *spool) WriteTo(writer io.Writer) (int64, error) {
	if s.file == nil {
		written, err := s.buffer.WriteTo(writer)
		if err != nil {
			return written, fmt.Errorf("copying buffered data section: %w", err)
		}
		return written, nil
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("rewinding data spool: %w", err)
	}
	written, err := io.Copy(writer, s.file)
	if err != nil {
		return written, fmt.Errorf("copying data spool: %w", err)
	}
	return written, nil
}

func (s *spool) Close() error {
	if s.file == nil {
		return nil
	}
	closeErr := s.file.Close()
	removeErr := os.Remove(s.path)
	s.file = nil
	if closeErr != nil {
		return fmt.Errorf("closing data spool: %w", closeErr)
	}
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("removing data spool: %w", removeErr)
	}
	return nil
}

type dataWriter struct {
	*spool

	store       *valueStore
	offsets     []writtenType
	usePointers bool
}

func newDataWriter(store *valueStore, usePointers bool, scratchPath ...string) *dataWriter {
	path := ""
	if len(scratchPath) != 0 {
		path = scratchPath[0]
	}
	return &dataWriter{
		spool:       newSpool(path),
		store:       store,
		offsets:     make([]writtenType, len(store.nodes)),
		usePointers: usePointers,
	}
}

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
	size, err := dw.writeValue(ref, true)
	if err != nil {
		return 0, err
	}
	if int64(offset) > int64(math.MaxUint32) {
		return 0, fmt.Errorf("offset of %d exceeds maximum when writing data", offset)
	}
	dw.offsets[ref] = writtenType{
		pointer: mmdbtype.Pointer(offset), //nolint:gosec // checked above
		size:    size,
		written: true,
	}
	return offset, nil
}

// writeValue writes a canonical value. If remember is true and this is the
// first occurrence, its offset is registered after the full value is emitted,
// matching the historical greedy pointer algorithm.
func (dw *dataWriter) writeValue(ref valueRef, remember bool) (int64, error) {
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
	written := int64(dw.Len() - start)
	if remember {
		dw.rememberOffset(ref, start, written)
	}
	return written, nil
}

func (dw *dataWriter) rememberOffset(ref valueRef, offset int, size int64) {
	dw.ensureOffset(ref)
	if dw.offsets[ref].written || int64(offset) > int64(math.MaxUint32) {
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
	if dw.usePointers && written.written && written.size > written.pointer.WrittenSize() {
		return written.pointer.WriteTo(dw)
	}
	start := dw.Len()
	size, err := dw.writeValue(ref, !written.written)
	if err != nil {
		return size, err
	}
	if !written.written {
		dw.rememberOffset(ref, start, size)
	}
	return size, nil
}

// These two methods retain compatibility with mmdbtype's internal writer
// interface. They intern metadata or other one-off values, then immediately
// emit by reference without hashing or map sorting on subsequent uses.
func (dw *dataWriter) WriteOrWritePointer(value mmdbtype.DataType) (int64, error) {
	ref, err := dw.store.intern(value)
	if err != nil {
		return 0, err
	}
	defer dw.store.release(ref)
	return dw.writeOrWritePointer(ref)
}

func (dw *dataWriter) WriteOrWritePointerString(value mmdbtype.String) (int64, error) {
	return dw.WriteOrWritePointer(value)
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
	var first byte
	var second byte
	if typeNumber < 8 {
		first = typeNumber << 5
	} else {
		second = typeNumber - 7
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
	if second != 0 {
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
