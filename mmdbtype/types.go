// Package mmdbtype provides types used within the MaxMind DB format.
package mmdbtype

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/bits"
	"reflect"
	"slices"

	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
)

type typeNum byte

const (
	typeNumExtended typeNum = iota
	typeNumPointer
	typeNumString
	typeNumFloat64
	typeNumBytes
	typeNumUint16
	typeNumUint32
	typeNumMap
	typeNumInt32
	typeNumUint64
	typeNumUint128
	typeNumSlice
	// We don't use the next two. They are placeholders. See the spec
	// for more details.
	typeNumContainer
	typeNumMarker
	typeNumBool
	typeNumFloat32
)

type writer interface {
	io.Writer
	WriteByte(byte) error
	WriteString(string) (int, error)
	WriteOrWritePointer(DataType) (int64, error)
	WriteOrWritePointerString(String) (int64, error)
}

// DataType represents a MaxMind DB data type.
type DataType interface {
	Copy() DataType

	// Equal reports whether other encodes to the same bytes as the receiver. It
	// is equality of the wire encoding, not of the Go value: an implementation
	// must report false for values that differ in any encoded bit even when Go
	// considers them equal, and true only when the two encode identically. The
	// writer deduplicates records on their encodings directly. The built-in
	// inserters use Equal to detect a merge that changes nothing, so an
	// implementation that is looser than the encoding can drop a change that
	// must replace a record.
	Equal(DataType) bool

	size() int
	typeNum() typeNum
	WriteTo(writer) (int64, error)
}

var (
	_ mmdbdata.CursorUnmarshaler = (*Bool)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Bytes)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Float32)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Float64)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Int32)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Map)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Slice)(nil)
	_ mmdbdata.CursorUnmarshaler = (*String)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Uint16)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Uint32)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Uint64)(nil)
	_ mmdbdata.CursorUnmarshaler = (*Uint128)(nil)
)

// Bool is the MaxMind DB boolean type.
type Bool bool

var _ DataType = (*Bool)(nil)

// Copy the value.
func (t Bool) Copy() DataType { return t }

// Equal checks for equality.
func (t Bool) Equal(other DataType) bool {
	otherT, ok := other.(Bool)
	return ok && t == otherT
}

func (t Bool) size() int {
	if t {
		return 1
	}
	return 0
}

func (t Bool) typeNum() typeNum {
	return typeNumBool
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Bool) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadBool()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Bool: %w", mmdbdata.NormalizeUnmarshalError[Bool](err))
	}
	*t = Bool(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Bool) WriteTo(w writer) (int64, error) {
	return writeCtrlByte(w, t.size(), t.typeNum())
}

// Bytes is the MaxMind DB bytes type.
type Bytes []byte

var _ DataType = Bytes(nil)

// Copy the value.
func (t Bytes) Copy() DataType {
	nv := make(Bytes, len(t))
	copy(nv, t)
	return nv
}

// Equal checks for equality.
func (t Bytes) Equal(other DataType) bool {
	otherT, ok := other.(Bytes)
	if !ok {
		return false
	}

	return bytes.Equal(t, otherT)
}

func (t Bytes) size() int {
	return len(t)
}

func (t Bytes) typeNum() typeNum {
	return typeNumBytes
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Bytes) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadBytes()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Bytes: %w", mmdbdata.NormalizeUnmarshalError[Bytes](err))
	}
	// ReadBytes returns a slice pointing to the underlying mmap.
	copied := make([]byte, len(value))
	copy(copied, value)
	*t = Bytes(copied)
	return next, nil
}

// WriteTo writes the value to w.
func (t Bytes) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	written, err := w.Write(t)
	numBytes += int64(written)
	if err != nil {
		return numBytes, fmt.Errorf(`writing "%s" as bytes: %w`, t, err)
	}
	return numBytes, nil
}

// Float32 is the MaxMind DB float type.
type Float32 float32

var _ DataType = (*Float32)(nil)

// Copy the value.
func (t Float32) Copy() DataType { return t }

// Equal reports whether other is a Float32 with the same wire encoding.
// Comparison is on the bit pattern rather than the Go value, so +0.0 and -0.0
// are not equal and two identical NaNs are.
func (t Float32) Equal(other DataType) bool {
	otherT, ok := other.(Float32)
	return ok && math.Float32bits(float32(t)) == math.Float32bits(float32(otherT))
}

func (t Float32) size() int {
	return 4
}

func (t Float32) typeNum() typeNum {
	return typeNumFloat32
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Float32) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadFloat32()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Float32: %w", mmdbdata.NormalizeUnmarshalError[Float32](err))
	}
	*t = Float32(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Float32) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	size := t.size()
	raw := math.Float32bits(float32(t))
	for i := size; i > 0; i-- {
		err = w.WriteByte(byte((raw >> (8 * (i - 1))) & 0xFF))
		if err != nil {
			return numBytes + int64(size-i), fmt.Errorf("writing %f as float32: %w", t, err)
		}
	}
	return numBytes + int64(size), nil
}

// Float64 is the MaxMind DB double type.
type Float64 float64

var _ DataType = (*Float64)(nil)

// Copy the value.
func (t Float64) Copy() DataType { return t }

// Equal reports whether other is a Float64 with the same wire encoding.
// Comparison is on the bit pattern rather than the Go value, so +0.0 and -0.0
// are not equal and two identical NaNs are.
func (t Float64) Equal(other DataType) bool {
	otherT, ok := other.(Float64)
	return ok && math.Float64bits(float64(t)) == math.Float64bits(float64(otherT))
}

func (t Float64) size() int {
	return 8
}

func (t Float64) typeNum() typeNum {
	return typeNumFloat64
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Float64) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadFloat64()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Float64: %w", mmdbdata.NormalizeUnmarshalError[Float64](err))
	}
	*t = Float64(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Float64) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	size := t.size()
	raw := math.Float64bits(float64(t))
	for i := size; i > 0; i-- {
		err = w.WriteByte(byte((raw >> (8 * (i - 1))) & 0xFF))
		if err != nil {
			return numBytes + int64(size-i), fmt.Errorf("writing %f as float64: %w", t, err)
		}
	}
	return numBytes + int64(size), nil
}

// Int32 is the MaxMind DB signed 32-bit integer type.
type Int32 int32

var _ DataType = (*Int32)(nil)

// Copy the value.
func (t Int32) Copy() DataType { return t }

// Equal checks for equality.
func (t Int32) Equal(other DataType) bool {
	otherT, ok := other.(Int32)
	return ok && t == otherT
}

func (t Int32) size() int {
	//nolint:gosec // we want the bit pattern, not the numeric value
	return 4 - bits.LeadingZeros32(uint32(t))/8
}

func (t Int32) typeNum() typeNum {
	return typeNumInt32
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Int32) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadInt32()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Int32: %w", mmdbdata.NormalizeUnmarshalError[Int32](err))
	}
	*t = Int32(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Int32) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	size := t.size()
	// We ignore leading zeros
	for i := size; i > 0; i-- {
		err = w.WriteByte(byte((int32(t) >> (8 * (i - 1))) & 0xFF))
		if err != nil {
			return numBytes + int64(size-i), fmt.Errorf("writing int32: %w", err)
		}
	}
	return numBytes + int64(size), nil
}

// Map is the MaxMind DB map type.
//
//nolint:recvcheck // preexisting/interface
type Map map[String]DataType

var _ DataType = Map(nil)

// Copy makes a deep copy of the Map.
func (t Map) Copy() DataType {
	newMap := make(Map, len(t))
	for k, v := range t {
		newMap[k] = v.Copy()
	}
	return newMap
}

// Equal checks for equality.
func (t Map) Equal(other DataType) bool {
	otherT, ok := other.(Map)
	if !ok {
		return false
	}

	if len(t) != len(otherT) {
		return false
	}

	if reflect.ValueOf(t).Pointer() == reflect.ValueOf(otherT).Pointer() {
		return true
	}

	for k, v := range t {
		if ov, ok := otherT[k]; !ok || !v.Equal(ov) {
			return false
		}
	}
	return true
}

func (t Map) size() int {
	return len(t)
}

func (t Map) typeNum() typeNum {
	return typeNumMap
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Map) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	return t.unmarshalMaxMindDBCursor(cursor, nil)
}

// unmarshalMaxMindDBCursor is the internal implementation that supports caching.
func (t *Map) unmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
	cache map[uint]DataType,
) (mmdbdata.Cursor, error) {
	entries, err := cursor.Map()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Map: %w", mmdbdata.NormalizeUnmarshalError[Map](err))
	}

	decoded := make(Map, entries.Size())
	var next mmdbdata.Cursor
	for {
		key, valueCursor, ok := entries.Next(next)
		if !ok {
			break
		}
		value, valueNext, valueErr := decodeDataTypeValue(valueCursor, cache)
		if valueErr != nil {
			return mmdbdata.Cursor{}, valueErr
		}
		next = valueNext
		decoded[String(key)] = value
	}
	next, err = entries.End()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf("reading Map entry: %w", err)
	}
	*t = decoded
	return next, nil
}

// WriteTo writes the value to w.
func (t Map) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	// We want database builds to be reproducible. As such, we insert
	// the map items in order by key value. In the future, we will
	// likely use a more relevant characteristic here (e.g., putting
	// fields more likely to be accessed first).
	//
	// For maps with a small number of keys (the common case for record
	// schemas), use a stack-allocated buffer to avoid a per-WriteTo
	// allocation. Map.WriteTo is on the data-section serialization hot path, so
	// this is amortized across the full write.
	var stackKeys [16]string
	var keys []string
	if len(t) <= len(stackKeys) {
		keys = stackKeys[:0]
	} else {
		keys = make([]string, 0, len(t))
	}
	for k := range t {
		keys = append(keys, string(k))
	}
	slices.Sort(keys)

	for _, ks := range keys {
		k := String(ks)
		written, err := w.WriteOrWritePointerString(k)
		numBytes += written
		if err != nil {
			return numBytes, err
		}
		written, err = w.WriteOrWritePointer(t[k])
		numBytes += written
		if err != nil {
			return numBytes, err
		}
	}
	return numBytes, nil
}

// Pointer is the MaxMind DB pointer type for internal use in the writer. You
// cannot use this type in data structures that you pass to methods on
// mmdbwriter.Tree. Inserting a value that contains a Pointer returns an
// error. A custom inserter can still receive a Pointer in its input value
// and must replace or discard it, since only direct inserts and inserter
// results are validated.
type Pointer uint32

var _ DataType = (*Pointer)(nil)

// Copy the value.
func (t Pointer) Copy() DataType { return t }

// Equal checks for equality.
func (t Pointer) Equal(other DataType) bool {
	otherT, ok := other.(Pointer)
	return ok && t == otherT
}

const (
	pointerMaxSize0 = 1 << 11
	pointerMaxSize1 = pointerMaxSize0 + (1 << 19)
	pointerMaxSize2 = pointerMaxSize1 + (1 << 27)
)

func (t Pointer) size() int {
	switch {
	case t < pointerMaxSize0:
		return 0
	case t < pointerMaxSize1:
		return 1
	case t < pointerMaxSize2:
		return 2
	default:
		return 3
	}
}

// WrittenSize is the actual total size of the pointer in the
// database data section.
func (t Pointer) WrittenSize() int64 {
	return int64(t.size() + 2)
}

func (t Pointer) typeNum() typeNum {
	return typeNumPointer
}

// WriteTo writes the value to w.
func (t Pointer) WriteTo(w writer) (int64, error) {
	size := t.size()
	switch size {
	case 0:
		err := w.WriteByte(0b00100000 | byte(0b111&(t>>8)))
		if err != nil {
			return 0, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & t))
		if err != nil {
			return 1, fmt.Errorf("writing pointer: %w", err)
		}
	case 1:
		v := t - pointerMaxSize0
		err := w.WriteByte(0b00101000 | byte(0b111&(v>>16)))
		if err != nil {
			return 0, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & (v >> 8)))
		if err != nil {
			return 1, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & v))
		if err != nil {
			return 2, fmt.Errorf("writing pointer: %w", err)
		}
	case 2:
		v := t - pointerMaxSize1
		err := w.WriteByte(0b00110000 | byte(0b111&(v>>24)))
		if err != nil {
			return 0, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & (v >> 16)))
		if err != nil {
			return 1, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & (v >> 8)))
		if err != nil {
			return 2, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & v))
		if err != nil {
			return 3, fmt.Errorf("writing pointer: %w", err)
		}
	case 3:
		err := w.WriteByte(0b00111000)
		if err != nil {
			return 0, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & (t >> 24)))
		if err != nil {
			return 1, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & (t >> 16)))
		if err != nil {
			return 2, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & (t >> 8)))
		if err != nil {
			return 3, fmt.Errorf("writing pointer: %w", err)
		}
		err = w.WriteByte(byte(0xFF & t))
		if err != nil {
			return 4, fmt.Errorf("writing pointer: %w", err)
		}
	}
	return t.WrittenSize(), nil
}

// Slice is the MaxMind DB array type.
//
//nolint:recvcheck // preexisting/interface
type Slice []DataType

var _ DataType = Slice(nil)

// Copy makes a deep copy of the Slice.
func (t Slice) Copy() DataType {
	newSlice := make(Slice, len(t))
	for k, v := range t {
		newSlice[k] = v.Copy()
	}
	return newSlice
}

// Equal checks for equality.
func (t Slice) Equal(other DataType) bool {
	otherT, ok := other.(Slice)
	if !ok {
		return false
	}

	if len(t) != len(otherT) {
		return false
	}

	if reflect.ValueOf(t).Pointer() == reflect.ValueOf(otherT).Pointer() {
		return true
	}

	for i, v := range t {
		if !otherT[i].Equal(v) {
			return false
		}
	}
	return true
}

func (t Slice) size() int {
	return len(t)
}

func (t Slice) typeNum() typeNum {
	return typeNumSlice
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Slice) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	return t.unmarshalMaxMindDBCursor(cursor, nil)
}

// unmarshalMaxMindDBCursor is the internal implementation that supports caching.
func (t *Slice) unmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
	cache map[uint]DataType,
) (mmdbdata.Cursor, error) {
	values, err := cursor.Slice()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Slice: %w", mmdbdata.NormalizeUnmarshalError[Slice](err))
	}
	size, err := values.Size()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf("reading Slice size: %w", err)
	}
	decoded := make(Slice, 0, size)
	var next mmdbdata.Cursor
	for {
		_, valueCursor, ok := values.Next(next)
		if !ok {
			break
		}
		value, valueNext, valueErr := decodeDataTypeValue(valueCursor, cache)
		if valueErr != nil {
			return mmdbdata.Cursor{}, valueErr
		}
		next = valueNext
		decoded = append(decoded, value)
	}
	next, err = values.End()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf("reading Slice element: %w", err)
	}
	*t = decoded
	return next, nil
}

// WriteTo writes the value to w.
func (t Slice) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	for _, e := range t {
		written, err := w.WriteOrWritePointer(e)
		numBytes += written
		if err != nil {
			return numBytes, err
		}
	}
	return numBytes, nil
}

// String is the MaxMind DB string type.
type String string

var _ DataType = (*String)(nil)

// Copy the value.
func (t String) Copy() DataType { return t }

// Equal checks for equality.
func (t String) Equal(other DataType) bool {
	otherT, ok := other.(String)
	return ok && t == otherT
}

func (t String) size() int {
	return len(t)
}

func (t String) typeNum() typeNum {
	return typeNumString
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *String) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadString()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading String: %w", mmdbdata.NormalizeUnmarshalError[String](err))
	}
	*t = String(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t String) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	written, err := w.WriteString(string(t))
	numBytes += int64(written)
	if err != nil {
		return numBytes, fmt.Errorf(`writing "%s" as a string: %w`, t, err)
	}
	return numBytes, nil
}

// Uint16 is the MaxMind DB unsigned 16-bit integer type.
type Uint16 uint16

var _ DataType = (*Uint16)(nil)

// Copy the value.
func (t Uint16) Copy() DataType { return t }

// Equal checks for equality.
func (t Uint16) Equal(other DataType) bool {
	otherT, ok := other.(Uint16)
	return ok && t == otherT
}

func (t Uint16) size() int {
	return 2 - bits.LeadingZeros16(uint16(t))/8
}

func (t Uint16) typeNum() typeNum {
	return typeNumUint16
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Uint16) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadUint()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Uint16: %w", mmdbdata.NormalizeUnmarshalError[Uint16](err))
	}
	if value > math.MaxUint16 {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Uint16: %w", mmdbdata.NewUnmarshalTypeError[Uint16](value))
	}
	*t = Uint16(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Uint16) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	size := t.size()
	// We ignore leading zeros
	for i := size; i > 0; i-- {
		err = w.WriteByte(byte(t >> (8 * (i - 1)) & 0xFF))
		if err != nil {
			return numBytes + int64(size-i), fmt.Errorf("writing uint16: %w", err)
		}
	}
	return numBytes + int64(size), nil
}

// Uint32 is the MaxMind DB unsigned 32-bit integer type.
type Uint32 uint32

var _ DataType = (*Uint32)(nil)

// Equal checks for equality.
func (t Uint32) Equal(other DataType) bool {
	otherT, ok := other.(Uint32)
	return ok && t == otherT
}

// Copy the value.
func (t Uint32) Copy() DataType { return t }

func (t Uint32) size() int {
	return 4 - bits.LeadingZeros32(uint32(t))/8
}

func (t Uint32) typeNum() typeNum {
	return typeNumUint32
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Uint32) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadUint()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Uint32: %w", mmdbdata.NormalizeUnmarshalError[Uint32](err))
	}
	if value > math.MaxUint32 {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Uint32: %w", mmdbdata.NewUnmarshalTypeError[Uint32](value))
	}
	*t = Uint32(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Uint32) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	size := t.size()
	// We ignore leading zeros
	for i := size; i > 0; i-- {
		err = w.WriteByte(byte(t >> (8 * (i - 1)) & 0xFF))
		if err != nil {
			return numBytes + int64(size-i), fmt.Errorf("writing uint32: %w", err)
		}
	}
	return numBytes + int64(size), nil
}

// Uint64 is the MaxMind DB unsigned 64-bit integer type.
type Uint64 uint64

var _ DataType = (*Uint64)(nil)

// Copy the value.
func (t Uint64) Copy() DataType { return t }

// Equal checks for equality.
func (t Uint64) Equal(other DataType) bool {
	otherT, ok := other.(Uint64)
	return ok && t == otherT
}

func (t Uint64) size() int {
	return 8 - bits.LeadingZeros64(uint64(t))/8
}

func (t Uint64) typeNum() typeNum {
	return typeNumUint64
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Uint64) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	value, next, err := cursor.ReadUint()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Uint64: %w", mmdbdata.NormalizeUnmarshalError[Uint64](err))
	}
	*t = Uint64(value)
	return next, nil
}

// WriteTo writes the value to w.
func (t Uint64) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	size := t.size()

	// We ignore leading zeros
	for i := size; i > 0; i-- {
		err = w.WriteByte(byte(t >> (8 * (i - 1)) & 0xFF))
		if err != nil {
			return numBytes + int64(size-i), fmt.Errorf("writing uint64: %w", err)
		}
	}
	return numBytes + int64(size), nil
}

// Uint128 is the MaxMind DB unsigned 128-bit integer type. A value stored in
// a Tree must be in the range [0, 2^128-1]. Inserting a value outside the
// range, or a nil *Uint128, returns an error. A custom inserter can still receive such a value
// in its input, since only direct inserts and inserter results are
// validated.
type Uint128 big.Int

var _ DataType = (*Uint128)(nil)

// Copy make a deep copy of the Uint128.
func (t *Uint128) Copy() DataType {
	nv := big.Int{}
	nv.Set((*big.Int)(t))
	uv := Uint128(nv)
	return &uv
}

// Equal reports whether other is a non-nil *Uint128 with the same value. A nil
// *Uint128 is not equal to any value, including another nil *Uint128.
func (t *Uint128) Equal(other DataType) bool {
	otherT, ok := other.(*Uint128)
	return ok && t != nil && otherT != nil && (*big.Int)(t).Cmp((*big.Int)(otherT)) == 0
}

func (t *Uint128) size() int {
	// We add 7 here as we want the ceiling of the division operation rather
	// than the floor.
	return ((*big.Int)(t).BitLen() + 7) / 8
}

func (t *Uint128) typeNum() typeNum {
	return typeNumUint128
}

// UnmarshalMaxMindDBCursor implements the mmdbdata.CursorUnmarshaler interface.
func (t *Uint128) UnmarshalMaxMindDBCursor(
	cursor mmdbdata.Cursor,
) (mmdbdata.Cursor, error) {
	hi, lo, next, err := cursor.ReadUint128()
	if err != nil {
		return mmdbdata.Cursor{}, fmt.Errorf(
			"reading Uint128: %w", mmdbdata.NormalizeUnmarshalError[Uint128](err))
	}
	v := new(big.Int)
	v.SetUint64(hi)
	v.Lsh(v, 64)
	v.Add(v, new(big.Int).SetUint64(lo))
	*t = Uint128(*v)
	return next, nil
}

// WriteTo writes the value to w.
func (t *Uint128) WriteTo(w writer) (int64, error) {
	numBytes, err := writeCtrlByte(w, t.size(), t.typeNum())
	if err != nil {
		return numBytes, err
	}

	written, err := w.Write((*big.Int)(t).Bytes())
	numBytes += int64(written)
	if err != nil {
		return numBytes, fmt.Errorf("writing uint128: %w", err)
	}
	return numBytes, nil
}

const (
	firstSize  = 29
	secondSize = firstSize + 256
	thirdSize  = secondSize + (1 << 16)
	maxSize    = thirdSize + (1 << 24)
)

func writeCtrlByte(w writer, size int, typeN typeNum) (int64, error) {
	var firstByte byte
	var secondByte byte

	if typeN < 8 {
		firstByte = byte(typeN << 5)
	} else {
		firstByte = byte(typeNumExtended << 5)
		secondByte = byte(typeN - 7)
	}

	leftOver := 0
	leftOverSize := 0
	switch {
	case size < firstSize:
		firstByte |= byte(size & 0xFF)
	case size < secondSize:
		firstByte |= 29
		leftOver = size - firstSize
		leftOverSize = 1
	case size < thirdSize:
		firstByte |= 30
		leftOver = size - secondSize
		leftOverSize = 2
	case size < maxSize:
		firstByte |= 31
		leftOver = size - thirdSize
		leftOverSize = 3
	default:
		return 0, fmt.Errorf(
			"cannot store %d bytes; max size is %d",
			size,
			maxSize-1,
		)
	}

	err := w.WriteByte(firstByte)
	if err != nil {
		return 0, fmt.Errorf(
			"writing first ctrl byte (type: %d, size: %d): %w",
			typeN,
			size,
			err,
		)
	}
	numBytes := int64(1)

	if secondByte != 0 {
		err = w.WriteByte(secondByte)
		if err != nil {
			return numBytes, fmt.Errorf(
				"writing second ctrl byte (type: %d, size: %d): %w",
				typeN,
				size,
				err,
			)
		}
		numBytes++
	}

	for i := leftOverSize - 1; i >= 0; i-- {
		v := byte((leftOver >> (8 * i)) & 0xFF)
		err = w.WriteByte(v)
		if err != nil {
			return numBytes, fmt.Errorf(
				"writing remaining ctrl bytes (type: %d, size: %d, value: %d): %w",
				typeN,
				size,
				v,
				err,
			)
		}
		numBytes++
	}
	return numBytes, nil
}

// isCacheableKind returns true if the given kind is worth caching. Currently,
// we have primarily found a benefit with containers, not scalar values.
// Potentially, longer strings may also benefit from caching, although it
// may be even better to just intern them, either here or directly in
// the maxminddb reader.
func isCacheableKind(kind mmdbdata.Kind) bool {
	return kind == mmdbdata.KindMap || kind == mmdbdata.KindSlice
}

// decodeDataTypeValue decodes the value at cursor and returns its successor.
// If cache is provided, it caches container types at their resolved offsets.
// Scalar types are cheap to decode and are not cached.
func decodeDataTypeValue(
	cursor mmdbdata.Cursor,
	cache map[uint]DataType,
) (DataType, mmdbdata.Cursor, error) {
	kind, err := cursor.Kind()
	if err != nil {
		return nil, mmdbdata.Cursor{}, fmt.Errorf("peeking kind: %w", err)
	}

	// Only check the cache for containers. This avoids map lookups for scalar
	// types in tight loops.
	useCache := cache != nil && isCacheableKind(kind)
	var offset uint
	if useCache {
		offset = cursor.Offset()
		if cached, ok := cache[offset]; ok {
			next, skipErr := cursor.Skip()
			if skipErr != nil {
				return nil, mmdbdata.Cursor{}, fmt.Errorf(
					"skipping cached value at offset %d: %w", offset, skipErr)
			}
			return cached, next, nil
		}
	}

	var value DataType
	var next mmdbdata.Cursor
	switch kind {
	case mmdbdata.KindString:
		var v String
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindFloat64:
		var v Float64
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindBytes:
		var v Bytes
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindUint16:
		var v Uint16
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindUint32:
		var v Uint32
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindInt32:
		var v Int32
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindUint64:
		var v Uint64
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindUint128:
		var v Uint128
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = &v
	case mmdbdata.KindBool:
		var v Bool
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindFloat32:
		var v Float32
		next, err = v.UnmarshalMaxMindDBCursor(cursor)
		value = v
	case mmdbdata.KindMap:
		var v Map
		next, err = v.unmarshalMaxMindDBCursor(cursor, cache)
		value = v
	case mmdbdata.KindSlice:
		var v Slice
		next, err = v.unmarshalMaxMindDBCursor(cursor, cache)
		value = v
	default:
		return nil, mmdbdata.Cursor{}, fmt.Errorf("unsupported data type: %v", kind)
	}

	if err != nil {
		return nil, mmdbdata.Cursor{}, err
	}

	if useCache {
		cache[offset] = value
	}

	return value, next, nil
}
