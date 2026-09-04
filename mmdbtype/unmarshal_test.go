package mmdbtype

import (
	"bytes"
	"math/big"
	"reflect"
	"testing"

	maxminddb "github.com/oschwald/maxminddb-golang/v2"
	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cursorDataType interface {
	DataType
	mmdbdata.CursorUnmarshaler
}

func TestCursorDataTypesRoundTrip(t *testing.T) {
	uint128 := Uint128(*new(big.Int).Lsh(big.NewInt(1), 100))
	tests := []struct {
		name        string
		value       DataType
		destination func() cursorDataType
	}{
		{"Bool", Bool(true), func() cursorDataType { return new(Bool) }},
		{"Bytes", Bytes{0, 1, 2}, func() cursorDataType { return new(Bytes) }},
		{"Float32", Float32(1.25), func() cursorDataType { return new(Float32) }},
		{"Float64", Float64(2.5), func() cursorDataType { return new(Float64) }},
		{"Int32", Int32(-42), func() cursorDataType { return new(Int32) }},
		{"Map", Map{"key": String("value")}, func() cursorDataType { return new(Map) }},
		{"Slice", Slice{String("value")}, func() cursorDataType { return new(Slice) }},
		{"String", String("value"), func() cursorDataType { return new(String) }},
		{"Uint16", Uint16(42), func() cursorDataType { return new(Uint16) }},
		{"Uint32", Uint32(1 << 20), func() cursorDataType { return new(Uint32) }},
		{"Uint64", Uint64(1 << 40), func() cursorDataType { return new(Uint64) }},
		{"Uint128", &uint128, func() cursorDataType { return new(Uint128) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var encoded bytes.Buffer
			writer := &dataWriter{Buffer: &encoded}
			_, err := test.value.WriteTo(writer)
			require.NoError(t, err)
			encoded.Write([]byte{0x41, 'x'})

			destination := test.destination()
			next, err := mmdbdata.NewDecoder(encoded.Bytes(), 0).Cursor().
				UnmarshalCursor(destination)
			require.NoError(t, err)
			assert.True(t, destination.Equal(test.value))

			trailing, _, err := next.ReadString()
			require.NoError(t, err)
			assert.Equal(t, "x", trailing)
		})
	}
}

func TestBytesCursorUnmarshalerCopiesInput(t *testing.T) {
	data := []byte{0x83, 1, 2, 3}
	var value Bytes
	_, err := mmdbdata.NewDecoder(data, 0).Cursor().UnmarshalCursor(&value)
	require.NoError(t, err)

	data[1] = 4
	assert.Equal(t, Bytes{1, 2, 3}, value)
}

func TestCursorUnmarshalerNormalizesTypeErrors(t *testing.T) {
	var value Bool
	_, err := mmdbdata.NewDecoder([]byte{0x41, 'x'}, 0).Cursor().
		UnmarshalCursor(&value)
	require.Error(t, err)

	var typeErr maxminddb.UnmarshalTypeError
	require.ErrorAs(t, err, &typeErr)
	assert.Equal(t, reflect.TypeFor[Bool](), typeErr.Type)
}

func TestUint16CursorUnmarshalerRejectsOverflow(t *testing.T) {
	// A uint64 containing 65,536.
	data := []byte{0x03, 0x02, 0x01, 0x00, 0x00}
	var value Uint16
	_, err := mmdbdata.NewDecoder(data, 0).Cursor().UnmarshalCursor(&value)
	require.Error(t, err)

	var typeErr maxminddb.UnmarshalTypeError
	require.ErrorAs(t, err, &typeErr)
	assert.Equal(t, reflect.TypeFor[Uint16](), typeErr.Type)
}

func TestUnmarshalerCachesResolvedContainerOffsets(t *testing.T) {
	// A two-element slice containing two pointers to the same map at offset 8,
	// followed by a trailing string at offset 6.
	data := []byte{
		0x02, 0x04,
		0x20, 0x08,
		0x20, 0x08,
		0x41, 'z',
		0xe1, 0x41, 'k', 0x41, 'a',
	}
	unmarshaler := NewUnmarshaler()
	next, err := mmdbdata.NewDecoder(data, 0).Cursor().UnmarshalCursor(unmarshaler)
	require.NoError(t, err)

	decoded, ok := unmarshaler.Result().(Slice)
	require.True(t, ok)
	require.Len(t, decoded, 2)
	first, ok := decoded[0].(Map)
	require.True(t, ok)
	second, ok := decoded[1].(Map)
	require.True(t, ok)
	assert.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(second).Pointer())
	assert.Len(t, unmarshaler.cache, 2)

	trailing, _, err := next.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "z", trailing)
}
