package mmdbtype

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUint128BigIntConversions(t *testing.T) {
	for _, test := range []struct {
		decimal string
		value   Uint128
	}{
		{"0", Uint128{}},
		{"18446744073709551615", Uint128{Low: math.MaxUint64}},
		{"18446744073709551616", Uint128{High: 1}},
		{"18446744073709551658", Uint128{High: 1, Low: 42}},
		{"340282366920938463463374607431768211455", Uint128{High: math.MaxUint64, Low: math.MaxUint64}},
	} {
		t.Run(test.decimal, func(t *testing.T) {
			input, ok := new(big.Int).SetString(test.decimal, 10)
			require.True(t, ok)
			value, err := Uint128FromBig(input)
			require.NoError(t, err)
			assert.Equal(t, test.value, value)
			assert.Equal(t, test.decimal, input.String(), "conversion changed its input")
			output := value.BigInt()
			assert.Equal(t, test.decimal, output.String())
			input.SetInt64(7)
			output.SetInt64(9)
			assert.Equal(t, test.value, value, "conversions must not share mutable storage")
			assert.Equal(t, test.decimal, value.BigInt().String())
		})
	}
}

func TestUint128FromBigRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   *big.Int
		message string
	}{
		{"nil", nil, "cannot convert a nil *big.Int to Uint128"},
		{"negative", big.NewInt(-1), "cannot convert a negative *big.Int to Uint128"},
		{"oversized", new(big.Int).Lsh(big.NewInt(1), 128), "cannot convert a *big.Int wider than 128 bits to Uint128"},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := Uint128FromBig(test.value)
			require.EqualError(t, err, test.message)
			assert.Zero(t, value)
		})
	}
}

func TestUint128EncodingBitBoundaries(t *testing.T) {
	for bit := range 128 {
		integer := new(big.Int).Lsh(big.NewInt(1), uint(bit))
		value, err := Uint128FromBig(integer)
		require.NoError(t, err)
		var buf bytes.Buffer
		written, err := value.WriteTo(&dataWriter{Buffer: &buf})
		require.NoError(t, err)
		payload := integer.Bytes()
		expected := append([]byte{byte(bit/8 + 1), 3}, payload...)
		assert.Equal(t, expected, buf.Bytes(), "bit %d", bit)
		assert.Equal(t, int64(len(expected)), written)
		var decoded Uint128
		_, err = decoded.UnmarshalMaxMindDBCursor(mmdbdata.NewDecoder(buf.Bytes(), 0).Cursor())
		require.NoError(t, err)
		assert.Equal(t, value, decoded)
	}
}

func TestUint128Copy(t *testing.T) {
	value := Uint128{High: 1, Low: 42}
	copied := value.Copy()
	value.High = 2
	value.Low = 3
	assert.Equal(t, Uint128{High: 1, Low: 42}, copied)
	assert.False(t, value.Equal(copied))
	assert.False(t, (Uint128{High: 1, Low: 42}).Equal(Uint128{High: 2, Low: 42}))
}

func TestUint128DecodePreservesReceiverOnError(t *testing.T) {
	for _, encoded := range [][]byte{
		{0x10, 0x03, 0xff}, // Truncated 16-byte value.
		{0x11, 0x03},       // A Uint128 cannot hold 17 bytes.
		{0x41, 'x'},        // Wrong type.
	} {
		before := Uint128{High: 1, Low: 42}
		value := before
		_, err := value.UnmarshalMaxMindDBCursor(mmdbdata.NewDecoder(encoded, 0).Cursor())
		require.Error(t, err)
		assert.Equal(t, before, value)
	}
}

type uint128FailingWriter struct {
	*dataWriter
	remaining int
	failure   error
}

func (w *uint128FailingWriter) WriteByte(b byte) error {
	if w.remaining == 0 {
		return w.failure
	}
	w.remaining--
	return w.dataWriter.WriteByte(b)
}

func TestUint128WriteErrors(t *testing.T) {
	value := Uint128{High: math.MaxUint64, Low: math.MaxUint64}
	failure := errors.New("write failed")
	for limit := range 18 {
		w := &uint128FailingWriter{
			dataWriter: &dataWriter{Buffer: new(bytes.Buffer)},
			remaining:  limit,
			failure:    failure,
		}
		written, err := value.WriteTo(w)
		require.ErrorIs(t, err, failure)
		assert.Equal(t, int64(limit), written)
		assert.Equal(t, limit, w.Len())
	}
}
