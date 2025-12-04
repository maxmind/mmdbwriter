package mmdbtype

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file were mostly taken from
// https://github.com/oschwald/maxminddb-golang/blob/master/decoder_test.go

func TestBool(t *testing.T) {
	bools := map[string]DataType{
		"0007": Bool(false),
		"0107": Bool(true),
	}

	validateEncoding(t, bools)
}

func TestFloat32(t *testing.T) {
	floats := map[string]DataType{
		"040800000000": Float32(0.0),
		"04083f800000": Float32(1.0),
		"04083f8ccccd": Float32(1.1),
		"04084048f5c3": Float32(3.14),
		"0408461c3ff6": Float32(9999.99),
		"0408bf800000": Float32(-1.0),
		"0408bf8ccccd": Float32(-1.1),
		"0408c048f5c3": -Float32(3.14),
		"0408c61c3ff6": Float32(-9999.99),
	}
	validateEncoding(t, floats)
}

func TestFloat64(t *testing.T) {
	float64s := map[string]DataType{
		"680000000000000000": Float64(0.0),
		"683fe0000000000000": Float64(0.5),
		"68400921fb54442eea": Float64(3.14159265359),
		"68405ec00000000000": Float64(123.0),
		"6841d000000007f8f4": Float64(1073741824.12457),
		"68bfe0000000000000": Float64(-0.5),
		"68c00921fb54442eea": Float64(-3.14159265359),
		"68c1d000000007f8f4": Float64(-1073741824.12457),
	}
	validateEncoding(t, float64s)
}

func TestInt32(t *testing.T) {
	int32s := map[string]DataType{
		"0001":         Int32(0),
		"0401ffffffff": Int32(-1),
		"0101ff":       Int32(255),
		"0401ffffff01": Int32(-255),
		"020101f4":     Int32(500),
		"0401fffffe0c": Int32(-500),
		"0201ffff":     Int32(65535),
		"0401ffff0001": Int32(-65535),
		"0301ffffff":   Int32(16777215),
		"0401ff000001": Int32(-16777215),
		"04017fffffff": Int32(2147483647),
		"040180000001": Int32(-2147483647),
	}
	validateEncoding(t, int32s)
}

func TestMap(t *testing.T) {
	maps := map[string]DataType{
		"e0":                             Map{},
		"e142656e43466f6f":               Map{"en": String("Foo")},
		"e242656e43466f6f427a6843e4baba": Map{"en": String("Foo"), "zh": String("人")},
		"e1446e616d65e242656e43466f6f427a6843e4baba": Map{
			"name": Map{
				"en": String("Foo"),
				"zh": String("人"),
			},
		},
		"e1496c616e677561676573020442656e427a68": Map{
			"languages": Slice{String("en"), String("zh")},
		},
	}
	validateEncoding(t, maps)
}

func TestPointers(t *testing.T) {
	pointers := map[string]DataType{
		"2000":       Pointer(0),
		"27ff":       Pointer(pointerMaxSize0 - 1),
		"280000":     Pointer(pointerMaxSize0),
		"2fffff":     Pointer(pointerMaxSize1 - 1),
		"30000000":   Pointer(pointerMaxSize1),
		"37ffffff":   Pointer(pointerMaxSize2 - 1),
		"3808080800": Pointer(pointerMaxSize2),
		"38ffffffff": Pointer(1<<32 - 1),
	}

	validateEncoding(t, pointers)
}

func TestSlice(t *testing.T) {
	slice := map[string]DataType{
		"0004":                 Slice{},
		"010443466f6f":         Slice{String("Foo")},
		"020443466f6f43e4baba": Slice{String("Foo"), String("人")},
	}
	validateEncoding(t, slice)
}

var testStrings = makeTestStrings()

func makeTestStrings() map[string]DataType {
	str := map[string]DataType{
		"40":       String(""),
		"4131":     String("1"),
		"43e4baba": String("人"),
		"5b313233343536373839303132333435363738393031323334353637": String(
			"123456789012345678901234567",
		),
		"5c31323334353637383930313233343536373839303132333435363738": String(
			"1234567890123456789012345678",
		),
		"5d003132333435363738393031323334353637383930313233343536373839": String(
			"12345678901234567890123456789",
		),
		"5d01313233343536373839303132333435363738393031323334353637383930": String(
			"123456789012345678901234567890"),
	}

	for k, v := range map[string]int{"5e00d7": 500, "5e06b3": 2000, "5f001053": 70000} {
		key := k + strings.Repeat("78", v)
		str[key] = String(strings.Repeat("x", v))
	}

	return str
}

func TestString(t *testing.T) {
	validateEncoding(t, testStrings)
}

func TestByte(t *testing.T) {
	b := map[string]DataType{}
	for key, val := range testStrings {
		oldCtrl, err := hex.DecodeString(key[0:2])
		require.NoError(t, err)
		newCtrl := []byte{oldCtrl[0] ^ 0xc0}
		key = strings.Replace(key, hex.EncodeToString(oldCtrl), hex.EncodeToString(newCtrl), 1)
		b[key] = Bytes([]byte(val.(String)))
	}

	validateEncoding(t, b)
}

func TestUint16(t *testing.T) {
	uint16s := map[string]DataType{
		"a0":     Uint16(0),
		"a1ff":   Uint16(255),
		"a201f4": Uint16(500),
		"a22a78": Uint16(10872),
		"a2ffff": Uint16(65535),
	}
	validateEncoding(t, uint16s)
}

func TestUint32(t *testing.T) {
	uint32s := map[string]DataType{
		"c0":         Uint32(0),
		"c1ff":       Uint32(255),
		"c201f4":     Uint32(500),
		"c22a78":     Uint32(10872),
		"c2ffff":     Uint32(65535),
		"c3ffffff":   Uint32(16777215),
		"c4ffffffff": Uint32(4294967295),
	}
	validateEncoding(t, uint32s)
}

func TestUint64(t *testing.T) {
	ctrlByte := "02"
	bits := 64

	uints := map[string]DataType{
		"00" + ctrlByte:          Uint64(0),
		"02" + ctrlByte + "01f4": Uint64(500),
		"02" + ctrlByte + "2a78": Uint64(10872),
	}
	for i := 0; i <= bits/8; i++ {
		expected := uint64((1 << (8 * i)) - 1)

		input := hex.EncodeToString([]byte{byte(i)}) + ctrlByte + strings.Repeat("ff", i)
		uints[input] = Uint64(expected)
	}

	validateEncoding(t, uints)
}

func TestUint128(t *testing.T) {
	ctrlByte := "03"
	bits := 128

	uints := map[string]DataType{
		"00" + ctrlByte:          (*Uint128)(big.NewInt(0)),
		"02" + ctrlByte + "01f4": (*Uint128)(big.NewInt(500)),
		"02" + ctrlByte + "2a78": (*Uint128)(big.NewInt(10872)),
	}
	for i := 1; i <= bits/8; i++ {
		expected := &big.Int{}

		expected.Lsh(big.NewInt(1), 8*uint(i))
		expected = expected.Sub(expected, big.NewInt(1))
		input := hex.EncodeToString([]byte{byte(i)}) + ctrlByte + strings.Repeat("ff", i)

		uints[input] = (*Uint128)(expected)
	}

	validateEncoding(t, uints)
}

func TestEqual(t *testing.T) {
	sameMap := Map{"same": String("map")}
	sameSlice := Slice{String("same")}
	tests := []struct {
		name   string
		a      DataType
		b      DataType
		expect bool
	}{
		{
			name:   "Bool same",
			a:      Bool(true),
			b:      Bool(true),
			expect: true,
		},
		{
			name:   "Bool different",
			a:      Bool(false),
			b:      Bool(true),
			expect: false,
		},
		{
			name:   "Bytes same",
			a:      Bytes([]byte{1}),
			b:      Bytes([]byte{1}),
			expect: true,
		},
		{
			name:   "Bytes different",
			a:      Bytes([]byte{1}),
			b:      Bytes([]byte{0}),
			expect: false,
		},
		{
			name:   "Bytes different length",
			a:      Bytes([]byte{1, 1}),
			b:      Bytes([]byte{1}),
			expect: false,
		},
		{
			name:   "Float32 same",
			a:      Float32(1),
			b:      Float32(1),
			expect: true,
		},
		{
			name:   "Float32 different",
			a:      Float32(1),
			b:      Float32(0),
			expect: false,
		},
		{
			name:   "Float64 same",
			a:      Float64(1),
			b:      Float64(1),
			expect: true,
		},
		{
			name:   "Float64 different",
			a:      Float64(1),
			b:      Float64(0),
			expect: false,
		},
		{
			name:   "Int32 same",
			a:      Int32(1),
			b:      Int32(1),
			expect: true,
		},
		{
			name:   "Int32 different",
			a:      Int32(1),
			b:      Int32(0),
			expect: false,
		},
		{
			name:   "Pointer same",
			a:      Pointer(1),
			b:      Pointer(1),
			expect: true,
		},
		{
			name:   "Pointer different",
			a:      Pointer(1),
			b:      Pointer(0),
			expect: false,
		},
		{
			name:   "String same",
			a:      String("a"),
			b:      String("a"),
			expect: true,
		},
		{
			name:   "String different",
			a:      String("a"),
			b:      String("b"),
			expect: false,
		},
		{
			name:   "Uint16 same",
			a:      Uint16(1),
			b:      Uint16(1),
			expect: true,
		},
		{
			name:   "Uint16 different",
			a:      Uint16(1),
			b:      Uint16(0),
			expect: false,
		},
		{
			name:   "Uint32 same",
			a:      Uint32(1),
			b:      Uint32(1),
			expect: true,
		},
		{
			name:   "Uint32 different",
			a:      Uint32(1),
			b:      Uint32(0),
			expect: false,
		},
		{
			name:   "Uint64 same",
			a:      Uint64(1),
			b:      Uint64(1),
			expect: true,
		},
		{
			name:   "Uint64 different",
			a:      Uint64(1),
			b:      Uint64(0),
			expect: false,
		},
		{
			name:   "Uint128 same",
			a:      (*Uint128)(big.NewInt(1)),
			b:      (*Uint128)(big.NewInt(1)),
			expect: true,
		},
		{
			name:   "Uint128 different",
			a:      (*Uint128)(big.NewInt(1)),
			b:      (*Uint128)(big.NewInt(0)),
			expect: false,
		},
		{
			name:   "Int32 and Uint32 with same value are not equal",
			a:      Int32(1),
			b:      Uint32(1),
			expect: false,
		},
		{
			name:   "Complex Slice with same values",
			a:      Slice{Int32(-1), Map{"a": String("blah")}, Uint16(0)},
			b:      Slice{Int32(-1), Map{"a": String("blah")}, Uint16(0)},
			expect: true,
		},
		{
			name:   "Complex Slice with first being prefix of second",
			a:      Slice{Int32(-1), Map{"a": String("blah")}, Uint16(0)},
			b:      Slice{Int32(-1), Map{"a": String("blah")}, Uint16(0), Uint32(10)},
			expect: false,
		},
		{
			name:   "Same underlying Slice",
			a:      sameSlice,
			b:      sameSlice,
			expect: true,
		},
		{
			name: "Complex Map with same values",
			a: Map{
				"v1": Map{
					"i1": Slice{String("fda"), Map{}},
				},

				"v2": Uint64(3212213),
			},

			b: Map{
				"v1": Map{
					"i1": Slice{String("fda"), Map{}},
				},
				"v2": Uint64(3212213),
			},
			expect: true,
		},
		{
			name: "Complex Map with second having extra value",
			a: Map{
				"v1": Map{
					"i1": Slice{String("fda"), Map{}},
				},

				"v2": Uint64(3212213),
			},
			b: Map{
				"v1": Map{
					"i1": Slice{String("fda"), Map{}},
				},
				"v2": Uint64(3212213),
				"v3": Bool(false),
			},
			expect: false,
		},
		{
			name:   "Same underlying Map",
			a:      sameMap,
			b:      sameMap,
			expect: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(
				t,
				test.expect,
				test.a.Equal(test.b),
			)
		})
	}
}

func validateEncoding(t *testing.T, tests map[string]DataType) {
	t.Helper()

	for expected, dt := range tests {
		w := &dataWriter{Buffer: &bytes.Buffer{}}

		numBytes, err := dt.WriteTo(w)
		require.NoError(t, err)

		assert.Equal(t, int64(len(expected)/2), numBytes, "number of bytes written")
		actual := hex.EncodeToString(w.Bytes())

		assert.Equal(t, expected, actual, "%v - size: %d", dt, dt.size())
	}
}

type dataWriter struct {
	*bytes.Buffer
}

func (dw *dataWriter) WriteOrWritePointer(t DataType) (int64, error) {
	return t.WriteTo(dw)
}
