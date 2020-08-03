package mmdbwriter

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
	bools := map[string]dataType{
		"0007": typeBool(false),
		"0107": typeBool(true),
	}

	validateEncoding(t, bools)
}

func TestFloat32(t *testing.T) {
	floats := map[string]dataType{
		"040800000000": typeFloat32(0.0),
		"04083f800000": typeFloat32(1.0),
		"04083f8ccccd": typeFloat32(1.1),
		"04084048f5c3": typeFloat32(3.14),
		"0408461c3ff6": typeFloat32(9999.99),
		"0408bf800000": typeFloat32(-1.0),
		"0408bf8ccccd": typeFloat32(-1.1),
		"0408c048f5c3": -typeFloat32(3.14),
		"0408c61c3ff6": typeFloat32(-9999.99),
	}
	validateEncoding(t, floats)
}

func TestFloat64(t *testing.T) {
	float64s := map[string]dataType{
		"680000000000000000": typeFloat64(0.0),
		"683fe0000000000000": typeFloat64(0.5),
		"68400921fb54442eea": typeFloat64(3.14159265359),
		"68405ec00000000000": typeFloat64(123.0),
		"6841d000000007f8f4": typeFloat64(1073741824.12457),
		"68bfe0000000000000": typeFloat64(-0.5),
		"68c00921fb54442eea": typeFloat64(-3.14159265359),
		"68c1d000000007f8f4": typeFloat64(-1073741824.12457),
	}
	validateEncoding(t, float64s)
}

func TestInt32(t *testing.T) {
	int32s := map[string]dataType{
		"0001":         typeInt32(0),
		"0401ffffffff": typeInt32(-1),
		"0101ff":       typeInt32(255),
		"0401ffffff01": typeInt32(-255),
		"020101f4":     typeInt32(500),
		"0401fffffe0c": typeInt32(-500),
		"0201ffff":     typeInt32(65535),
		"0401ffff0001": typeInt32(-65535),
		"0301ffffff":   typeInt32(16777215),
		"0401ff000001": typeInt32(-16777215),
		"04017fffffff": typeInt32(2147483647),
		"040180000001": typeInt32(-2147483647),
	}
	validateEncoding(t, int32s)
}

func TestMap(t *testing.T) {
	maps := map[string]dataType{
		"e0":                             typeMap{},
		"e142656e43466f6f":               typeMap{"en": typeString("Foo")},
		"e242656e43466f6f427a6843e4baba": typeMap{"en": typeString("Foo"), "zh": typeString("人")},
		"e1446e616d65e242656e43466f6f427a6843e4baba": typeMap{
			"name": typeMap{
				"en": typeString("Foo"),
				"zh": typeString("人"),
			},
		},
		"e1496c616e677561676573020442656e427a68": typeMap{
			"languages": typeSlice{typeString("en"), typeString("zh")},
		},
	}
	validateEncoding(t, maps)
}

func TestSlice(t *testing.T) {
	slice := map[string]dataType{
		"0004":                 typeSlice{},
		"010443466f6f":         typeSlice{typeString("Foo")},
		"020443466f6f43e4baba": typeSlice{typeString("Foo"), typeString("人")},
	}
	validateEncoding(t, slice)
}

var testStrings = makeTestStrings()

func makeTestStrings() map[string]dataType {
	str := map[string]dataType{
		"40":       typeString(""),
		"4131":     typeString("1"),
		"43e4baba": typeString("人"),
		"5b313233343536373839303132333435363738393031323334353637":       typeString("123456789012345678901234567"),
		"5c31323334353637383930313233343536373839303132333435363738":     typeString("1234567890123456789012345678"),
		"5d003132333435363738393031323334353637383930313233343536373839": typeString("12345678901234567890123456789"),
		"5d01313233343536373839303132333435363738393031323334353637383930": typeString(
			"123456789012345678901234567890"),
	}

	for k, v := range map[string]int{"5e00d7": 500, "5e06b3": 2000, "5f001053": 70000} {
		key := k + strings.Repeat("78", v)
		str[key] = typeString(strings.Repeat("x", v))
	}

	return str
}

func TestString(t *testing.T) {
	validateEncoding(t, testStrings)
}

func TestByte(t *testing.T) {
	b := make(map[string]dataType)
	for key, val := range testStrings {
		oldCtrl, _ := hex.DecodeString(key[0:2])
		newCtrl := []byte{oldCtrl[0] ^ 0xc0}
		key = strings.Replace(key, hex.EncodeToString(oldCtrl), hex.EncodeToString(newCtrl), 1)
		b[key] = typeBytes([]byte(val.(typeString)))
	}

	validateEncoding(t, b)
}

func TestUint16(t *testing.T) {
	uint16s := map[string]dataType{
		"a0":     typeUint16(0),
		"a1ff":   typeUint16(255),
		"a201f4": typeUint16(500),
		"a22a78": typeUint16(10872),
		"a2ffff": typeUint16(65535),
	}
	validateEncoding(t, uint16s)
}

func TestUint32(t *testing.T) {
	uint32s := map[string]dataType{
		"c0":         typeUint32(0),
		"c1ff":       typeUint32(255),
		"c201f4":     typeUint32(500),
		"c22a78":     typeUint32(10872),
		"c2ffff":     typeUint32(65535),
		"c3ffffff":   typeUint32(16777215),
		"c4ffffffff": typeUint32(4294967295),
	}
	validateEncoding(t, uint32s)
}

func TestUint64(t *testing.T) {
	ctrlByte := "02"
	bits := uint64(64)

	uints := map[string]dataType{
		"00" + ctrlByte:          typeUint64(0),
		"02" + ctrlByte + "01f4": typeUint64(500),
		"02" + ctrlByte + "2a78": typeUint64(10872),
	}
	for i := uint64(0); i <= bits/8; i++ {
		expected := uint64((1 << (8 * i)) - 1)

		input := hex.EncodeToString([]byte{byte(i)}) + ctrlByte + strings.Repeat("ff", int(i))
		uints[input] = typeUint64(expected)
	}

	validateEncoding(t, uints)
}

func TestUint128(t *testing.T) {
	ctrlByte := "03"
	bits := uint(128)

	uints := map[string]dataType{
		"00" + ctrlByte:          (*typeUint128)(big.NewInt(0)),
		"02" + ctrlByte + "01f4": (*typeUint128)(big.NewInt(500)),
		"02" + ctrlByte + "2a78": (*typeUint128)(big.NewInt(10872)),
	}
	for i := uint(1); i <= bits/8; i++ {
		expected := powBigInt(big.NewInt(2), 8*i)
		expected = expected.Sub(expected, big.NewInt(1))
		input := hex.EncodeToString([]byte{byte(i)}) + ctrlByte + strings.Repeat("ff", int(i))

		uints[input] = (*typeUint128)(expected)
	}

	validateEncoding(t, uints)
}

// No pow or bit shifting for big int, apparently :-(
// This is _not_ meant to be a comprehensive power function
func powBigInt(bi *big.Int, pow uint) *big.Int {
	newInt := big.NewInt(1)
	for i := uint(0); i < pow; i++ {
		newInt.Mul(newInt, bi)
	}
	return newInt
}

func validateEncoding(t *testing.T, tests map[string]dataType) {
	for expected, dt := range tests {
		w := &bytes.Buffer{}

		require.NoError(t, dt.writeTo(w))

		actual := hex.EncodeToString(w.Bytes())

		assert.Equal(t, expected, actual, "%v - size: %d", dt, dt.size())
	}
}

// func TestPointers(t *testing.T) {
// 	bytes, err := ioutil.ReadFile(testFile("maps-with-pointers.raw"))
// 	require.NoError(t, err)
// 	d := decoder{bytes}

// 	expected := map[uint]map[string]string{
// 		0:  {"long_key": "long_value1"},
// 		22: {"long_key": "long_value2"},
// 		37: {"long_key2": "long_value1"},
// 		50: {"long_key2": "long_value2"},
// 		55: {"long_key": "long_value1"},
// 		57: {"long_key2": "long_value2"},
// 	}

// 	for offset, expectedValue := range expected {
// 		var actual map[string]string
// 		_, err := d.decode(offset, reflect.ValueOf(&actual), 0)
// 		assert.NoError(t, err)
// 		if !reflect.DeepEqual(actual, expectedValue) {
// 			t.Errorf("Decode for pointer at %d failed", offset)
// 		}
// 	}
// }
