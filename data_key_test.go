package mmdbwriter

import (
	"io"
	"math/big"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func TestDataHasherIsIndependentOfMapIterationOrder(t *testing.T) {
	first := mmdbtype.Map{
		"boolean": mmdbtype.Bool(true),
		"nested": mmdbtype.Map{
			"number": mmdbtype.Uint32(42),
			"string": mmdbtype.String("value"),
		},
	}
	second := mmdbtype.Map{}
	second["nested"] = mmdbtype.Map{
		"string": mmdbtype.String("value"),
		"number": mmdbtype.Uint32(42),
	}
	second["boolean"] = mmdbtype.Bool(true)

	hasher := newDataHasher()
	firstHash, err := hasher.Hash(first)
	require.NoError(t, err)
	secondHash, err := hasher.Hash(second)
	require.NoError(t, err)

	assert.Equal(t, firstHash, secondHash)
}

func TestDataHasherHashesDeepCopiesEqually(t *testing.T) {
	value := benchmarkEnterpriseValue()
	hasher := newDataHasher()

	valueHash, err := hasher.Hash(value)
	require.NoError(t, err)
	copyHash, err := hasher.Hash(value.Copy())
	require.NoError(t, err)

	assert.Equal(t, valueHash, copyHash)
}

func TestDataHasherNormalizesPointerBackedValues(t *testing.T) {
	boolean := mmdbtype.Bool(true)
	bytesValue := mmdbtype.Bytes{1, 2, 3}
	float32Value := mmdbtype.Float32(1.5)
	float64Value := mmdbtype.Float64(2.5)
	int32Value := mmdbtype.Int32(-3)
	mapValue := mmdbtype.Map{"value": mmdbtype.String("map")}
	pointer := mmdbtype.Pointer(4)
	sliceValue := mmdbtype.Slice{mmdbtype.String("slice")}
	stringValue := mmdbtype.String("string")
	uint16Value := mmdbtype.Uint16(16)
	uint32Value := mmdbtype.Uint32(32)
	uint64Value := mmdbtype.Uint64(64)

	tests := []struct {
		name    string
		value   mmdbtype.DataType
		pointer mmdbtype.DataType
	}{
		{"bool", boolean, &boolean},
		{"bytes", bytesValue, &bytesValue},
		{"float32", float32Value, &float32Value},
		{"float64", float64Value, &float64Value},
		{"int32", int32Value, &int32Value},
		{"map", mapValue, &mapValue},
		{"pointer", pointer, &pointer},
		{"slice", sliceValue, &sliceValue},
		{"string", stringValue, &stringValue},
		{"uint16", uint16Value, &uint16Value},
		{"uint32", uint32Value, &uint32Value},
		{"uint64", uint64Value, &uint64Value},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hasher := newDataHasher()
			valueHash, err := hasher.Hash(test.value)
			require.NoError(t, err)
			pointerHash, err := hasher.Hash(test.pointer)
			require.NoError(t, err)
			assert.Equal(t, valueHash, pointerHash)
			assert.True(t, wireDataEqual(test.value, test.pointer))
			assert.True(t, wireDataEqual(test.pointer, test.value))

			dataMap := newDataMap()
			value, err := dataMap.store(test.value)
			require.NoError(t, err)
			pointerValue, err := dataMap.store(test.pointer)
			require.NoError(t, err)
			assert.Same(t, value, pointerValue)
		})
	}

	tree, err := New(Options{IPVersion: 4, IncludeReservedNetworks: true})
	require.NoError(t, err)
	require.NoError(t, tree.Insert(netip.MustParsePrefix("1.2.3.0/24"), &mapValue))
	_, err = tree.WriteTo(io.Discard)
	require.NoError(t, err)
}

func TestDataHasherRejectsNilPointerBackedValue(t *testing.T) {
	var value *mmdbtype.String

	_, err := newDataHasher().Hash(value)
	require.EqualError(t, err, "cannot hash a nil *mmdbtype.String")

	_, err = newDataHasher().Hash(mmdbtype.Map{"nil": value})
	require.EqualError(
		t,
		err,
		`hashing map key "nil": cannot hash a nil *mmdbtype.String`,
	)
}

func TestDataHasherSeedsEveryValueKind(t *testing.T) {
	uint128 := mmdbtype.Uint128(*big.NewInt(128))
	values := []mmdbtype.DataType{
		mmdbtype.Bool(true),
		mmdbtype.Bytes("bytes"),
		mmdbtype.Float32(1.25),
		mmdbtype.Float64(2.5),
		mmdbtype.Int32(-3),
		mmdbtype.Map{"value": mmdbtype.Uint32(4)},
		mmdbtype.Pointer(5),
		mmdbtype.Slice{mmdbtype.Uint64(6)},
		mmdbtype.String("string"),
		mmdbtype.Uint16(16),
		mmdbtype.Uint32(32),
		mmdbtype.Uint64(64),
		&uint128,
	}
	first := newDataHasher()
	second := newDataHasher()
	for _, value := range values {
		firstHash, err := first.Hash(value)
		require.NoError(t, err)
		secondHash, err := second.Hash(value)
		require.NoError(t, err)
		assert.NotEqual(t, firstHash, secondHash, "%T hash is not seeded", value)
	}
}

func TestDataHasherSeparatesPreviouslyCollidingScalars(t *testing.T) {
	tests := []struct {
		name   string
		first  mmdbtype.DataType
		second mmdbtype.DataType
	}{
		{
			name:   "uint16 and uint32",
			first:  mmdbtype.Uint16(1000),
			second: mmdbtype.Uint32(1001),
		},
		{
			name:   "uint32 and uint64",
			first:  mmdbtype.Uint32(1),
			second: mmdbtype.Uint64(6),
		},
		{
			// Bytes and String hash the same pre-salt bytes, so this pair
			// depends entirely on the per-type salts.
			name:   "bytes and string",
			first:  mmdbtype.Bytes("x"),
			second: mmdbtype.String("x"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hasher := newDataHasher()

			firstHash, err := hasher.Hash(test.first)
			require.NoError(t, err)
			secondHash, err := hasher.Hash(test.second)
			require.NoError(t, err)

			assert.NotEqual(t, firstHash, secondHash)
		})
	}
}

func TestDataHasherIsSensitiveToSliceOrder(t *testing.T) {
	hasher := newDataHasher()
	first, err := hasher.Hash(mmdbtype.Slice{
		mmdbtype.String("a"),
		mmdbtype.String("b"),
	})
	require.NoError(t, err)
	second, err := hasher.Hash(mmdbtype.Slice{
		mmdbtype.String("b"),
		mmdbtype.String("a"),
	})
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
}

func TestSliceIdentityPointerDoesNotAllocate(t *testing.T) {
	value := mmdbtype.Slice{mmdbtype.String("value")}
	var pointer uintptr
	allocations := testing.AllocsPerRun(1_000, func() {
		pointer = sliceIdentityPointer(value)
	})

	assert.NotZero(t, pointer)
	assert.Zero(t, allocations)
}

func TestEmptyContainerIdentitiesAreCanonical(t *testing.T) {
	nilMapIdentity, ok := keyIdentity(mmdbtype.Map(nil))
	require.True(t, ok)
	emptyMapIdentity, ok := keyIdentity(mmdbtype.Map{})
	require.True(t, ok)
	assert.Equal(t, nilMapIdentity, emptyMapIdentity)

	nilSliceIdentity, ok := keyIdentity(mmdbtype.Slice(nil))
	require.True(t, ok)
	emptySliceIdentity, ok := keyIdentity(make(mmdbtype.Slice, 0, 1))
	require.True(t, ok)
	assert.Equal(t, nilSliceIdentity, emptySliceIdentity)
}

func TestDataHasherRejectsNilNestedValue(t *testing.T) {
	_, err := newDataHasher().Hash(mmdbtype.Map{"nil": nil})
	require.EqualError(t, err, `hashing map key "nil": cannot hash a nil MMDB value`)
}

// TestDataHasherPropagatesSliceChildError mirrors the existing map-child error
// assertion for slice children.
func TestDataHasherPropagatesSliceChildError(t *testing.T) {
	_, err := newDataHasher().Hash(mmdbtype.Slice{nil})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "hashing slice index 0")
	assert.Contains(t, err.Error(), "cannot hash a nil MMDB value")
}
