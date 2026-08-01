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
		first  mmdbtype.DataType
		second mmdbtype.DataType
	}{
		{mmdbtype.Uint16(1000), mmdbtype.Uint32(1001)},
		{mmdbtype.Uint32(1), mmdbtype.Uint64(6)},
	}
	hasher := newDataHasher()
	for _, test := range tests {
		firstHash, err := hasher.Hash(test.first)
		require.NoError(t, err)
		secondHash, err := hasher.Hash(test.second)
		require.NoError(t, err)
		assert.NotEqual(t, firstHash, secondHash)
	}
}

func TestSliceIdentityDoesNotAllocate(t *testing.T) {
	value := mmdbtype.Slice{mmdbtype.String("value")}
	var identity dataMapIdentityKey
	allocations := testing.AllocsPerRun(1_000, func() {
		identity = sliceDataIdentity(value)
	})

	assert.NotZero(t, identity.ptr)
	assert.Zero(t, allocations)
}

func TestEmptyContainerIdentitiesAreCanonical(t *testing.T) {
	assert.Equal(t, mapDataIdentity(nil), mapDataIdentity(mmdbtype.Map{}))
	assert.Equal(t, sliceDataIdentity(nil), sliceDataIdentity(make(mmdbtype.Slice, 0, 1)))
}

func TestDataHasherPromotesRepeatedContainers(t *testing.T) {
	shared := mmdbtype.Map{"value": mmdbtype.String("shared")}
	value := mmdbtype.Slice{shared, shared}
	identity := mapDataIdentity(shared)
	hasher := newDataHasher()

	_, err := hasher.Hash(value)
	require.NoError(t, err)

	index, ok := hasher.cacheByIdentity[identity]
	require.True(t, ok)
	assert.Equal(t, shared, hasher.cache[index].mapValue)
	assert.NotContains(t, hasher.probationByID, identity)
}

func TestDataHasherBoundsContainerCaches(t *testing.T) {
	hasher := newDataHasher()
	for index := range dataHashCacheSize + dataHashProbationSize + 1 {
		shared := mmdbtype.Map{"value": mmdbtype.Uint32(index)}
		_, err := hasher.Hash(mmdbtype.Slice{shared, shared})
		require.NoError(t, err)
	}

	assert.Len(t, hasher.cache, dataHashCacheSize)
	assert.LessOrEqual(t, len(hasher.probation), dataHashProbationSize)
}

func TestDataHasherRejectsNilNestedValue(t *testing.T) {
	_, err := newDataHasher().Hash(mmdbtype.Map{"nil": nil})
	require.EqualError(t, err, `hashing map key "nil": cannot hash a nil MMDB value`)
}
