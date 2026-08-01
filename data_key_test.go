package mmdbwriter

import (
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
