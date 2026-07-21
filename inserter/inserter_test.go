package inserter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

var benchmarkMergeValue mmdbtype.DataType

func TestRemove(t *testing.T) {
	v, err := Remove(mmdbtype.Map{}, mmdbtype.Map{})
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestReplace(t *testing.T) {
	v, err := Replace(mmdbtype.Bool(true), mmdbtype.Uint64(1))
	require.NoError(t, err)
	assert.Equal(t, mmdbtype.Uint64(1), v)
}

func TestTopLevelMerge(t *testing.T) {
	tests := []struct {
		description string
		existing    mmdbtype.DataType
		new         mmdbtype.DataType
		expected    mmdbtype.DataType
		expectedErr string
	}{
		{
			description: "all nils",
			existing:    nil,
			new:         nil,
			expected:    nil,
			expectedErr: "the new value is a <nil>, not a Map; " +
				"TopLevelMerge only works if both values are Map values",
		},
		{
			description: "existing slice, new map",
			existing:    mmdbtype.Slice{},
			new:         mmdbtype.Map{"a": mmdbtype.String("b")},
			expectedErr: "the existing value is a mmdbtype.Slice, not a Map; " +
				"TopLevelMerge only works if both values are Map values",
		},
		{
			description: "existing map, new slice",
			existing:    mmdbtype.Map{"a": mmdbtype.String("b")},
			new:         mmdbtype.Slice{},
			expectedErr: "the new value is a mmdbtype.Slice, not a Map; " +
				"TopLevelMerge only works if both values are Map values",
		},
		{
			description: "existing nil, new map",
			existing:    nil,
			new:         mmdbtype.Map{"a": mmdbtype.String("b")},
			expected:    mmdbtype.Map{"a": mmdbtype.String("b")},
		},
		{
			description: "merge",
			existing: mmdbtype.Map{
				"only-existing": mmdbtype.Slice{mmdbtype.Float32(1)},
				"both": mmdbtype.Map{
					"existing": mmdbtype.Bool(false),
				},
			},
			new: mmdbtype.Map{
				"only-new": mmdbtype.Slice{mmdbtype.Uint16(1)},
				"both": mmdbtype.Map{
					"new": mmdbtype.Bool(true),
				},
			},
			expected: mmdbtype.Map{
				"only-existing": mmdbtype.Slice{mmdbtype.Float32(1)},
				"only-new":      mmdbtype.Slice{mmdbtype.Uint16(1)},
				"both": mmdbtype.Map{
					"new": mmdbtype.Bool(true),
				},
			},
		},
	}

	for _, test := range tests {
		v, err := TopLevelMerge(test.existing, test.new)
		if test.expectedErr != "" {
			require.EqualError(t, err, test.expectedErr)
		} else {
			require.NoError(t, err)
			assert.Equal(t, test.expected, v)
		}
	}
}

func BenchmarkTopLevelMergeOverwriteHeavy(b *testing.B) {
	existing := benchmarkFlatMap("existing", 0, 64)
	newValue := benchmarkFlatMap("new", 0, 64)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		value, err := TopLevelMerge(existing, newValue)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMergeValue = value
	}
}

func BenchmarkTopLevelMergeAdditive(b *testing.B) {
	existing := benchmarkFlatMap("existing", 0, 64)
	newValue := benchmarkFlatMap("new", 64, 16)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		value, err := TopLevelMerge(existing, newValue)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMergeValue = value
	}
}

func BenchmarkDeepMergeNestedOverwrite(b *testing.B) {
	existing := benchmarkNestedMap("existing", 16, 0)
	newValue := benchmarkNestedMap("new", 16, 0)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		value, err := DeepMerge(existing, newValue)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMergeValue = value
	}
}

func BenchmarkDeepMergeNestedAdditive(b *testing.B) {
	existing := benchmarkNestedMap("existing", 16, 0)
	newValue := benchmarkNestedMap("new", 4, 8)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		value, err := DeepMerge(existing, newValue)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMergeValue = value
	}
}

func benchmarkFlatMap(valuePrefix string, start, count int) mmdbtype.Map {
	m := make(mmdbtype.Map, count)
	for i := range count {
		key := mmdbtype.String(fmt.Sprintf("key-%02d", start+i))
		m[key] = mmdbtype.String(fmt.Sprintf("%s-%02d", valuePrefix, i))
	}
	return m
}

func benchmarkNestedMap(
	valuePrefix string,
	groups int,
	fieldStart int,
) mmdbtype.Map {
	const fields = 8

	m := make(mmdbtype.Map, groups)
	for group := range groups {
		nested := make(mmdbtype.Map, fields)
		for field := range fields {
			key := mmdbtype.String(fmt.Sprintf("field-%02d", fieldStart+field))
			nested[key] = mmdbtype.String(
				fmt.Sprintf("%s-%02d-%02d", valuePrefix, group, field),
			)
		}
		key := mmdbtype.String(fmt.Sprintf("section-%02d", group))
		m[key] = nested
	}
	return m
}

func TestDeepMerge(t *testing.T) {
	tests := []struct {
		description string
		existing    mmdbtype.DataType
		new         mmdbtype.DataType
		expected    mmdbtype.DataType
		expectedErr string
	}{
		{
			description: "all nils",
			existing:    nil,
			new:         nil,
			expected:    nil,
		},
		{
			description: "existing slice, new map",
			existing:    mmdbtype.Slice{},
			new:         mmdbtype.Map{"a": mmdbtype.String("b")},
			expected:    mmdbtype.Map{"a": mmdbtype.String("b")},
		},
		{
			description: "existing map, new slice",
			existing:    mmdbtype.Map{"a": mmdbtype.String("b")},
			new:         mmdbtype.Slice{},
			expected:    mmdbtype.Slice{},
		},
		{
			description: "existing nil, new map",
			existing:    nil,
			new:         mmdbtype.Map{"a": mmdbtype.String("b")},
			expected:    mmdbtype.Map{"a": mmdbtype.String("b")},
		},
		{
			description: "merge",
			existing: mmdbtype.Map{
				"only-existing": mmdbtype.Slice{mmdbtype.Float32(1)},
				"both": mmdbtype.Map{
					"existing": mmdbtype.Bool(false),
				},
				"both-slice": mmdbtype.Slice{
					mmdbtype.Map{
						"existing": mmdbtype.Uint32(1),
					},
					mmdbtype.Map{
						"existing": mmdbtype.Uint32(2),
					},
				},
			},
			new: mmdbtype.Map{
				"only-new": mmdbtype.Slice{mmdbtype.Uint16(1)},
				"both": mmdbtype.Map{
					"new": mmdbtype.Bool(true),
				},
				"both-slice": mmdbtype.Slice{
					mmdbtype.Map{
						"new": mmdbtype.Uint32(2),
					},
				},
			},
			expected: mmdbtype.Map{
				"only-existing": mmdbtype.Slice{mmdbtype.Float32(1)},
				"only-new":      mmdbtype.Slice{mmdbtype.Uint16(1)},
				"both": mmdbtype.Map{
					"new":      mmdbtype.Bool(true),
					"existing": mmdbtype.Bool(false),
				},
				"both-slice": mmdbtype.Slice{
					mmdbtype.Map{
						"existing": mmdbtype.Uint32(1),
						"new":      mmdbtype.Uint32(2),
					},
					mmdbtype.Map{
						"existing": mmdbtype.Uint32(2),
					},
				},
			},
		},
	}

	for _, test := range tests {
		v, err := DeepMerge(test.existing, test.new)
		if test.expectedErr != "" {
			require.EqualError(t, err, test.expectedErr)
		} else {
			require.NoError(t, err)
			assert.Equal(t, test.expected, v)
		}
	}
}
