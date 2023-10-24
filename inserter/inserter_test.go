package inserter

import (
	"testing"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemove(t *testing.T) {
	v, err := Remove(mmdbtype.Map{})
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestReplaceWith(t *testing.T) {
	v, err := ReplaceWith(mmdbtype.Uint64(1))(mmdbtype.Bool(true))
	require.NoError(t, err)
	assert.Equal(t, mmdbtype.Uint64(1), v)
}

func TestTopLevelMergeWith(t *testing.T) {
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
				"TopLevelMergeWith only works if both values are Map values",
		},
		{
			description: "existing slice, new map",
			existing:    mmdbtype.Slice{},
			new:         mmdbtype.Map{"a": mmdbtype.String("b")},
			expectedErr: "the existing value is a mmdbtype.Slice, not a Map; " +
				"TopLevelMergeWith only works if both values are Map values",
		},
		{
			description: "existing map, new slice",
			existing:    mmdbtype.Map{"a": mmdbtype.String("b")},
			new:         mmdbtype.Slice{},
			expectedErr: "the new value is a mmdbtype.Slice, not a Map; " +
				"TopLevelMergeWith only works if both values are Map values",
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
		v, err := TopLevelMergeWith(test.new)(test.existing)
		if test.expectedErr != "" {
			require.EqualError(t, err, test.expectedErr)
		} else {
			require.NoError(t, err)
			assert.Equal(t, test.expected, v)
		}
	}
}

func TestDeepMergeWith(t *testing.T) {
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
		v, err := DeepMergeWith(test.new)(test.existing)
		if test.expectedErr != "" {
			require.EqualError(t, err, test.expectedErr)
		} else {
			require.NoError(t, err)
			assert.Equal(t, test.expected, v)
		}
	}
}
