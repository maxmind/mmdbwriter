package mmdbtype

import (
	"bytes"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2/mmdbdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnmarshalerCache verifies that the Unmarshaler has a cache field
// and that NewUnmarshaler initializes it correctly.
func TestUnmarshalerCache(t *testing.T) {
	u := NewUnmarshaler()
	assert.NotNil(t, u)

	// The cache should be initialized (non-nil)
	assert.NotNil(t, u.cache)
}

// TestZeroUnmarshaler verifies that the zero value of Unmarshaler is safe to use
// and correctly unmarshals data without caching enabled.
func TestZeroUnmarshaler(t *testing.T) {
	// Create test data with nested structures
	testData := Map{
		"outer": Map{
			"inner": Slice{
				String("value1"),
				String("value2"),
				Map{
					"deep": String("nested"),
					"num":  Uint32(42),
				},
			},
			"other": Uint64(100),
		},
		"simple": String("test"),
	}

	// Encode to bytes
	var buf bytes.Buffer
	dw := &dataWriter{Buffer: &buf}
	_, err := testData.WriteTo(dw)
	require.NoError(t, err)

	// Test 1: Use zero value Unmarshaler directly
	var zeroUnmarshaler Unmarshaler
	decoder := mmdbdata.NewDecoder(buf.Bytes(), 0)
	err = zeroUnmarshaler.UnmarshalMaxMindDB(decoder)
	require.NoError(t, err)

	decoded := zeroUnmarshaler.Result()
	require.NotNil(t, decoded)

	// Verify the decoded data matches the original
	assert.True(t, testData.Equal(decoded), "decoded data should match original")

	// Verify we can call Clear on zero value (should be safe)
	zeroUnmarshaler.Clear()
	assert.Nil(t, zeroUnmarshaler.Result())
}
