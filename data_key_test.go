package mmdbwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

func BenchmarkKeyGeneration(b *testing.B) {
	b.Run("simple value", func(b *testing.B) {
		value := mmdbtype.String("some test value that is not too long")
		writer := newKeyWriter()

		for i := 0; i < b.N; i++ {
			_, err := writer.key(value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("nested value", func(b *testing.B) {
		value := mmdbtype.Map{
			"string": mmdbtype.String("some string value"),
			"number": mmdbtype.Uint64(123456789),
			"slice": mmdbtype.Slice{
				mmdbtype.String("some string value"),
				mmdbtype.Uint64(123456789),
			},
		}
		writer := newKeyWriter()

		for i := 0; i < b.N; i++ {
			_, err := writer.key(value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestKeyWriter(t *testing.T) {
	assertions := assert.New(t)
	writer := newKeyWriter()

	valueOne := mmdbtype.String("some test value to be turned into a key")
	valueTwo := mmdbtype.String("another test value to be turned into a key")

	keyOne, err := writer.key(valueOne)

	assertions.NoErrorf(err, "expected no error")

	keyTwo, err := writer.key(valueTwo)

	assertions.NoErrorf(err, "expected no error")

	// The keys should be uniformly distributed, so the change of this test breaking at random is 1 in 2^64.
	assertions.NotEqual(keyOne, keyTwo, "expected keys to be different")

	keyOneAgain, err := writer.key(valueOne)

	assertions.NoErrorf(err, "expected no error")

	// Test this after keyTwo was created to make sure the state is not being reused.
	assertions.Equal(keyOne, keyOneAgain, "expected keys to be the same for the same value")
}
