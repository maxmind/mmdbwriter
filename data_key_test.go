package mmdbwriter

import (
	"testing"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

func BenchmarkKeyGeneration(b *testing.B) {
	b.Run("small value", func(b *testing.B) {
		value := mmdbtype.String("some test value that is not too long")
		writer := newKeyWriter()

		for i := 0; i < b.N; i++ {
			_, err := writer.key(value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("large value", func(b *testing.B) {
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
